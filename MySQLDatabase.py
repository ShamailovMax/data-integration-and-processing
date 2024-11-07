import logging
import pandas as pd
import mysql.connector
import csv

import os

from decorators import retry

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("mysql_operations.log"),
        logging.StreamHandler()
    ]
)

class MySQLDatabase:
    def __init__(self, host, database, user, password, allow_local_infile=False):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.allow_local_infile=allow_local_infile
        self.conn = None
        self.logger = logging.getLogger(self.__class__.__name__)  # Создаем логгер для класса

    @retry(retries=3, delay=5, logger=logging.getLogger(__name__))
    def connect(self):
        """Подключается к MySQL."""
        try:
            self.conn = mysql.connector.connect(
                user=self.user, 
                password=self.password, 
                host=self.host, 
                database=self.database,
                allow_local_infile=self.allow_local_infile
            )
            self.logger.info("Подключение к MySQL установлено.")
        except mysql.connector.Error as e:
            self.logger.error(f"Ошибка подключения к базе данных: {e}")

    def disconnect(self):
        """Отключается от MySQL."""
        if self.conn:
            self.conn.close()
            self.logger.info("Соединение с MySQL закрыто.")

    def create_table(self, table_name, df):
        """Создает таблицу в MySQL в указанной схеме."""
        replacements = {
            'float64': 'decimal(10, 2)',
            'object': 'varchar(255)',
            'int64': 'bigint',
            'int32': 'int',
            'int16': 'smallint',
            'bool': 'tinyint(1)',
            'datetime64[ns]': 'datetime',
            'timedelta64[ns]': 'varchar(50)',
            'string': 'varchar(255)'
        }

        col_str = []
        for col, dtype in zip(df.columns, df.dtypes):
            dtype_str = str(dtype)
            if dtype_str in replacements:
                col_type = replacements[dtype_str]
            else:
                col_type = 'varchar(255)'
            col_str.append(f"{col} {col_type}")

        col_str = ", ".join(col_str)
        full_table_name = f"{self.database}.{table_name}"

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {full_table_name};")
                cursor.execute(f"CREATE TABLE {full_table_name} ({col_str});")
                self.conn.commit()
            self.logger.info(f"Таблица {full_table_name} создана в MySQL.")
        except (Exception, mysql.connector.DatabaseError) as error:
            self.logger.error(f"Ошибка при создании таблицы: {error}")
            self.conn.rollback()

    def insert_data_from_csv(self, table_name, file_path):
        """Читает CSV-файл и вставляет данные в таблицу."""
        cursor = self.conn.cursor()

        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)
            columns = next(reader)
            rows = list(reader) 

        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        for row in rows:
            cursor.execute(query, tuple(row))

        self.conn.commit()
        cursor.close()

    def load_data_to_db(self, df, table_name):
        """Загружает данные из DataFrame в MySQL."""
        temp_csv_path = f"{table_name}.csv"
        full_table_name = f"`{self.database}`.`{table_name}`"
        df.to_csv(temp_csv_path, encoding='utf-8', header=True, index=False)

        try:
            self.insert_data_from_csv(table_name, temp_csv_path)
            self.logger.info(f"Данные загружены в таблицу {full_table_name} в MySQL.")
        except (Exception, mysql.connector.DatabaseError) as error:
            self.logger.error(f"Ошибка при загрузке данных: {error}")
            self.conn.rollback()
        finally:
            if os.path.exists(temp_csv_path):
                os.remove(temp_csv_path)
                self.logger.info(f"Временный CSV файл {temp_csv_path} удален.")

    @staticmethod
    def clean_name(name):
        """Очищает имя от недопустимых символов."""
        return name.lower() \
                   .replace(" ", "_") \
                   .replace("?", "") \
                   .replace("-", "_") \
                   .replace(r"/", "_") \
                   .replace("\\", "_") \
                   .replace("%", "") \
                   .replace(")", "") \
                   .replace(r"(", "") \
                   .replace("$", "")

    def rename_columns(self, df, column_mapping):
        """Переименовывает столбцы DataFrame согласно переданному словарю."""
        df.columns = [self.clean_name(col) for col in df.columns]
        cleaned_column_mapping = {self.clean_name(key): value for key, value in column_mapping.items()}
        self.logger.info(f"Столбцы переименованы согласно column_mapping: {column_mapping}")
        return df.rename(columns=cleaned_column_mapping)

    def process_data(self, data_source, column_mapping=None, table_name=None):
        """Обрабатывает и загружает данные из Excel в MySQL."""
        try:
            df = pd.read_excel(data_source) if isinstance(data_source, str) else data_source
            self.logger.info("Данные успешно загружены из источника.")
            df = self.rename_columns(df, column_mapping)
            if table_name is None and isinstance(data_source, str):
                table_name = self.clean_name(os.path.splitext(os.path.basename(data_source))[0])
            self.create_table(table_name, df)
            self.load_data_to_db(df, table_name)
            self.logger.info(f"Данные из источника {data_source} загружены в таблицу {table_name} в MySQL.")
        except (Exception, mysql.connector.DatabaseError) as error:
            self.logger.error(f"Ошибка при обработке данных: {error}")
            self.conn.rollback()

    def transfer_from_clickhouse(self, clickhouse_db, ch_table, my_sql_table, column_mapping=None):
        """Копирует данные из ClickHouse в MySQL с переименованием колонок."""
        try:
            # Запрос данных из ClickHouse
            query = f"SELECT * FROM {ch_table};"
            df = clickhouse_db.client.query_df(query)
            self.logger.info(f"Данные из таблицы {ch_table} успешно получены из ClickHouse.")

            # Переименование колонок, если задано сопоставление
            if column_mapping:
                df = self.rename_columns(df, column_mapping)
            
            # Создание таблицы в MySQL и загрузка данных
            self.create_table(my_sql_table, df)
            self.load_data_to_db(df, my_sql_table)
            self.logger.info(f"Данные успешно перенесены из ClickHouse в MySQL в таблицу {my_sql_table}.")
        
        except Exception as error:
            self.logger.error(f"Ошибка при копировании данных из ClickHouse в MySQL: {error}")
            self.conn.rollback()
