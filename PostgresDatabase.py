import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

class PostgresDatabase:
    def __init__(self, host, database, user, password, schema="public"):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.schema = schema
        self.conn = None

    def connect(self):
        """Подключается к PostgreSQL."""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                dbname=self.database,
                user=self.user,
                password=self.password
            )
            with self.conn.cursor() as cursor:
                cursor.execute(f"SET search_path TO {self.schema};")
            print("Соединение с PostgreSQL установлено.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Ошибка подключения к PostgreSQL: {error}")
            self.conn = None

    def disconnect(self):
        """Отключается от PostgreSQL."""
        if self.conn:
            self.conn.close()
            print("Соединение с PostgreSQL закрыто.")

    def create_table(self, table_name, df):
        """Создает таблицу в PostgreSQL в указанной схеме."""
        replacements = {
            'float64': 'decimal',
            'object': 'varchar',
            'int64': 'int',
            'int32': 'int',
            'int16': 'smallint',
            'bool': 'boolean',
            'datetime64[ns]': 'timestamp',
            'timedelta64[ns]': 'varchar',
            'string': 'varchar'
        }
        col_str = ", ".join(f"{col} {replacements.get(str(dtype), 'varchar')}" 
                            for col, dtype in zip(df.columns, df.dtypes))
        full_table_name = f"{self.schema}.{table_name}"
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {full_table_name};")
                cursor.execute(f"CREATE TABLE {full_table_name} ({col_str});")
                self.conn.commit()
            print(f"Таблица {full_table_name} создана в PostgreSQL.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Ошибка при создании таблицы: {error}")
            self.conn.rollback()

    def load_data_to_db(self, df, table_name):
        """Загружает данные из DataFrame в PostgreSQL."""
        temp_csv_path = f"{table_name}.csv"
        full_table_name = f"{self.schema}.{table_name}"
        df.to_csv(temp_csv_path, encoding='utf-8', header=True, index=False)
        
        try:
            with open(temp_csv_path, 'r', encoding='utf-8') as my_file:
                with self.conn.cursor() as cursor:
                    sql_statement = f"""COPY {full_table_name} FROM STDIN WITH
                                        CSV
                                        ENCODING 'UTF8'
                                        HEADER
                                        DELIMITER AS ',';"""
                    cursor.copy_expert(sql=sql_statement, file=my_file)
                    cursor.execute(f"GRANT SELECT ON TABLE {full_table_name} TO PUBLIC;")
                    self.conn.commit()
            print(f"Данные загружены в таблицу {full_table_name} в PostgreSQL.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Ошибка при загрузке данных: {error}")
            self.conn.rollback()
        finally:
            if os.path.exists(temp_csv_path):
                os.remove(temp_csv_path)

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
        return df.rename(columns=cleaned_column_mapping)

    def process_data(self, data_source, column_mapping=None, table_name=None):
        """Обрабатывает и загружает данные из Excel в PostgreSQL."""
        try:
            df = pd.read_excel(data_source) if isinstance(data_source, str) else data_source
            df = self.rename_columns(df, column_mapping)
            if table_name is None and isinstance(data_source, str):
                table_name = self.clean_name(os.path.splitext(os.path.basename(data_source))[0])
            self.create_table(table_name, df)
            self.load_data_to_db(df, table_name)
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Ошибка при обработке данных: {error}")
            self.conn.rollback()

    def transfer_from_clickhouse(self, clickhouse_db, ch_table, pg_table, column_mapping=None):
        """Копирует данные из ClickHouse в PostgreSQL с переименованием колонок."""
        try:
            # Запрос данных из ClickHouse
            query = f"SELECT * FROM {ch_table};"
            df = clickhouse_db.client.query_df(query)
            print(f"Данные из таблицы {ch_table} успешно получены из ClickHouse.")

            # Переименование колонок, если задано сопоставление
            if column_mapping:
                df = self.rename_columns(df, column_mapping)
            
            # Создание таблицы в PostgreSQL и загрузка данных
            self.create_table(pg_table, df)
            self.load_data_to_db(df, pg_table)
            print(f"Данные успешно перенесены из ClickHouse в PostgreSQL.")
        
        except Exception as error:
            print(f"Ошибка при копировании данных из ClickHouse в PostgreSQL: {error}")
            self.conn.rollback()
