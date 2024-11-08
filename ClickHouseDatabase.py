import logging
import pandas as pd
import clickhouse_connect
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from decorators import retry

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("database_operations.log"),
        logging.StreamHandler()
    ]
)

class ClickHouseDatabase:
    def __init__(self, host, port, user, password, schema="default"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.schema = schema  # Новое поле для схемы
        self.client = None
        self.logger = logging.getLogger(self.__class__.__name__)

    @retry(retries=3, delay=5, logger=logging.getLogger(__name__))
    def connect(self):
        """Подключается к ClickHouse и создает схему, если ее нет."""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password
            )
            self.logger.info("Соединение с ClickHouse установлено.")

            # Проверка наличия схемы
            schema_check_query = f"EXISTS DATABASE {self.schema}"
            schema_exists = self.client.command(schema_check_query)

            if schema_exists:
                self.logger.info(f"Схема {self.schema} уже существует.")
            else:
                # Создание схемы, если она отсутствует
                self.client.command(f"CREATE DATABASE {self.schema}")
                self.logger.info(f"Схема {self.schema} создана, так как отсутствовала.")

        except Exception as error:
            self.logger.error(f"Ошибка подключения к ClickHouse: {error}")
            self.client = None

    def disconnect(self):
        """Отключается от ClickHouse."""
        if self.client:
            self.client.close()
            self.logger.info("Соединение с ClickHouse закрыто.")

    @staticmethod
    def get_clickhouse_types(df):
        """Сопоставляет типы данных Pandas с типами ClickHouse, включая поддержку Nullable для разных типов."""
        dtype_mapping = {
            'float64': 'Nullable(Float64)',
            'float32': 'Nullable(Float32)',
            'object': 'Nullable(String)',
            'int64': 'Nullable(Int64)',
            'int32': 'Nullable(Int32)',
            'int16': 'Nullable(Int16)',
            'int8': 'Nullable(Int8)',
            'uint64': 'Nullable(UInt64)',
            'uint32': 'Nullable(UInt32)',
            'uint16': 'Nullable(UInt16)',
            'uint8': 'Nullable(UInt8)',
            'bool': 'Nullable(UInt8)',
            'datetime64[ns]': 'Nullable(DateTime)',
            'datetime64[ns, UTC]': 'Nullable(DateTime)',
            'timedelta64[ns]': 'Nullable(String)',
            'category': 'Nullable(String)',
        }
        return [dtype_mapping.get(str(dtype), 'Nullable(String)') for dtype in df.dtypes]

    def create_table(self, table_name, df, engine="MergeTree", engine_params=None):
        """Создает таблицу в ClickHouse в указанной схеме."""
        full_table_name = f"{self.schema}.{table_name}" if self.schema else table_name
        ch_table_schema = ", ".join(f"{col} {dtype}" for col, dtype in zip(df.columns, self.get_clickhouse_types(df)))
        engine_clause = f"{engine}({engine_params})" if engine == "ReplicatedMergeTree" and engine_params else engine
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            {ch_table_schema}
        ) ENGINE = {engine_clause} ORDER BY tuple();
        """
        try:
            self.client.command(create_table_query)
            self.logger.info(f"Таблица {full_table_name} создана в ClickHouse.")
        except Exception as error:
            self.logger.error(f"Ошибка при создании таблицы в ClickHouse: {error}")

    def load_data(self, table_name, df):
        """Загружает данные из DataFrame в ClickHouse."""
        full_table_name = f"{self.schema}.{table_name}" if self.schema else table_name
        try:
            self.client.insert_df(full_table_name, df)
            self.logger.info(f"Данные успешно загружены в таблицу {full_table_name} в ClickHouse.")
        except Exception as error:
            self.logger.error(f"Ошибка при загрузке данных в ClickHouse: {error}")

    def transfer_from_postgres(self, postgres_db, pg_table, ch_table, column_mapping=None, engine="MergeTree", engine_params=None):
        """Копирует данные из PostgreSQL в ClickHouse с переименованием колонок."""
        query = f"SELECT * FROM {pg_table};"
        try:
            with postgres_db.conn.cursor() as cursor:
                cursor.execute(query)
                colnames = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                df = pd.DataFrame(rows, columns=colnames)
                self.logger.info(f"Данные из таблицы {pg_table} получены из PostgreSQL.")

                if column_mapping:
                    df.rename(columns=column_mapping, inplace=True)
                    self.logger.info(f"Столбцы переименованы согласно column_mapping: {column_mapping}")

                self.create_table(ch_table, df, engine, engine_params)
                self.load_data(ch_table, df)
                self.logger.info(f"Данные из PostgreSQL успешно загружены в ClickHouse в таблицу {ch_table}.")
        except Exception as error:
            self.logger.error(f"Ошибка при копировании данных из PostgreSQL в ClickHouse: {error}")

    def transfer_to_postgres(self, postgres_db, ch_table, pg_table, column_mapping=None):
        """Копирует данные из ClickHouse в PostgreSQL с переименованием колонок."""
        full_table_name = f"{self.schema}.{ch_table}" if self.schema else ch_table
        try:
            query = f"SELECT * FROM {full_table_name};"
            df = self.client.query_df(query)
            self.logger.info(f"Данные из таблицы {full_table_name} успешно получены из ClickHouse.")

            if column_mapping:
                df.rename(columns=column_mapping, inplace=True)
                self.logger.info(f"Столбцы переименованы согласно column_mapping: {column_mapping}")

            with postgres_db.connection.cursor() as cursor:
                columns = ", ".join([f"{col} {self.get_postgres_type(dtype)}" for col, dtype in zip(df.columns, df.dtypes)])
                create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
                    sql.Identifier(pg_table),
                    sql.SQL(columns)
                )
                cursor.execute(create_table_query)
                postgres_db.connection.commit()
                self.logger.info(f"Таблица {pg_table} создана в PostgreSQL.")

                insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                    sql.Identifier(pg_table),
                    sql.SQL(", ").join(map(sql.Identifier, df.columns))
                )
                execute_values(cursor, insert_query.as_string(cursor), df.values.tolist())
                postgres_db.connection.commit()
                self.logger.info(f"Данные успешно загружены в таблицу {pg_table} в PostgreSQL.")
        except Exception as error:
            self.logger.error(f"Ошибка при копировании данных из ClickHouse в PostgreSQL: {error}")

    @staticmethod
    def get_postgres_type(dtype):
        """Преобразует типы данных Pandas в соответствующие типы PostgreSQL."""
        dtype_mapping = {
            'float64': 'DOUBLE PRECISION',
            'float32': 'REAL',
            'object': 'TEXT',
            'int64': 'BIGINT',
            'int32': 'INTEGER',
            'int16': 'SMALLINT',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
        }
        return dtype_mapping.get(str(dtype), 'TEXT')
