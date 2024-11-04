import pandas as pd
import clickhouse_connect

class ClickHouseDatabase:
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.client = None

    def connect(self):
        """Подключается к ClickHouse."""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password
            )
            print("Соединение с ClickHouse установлено.")
        except Exception as error:
            print(f"Ошибка подключения к ClickHouse: {error}")
            self.client = None

    def disconnect(self):
        """Отключается от ClickHouse."""
        if self.client:
            self.client.close()
            print("Соединение с ClickHouse закрыто.")

    @staticmethod
    def get_clickhouse_types(df):
        """Сопоставляет типы данных Pandas с типами ClickHouse."""
        dtype_mapping = {
            'float64': 'Float64',
            'object': 'String',
            'int64': 'Int64',
            'datetime64[ns]': 'DateTime',
            'timedelta64[ns]': 'String'
        }
        return [dtype_mapping.get(str(dtype), 'String') for dtype in df.dtypes]

    def create_table(self, table_name, df, engine="MergeTree", engine_params=None):
        """Создает таблицу в ClickHouse."""
        ch_table_schema = ", ".join(f"{col} {dtype}" for col, dtype in zip(df.columns, self.get_clickhouse_types(df)))
        engine_clause = f"{engine}({engine_params})" if engine == "ReplicatedMergeTree" and engine_params else engine
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {ch_table_schema}
        ) ENGINE = {engine_clause} ORDER BY tuple();
        """
        try:
            self.client.command(create_table_query)
            print(f"Таблица {table_name} создана в ClickHouse.")
        except Exception as error:
            print(f"Ошибка при создании таблицы в ClickHouse: {error}")

    def load_data(self, table_name, df):
        """Загружает данные из DataFrame в ClickHouse."""
        try:
            self.client.insert_df(table_name, df)
            print(f"Данные успешно загружены в таблицу {table_name} в ClickHouse.")
        except Exception as error:
            print(f"Ошибка при загрузке данных в ClickHouse: {error}")

    def transfer_from_postgres(self, postgres_db, pg_table, ch_table, column_mapping=None, engine="MergeTree", engine_params=None):
        """Копирует данные из PostgreSQL в ClickHouse с переименованием колонок."""
        query = f"SELECT * FROM {pg_table};"
        try:
            postgres_db.cursor.execute(query)
            colnames = [desc[0] for desc in postgres_db.cursor.description]
            rows = postgres_db.cursor.fetchall()
            df = pd.DataFrame(rows, columns=colnames)
            if column_mapping:
                df = postgres_db.rename_columns(df, column_mapping)
            self.create_table(ch_table, df, engine, engine_params)
            self.load_data(ch_table, df)
        except Exception as error:
            print(f"Ошибка при копировании данных из PostgreSQL в ClickHouse: {error}")


