from PostgresDatabase import PostgresDatabase
from ClickHouseDatabase import ClickHouseDatabase

def main():
    # Подключение к PostgreSQL
    pg_db = PostgresDatabase(
        host="localhost",
        database="test_etl",
        user="postgres",
        password="1234"
    )
    pg_db.connect()

    # Подключение к ClickHouse
    ch_db = ClickHouseDatabase(
        host="localhost",
        port=8123,
        user="default",
        password=""
    )
    ch_db.connect()

    # Настройки для PostgreSQL и Excel
    column_mapping_1 = {
        "автор": "au",
        "дата": "da",
        "отзыв": "ot",
        "продукт": "pr",
        "артикул": "ar",
    }
    # pg_db.process_data(
    #     'wildberries_reviews.xlsx',
    #     column_mapping_1,
    #     table_name="test_t_re"
    # )

    # Перенос данных из PostgreSQL в ClickHouse
    ch_db.transfer_from_postgres(
        postgres_db=pg_db,
        pg_table="test_t",
        ch_table="clickhouse_reviews_re",
        column_mapping=column_mapping_1,
        engine="MergeTree",
        engine_params="'/clickhouse/tables/{shard}/clickhouse_reviews_re', '{replica}'"
    )

    # Отключение от баз данных
    pg_db.disconnect()
    ch_db.disconnect()


if __name__ == "__main__":
    main()
