from PostgresDatabase import PostgresDatabase
from ClickHouseDatabase import ClickHouseDatabase

def main():
    # Подключение к PostgreSQL
    pg_db = PostgresDatabase(
        host="localhost",
        database="test_etl",
        user="postgres",
        password="1234",
        # schema="tap_csv"
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

    # Настройка переименования колонок
    column_mapping = {
        "author": "автор",
        "date": "дата",
        "review": "отзыв",
        "product": "продукт",
        "article": "артикул"
    }
    # Из xlsx в PostgreSQL
    # pg_db.process_data(
    #     'wildberries_reviews.xlsx',
    #     column_mapping_1,
    #     table_name="test_t_re"
    # )

    # Перенос данных из ClickHouse в PostgreSQL с переименованием таблицы и колонок
    pg_db.transfer_from_clickhouse(
        clickhouse_db=ch_db,          # Объект подключения к базе данных ClickHouse
        ch_table="clickhouse_reviews_re",  # Имя существующей таблицы в ClickHouse
        pg_table="clickhouse_reviews_ee",  # Имя новой таблицы в PostgreSQL
        column_mapping=column_mapping      # Словарь для переименования колонок
    )

    # Перенос данных из PostgreSQL в ClickHouse
    # ch_db.transfer_from_postgres(
    #     postgres_db=pg_db,
    #     pg_table="test",  # Указываем только имя таблицы
    #     ch_table="mydb",
    #     # column_mapping=column_mapping_1,
    #     engine="MergeTree",
    #     engine_params="'/clickhouse/tables/{shard}/test', '{replica}'"
    # )

    # Отключение от баз данных
    pg_db.disconnect()
    ch_db.disconnect()


if __name__ == "__main__":
    main()
