from ClickHouseDatabase import ClickHouseDatabase
from PostgresDatabase import PostgresDatabase


class ETLManager:
    def __init__(self):
        # Подключение к PostgreSQL
        self.pg_db = PostgresDatabase(
            host="localhost",
            database="test_etl",
            user="postgres",
            password="1234"
        )
        self.pg_db.connect()

        # Подключение к ClickHouse
        self.ch_db = ClickHouseDatabase(
            host="localhost",
            port=8123,
            user="default",
            password=""
        )
        self.ch_db.connect()

        # Настройка переименования колонок
        self.column_mapping = {
            "author": "автор",
            "date": "дата",
            "review": "отзыв",
            "product": "продукт",
            "article": "артикул"
        }

    def load_data_from_xlsx_to_postgres(self):
        """Загрузка данных из файла xlsx в PostgreSQL."""
        self.pg_db.process_data(
            'wildberries_reviews.xlsx',
            self.column_mapping,
            table_name="test_t_re"
        )

    def transfer_data_from_clickhouse_to_postgres(self):
        """Перенос данных из ClickHouse в PostgreSQL."""
        self.pg_db.transfer_from_clickhouse(
            clickhouse_db=self.ch_db,
            ch_table="mydb",
            pg_table="clickhouse_reviews_mydb",
            column_mapping=self.column_mapping
        )

    def transfer_data_from_postgres_to_clickhouse(self):
        """Перенос данных из PostgreSQL в ClickHouse."""
        self.ch_db.transfer_from_postgres(
            postgres_db=self.pg_db,
            pg_table="test_t_re",
            ch_table="test_t_re_ch",
            engine="MergeTree",
            engine_params="'/clickhouse/tables/{shard}/test_t_re_ch', '{replica}'"
        )

    def show_menu(self):
        """Отображает меню для выбора действия."""
        print("\nВыберите действие:")
        print("1. Загрузить данные из xlsx в PostgreSQL")
        print("2. Перенести данные из ClickHouse в PostgreSQL")
        print("3. Перенести данные из PostgreSQL в ClickHouse")
        print("4. Выйти")

        choice = input("Введите номер действия: ")
        
        if choice == "1":
            self.load_data_from_xlsx_to_postgres()
        elif choice == "2":
            self.transfer_data_from_clickhouse_to_postgres()
        elif choice == "3":
            self.transfer_data_from_postgres_to_clickhouse()
        elif choice == "4":
            print("Завершение программы.")
            return False
        else:
            print("Некорректный выбор. Пожалуйста, попробуйте снова.")
        return True

    def close_connections(self):
        """Закрывает соединения с базами данных."""
        self.pg_db.disconnect()
        self.ch_db.disconnect()

    def run(self):
        """Запускает меню и выполняет выбранные действия."""
        while True:
            if not self.show_menu():
                break
        self.close_connections()

