import logging
from ClickHouseDatabase import ClickHouseDatabase
from PostgresDatabase import PostgresDatabase

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_manager.log"),
        logging.StreamHandler()
    ]
)

class ETLManager:
    def __init__(self):
        # Логгер для класса
        self.logger = logging.getLogger(self.__class__.__name__)

        # Подключение к PostgreSQL
        self.pg_db = PostgresDatabase(
            host="localhost",
            database="test_etl",
            user="postgres",
            password="1234"
        )
        self.logger.info("Попытка подключения к PostgreSQL.")
        self.pg_db.connect()

        # Подключение к ClickHouse
        self.ch_db = ClickHouseDatabase(
            host="localhost",
            port=8123,
            user="default",
            password=""
        )
        self.logger.info("Попытка подключения к ClickHouse.")
        self.ch_db.connect()

        # Настройка переименования колонок
        self.column_mapping = {
            "автор": "author",
            "дата": "date",
            "отзыв": "review",
            "продукт": "product",
            "артикул": "article"
        }

    def load_data_from_xlsx_to_postgres(self):
        """Загрузка данных из файла xlsx в PostgreSQL."""
        self.logger.info("Начало загрузки данных из XLSX в PostgreSQL.")
        try:
            self.pg_db.process_data(
                'wildberries_reviews.xlsx',
                self.column_mapping,
                table_name="test_t_re"
            )
            self.logger.info("Данные из XLSX успешно загружены в PostgreSQL.")
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке данных из XLSX в PostgreSQL: {e}")

    def transfer_data_from_clickhouse_to_postgres(self):
        """Перенос данных из ClickHouse в PostgreSQL."""
        self.logger.info("Начало переноса данных из ClickHouse в PostgreSQL.")
        try:
            if not self.pg_db.conn or not self.ch_db.client:
                self.logger.error("Соединения с базами данных не установлены.")
                return
            
            self.pg_db.transfer_from_clickhouse(
                clickhouse_db=self.ch_db,
                ch_table="test_t_re_ch",
                pg_table="test_t_re_ch",
                column_mapping=self.column_mapping
            )
            self.logger.info("Данные успешно перенесены из ClickHouse в PostgreSQL.")
        except Exception as e:
            self.logger.error(f"Ошибка при переносе данных из ClickHouse в PostgreSQL: {e}")

    def transfer_data_from_postgres_to_clickhouse(self):
        """Перенос данных из PostgreSQL в ClickHouse."""
        self.logger.info("Начало переноса данных из PostgreSQL в ClickHouse.")
        try:
            if not self.pg_db.conn or not self.ch_db.client:
                self.logger.error("Соединения с базами данных не установлены.")
                return

            self.ch_db.transfer_from_postgres(
                postgres_db=self.pg_db,
                pg_table="test_t_re",
                ch_table="test_t_re_ch",
                engine="MergeTree",
                engine_params="'/clickhouse/tables/{shard}/test_t_re_ch', '{replica}'",
                column_mapping=self.column_mapping
            )
            self.logger.info("Данные успешно перенесены из PostgreSQL в ClickHouse.")
        except Exception as e:
            self.logger.error(f"Ошибка при переносе данных из PostgreSQL в ClickHouse: {e}")

    def show_menu(self):
        """Отображает меню для выбора действия."""
        print("\nВыберите действие:")
        print("1. Загрузить данные из xlsx в PostgreSQL")
        print("2. Перенести данные из ClickHouse в PostgreSQL")
        print("3. Перенести данные из PostgreSQL в ClickHouse")
        print("4. Выйти")

        choice = input("Введите номер действия: ")
        self.logger.info(f"Пользователь выбрал действие: {choice}")
        
        if choice == "1":
            self.load_data_from_xlsx_to_postgres()
        elif choice == "2":
            self.transfer_data_from_clickhouse_to_postgres()
        elif choice == "3":
            self.transfer_data_from_postgres_to_clickhouse()
        elif choice == "4":
            self.logger.info("Завершение программы.")
            return False
        else:
            self.logger.warning("Некорректный выбор. Пожалуйста, попробуйте снова.")
            print("Некорректный выбор. Пожалуйста, попробуйте снова.")
        return True

    def close_connections(self):
        """Закрывает соединения с базами данных."""
        self.logger.info("Закрытие соединений с базами данных.")
        self.pg_db.disconnect()
        self.ch_db.disconnect()

    def run(self):
        """Запускает меню и выполняет выбранные действия."""
        try:
            while True:
                if not self.show_menu():
                    break
        finally:
            self.close_connections()
