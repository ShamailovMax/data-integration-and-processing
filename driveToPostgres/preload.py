import os
import pandas as pd 
import psycopg2 

class DatabaseImporter:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.conn = None
        self.cursor = None

    def connect(self):
        """Устанавливает соединение с базой данных."""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                dbname=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.conn.cursor()
            print("Соединение с базой данных установлено.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Произошла ошибка при подключении: {error}")
    
    def disconnect(self):
        """Закрывает соединение с базой данных."""
        if self.conn is not None:
            self.cursor.close()
            self.conn.close()
            print("Соединение закрыто.")

    @staticmethod
    def clean_name(name):
        """Очищает имя от недопустимых символов."""
        return name.lower().replace(" ", "_") \
                         .replace("?", "") \
                         .replace("-", "_") \
                         .replace(r"/", "_") \
                         .replace("\\", "_") \
                         .replace("%", "") \
                         .replace(")", "") \
                         .replace(r"(", "") \
                         .replace("$", "")

    def rename_columns(self, df):
        """Переименовывает столбцы DataFrame."""
        df.columns = [self.clean_name(col) for col in df.columns]
        return df

    def create_table(self, table_name, df):
        """Создает таблицу в базе данных."""
        replacements = {
            'float64': 'decimal',
            'object': 'varchar',
            'int64': 'int',
            'datetime64': 'timestamp',
            'timedelta64[ns]': 'varchar'
        }

        col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(replacements)))

        try:
            # Удаляет старую таблицу, если она существует
            self.cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
            # Создает новую таблицу
            self.cursor.execute(f"CREATE TABLE {table_name} ({col_str});")
            self.conn.commit()
            print(f"Таблица {table_name} создана.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Произошла ошибка при создании таблицы: {error}")
            self.conn.rollback()

    def load_data_to_db(self, df, table_name):
        """Загружает данные из DataFrame в таблицу базы данных."""
        temp_csv_path = f"{table_name}.csv"
        df.to_csv(temp_csv_path, encoding='utf-8', header=True, index=False)

        try:
            with open(temp_csv_path, 'r', encoding='utf-8') as my_file:
                print("Файл открыт в памяти")
                
                # Копирует данные из CSV-файла в таблицу
                sql_statement = f"""COPY {table_name} FROM STDIN WITH
                                    CSV
                                    ENCODING 'UTF8'
                                    HEADER
                                    DELIMITER AS ',';"""
                self.cursor.copy_expert(sql=sql_statement, file=my_file)

            # Предоставляет права на выборку всем пользователям
            self.cursor.execute(f"GRANT SELECT ON TABLE {table_name} TO PUBLIC;")

            # Фиксирует изменения
            self.conn.commit()

            # Удаляет временный CSV-файл
            os.remove(temp_csv_path)

            print(f"Данные загружены в таблицу {table_name} успешно.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Произошла ошибка при загрузке данных: {error}")
            self.conn.rollback()

    def process_data(self, excel_file):
        """Основная функция обработки данных."""
        try:
            # Чтение файла Excel
            df = pd.read_excel(excel_file)
            df.head()

            # Переименовывание столбцов
            df = self.rename_columns(df)

            # Очистка имени таблицы
            table_name = self.clean_name(os.path.splitext(os.path.basename(excel_file))[0])

            # Создание таблицы
            self.create_table(table_name, df)

            # Загрузка данных в базу данных
            self.load_data_to_db(df, table_name)

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Произошла ошибка: {error}")
            self.conn.rollback()


def main():
    # Настройки подключения к базе данных
    db_importer = DatabaseImporter(
        host="localhost",
        database="test_etl",  # Укажите название вашей базы данных
        user="postgres",      # Укажите вашего пользователя
        password="1234"   # Укажите пароль
    )

    # Устанавливаем соединение
    db_importer.connect()

    # Обрабатываем данные
    db_importer.process_data('wildberries_reviews.xlsx')

    # Закрываем соединение
    db_importer.disconnect()


if __name__ == "__main__":
    main()