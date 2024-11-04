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

    def rename_columns(self, df, column_mapping):
        """Переименовывает столбцы DataFrame согласно переданному словарю."""
        # Очистка имен столбцов DataFrame
        df.columns = [self.clean_name(col) for col in df.columns]
        
        # Очистка и переименование колонок
        cleaned_column_mapping = {self.clean_name(key): value for key, value in column_mapping.items()}
        df = df.rename(columns=cleaned_column_mapping)
        
        # Проверка наличия всех колонок в DataFrame после переименования
        missing_columns = [col for col in cleaned_column_mapping if col not in df.columns]
        if missing_columns:
            print(f"Предупреждение: отсутствуют колонки в DataFrame: {missing_columns}")
        
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

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Произошла ошибка при загрузке данных: {error}")
            self.conn.rollback()
        finally:
            # Удаляет временный CSV-файл
            if os.path.exists(temp_csv_path):
                os.remove(temp_csv_path)

    def process_data(self, data_source, column_mapping, table_name=None):
        """
        Основная функция обработки данных.
        :param data_source: Путь к файлу Excel или другой источник данных.
        :param column_mapping: Словарь для переименования колонок.
        :param table_name: Имя таблицы для создания/обновления.
        """
        try:
            # Чтение данных из источника
            if isinstance(data_source, str):  # Предполагаем, что это путь к файлу Excel
                df = pd.read_excel(data_source)
            elif isinstance(data_source, pd.DataFrame):  # Уже готовый DataFrame
                df = data_source
            else:
                raise ValueError("Неверный тип данных для параметра data_source.")

            # Переименовывание столбцов
            df = self.rename_columns(df, column_mapping)

            # Очистка имени таблицы
            if table_name is None:
                if isinstance(data_source, str):
                    table_name = self.clean_name(os.path.splitext(os.path.basename(data_source))[0])
                else:
                    raise ValueError("Не удалось определить имя таблицы. Пожалуйста, укажите его явно.")

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
        password=""   # Укажите пароль
    )

    # Устанавливаем соединение
    db_importer.connect()

    # Пример использования с файлом Excel
    column_mapping_1 = {
        "Автор": "autorrr",
        "Дата": "data",
        "Отзыв": "otziv",
        "Продукт": "produkt",
        "Артикул": "arikul",
        # Добавьте остальные колонки здесь...
    }
    db_importer.process_data(
        'wildberries_reviews.xlsx',
        column_mapping_1,
        table_name="test"  # Укажите новое имя таблицы, если нужно
    )

    # Закрываем соединение
    db_importer.disconnect()


if __name__ == "__main__":
    main()











        # host="localhost",
        # database="test_etl",  # Укажите название вашей базы данных
        # user="postgres",      # Укажите вашего пользователя
        # password=""   # Укажите пароль



    # Пример использования с другим источником данных (например, DataFrame)
    # df = pd.DataFrame({
    #     'A': [1, 2, 3],
    #     'B': ['a', 'b', 'c']
    # })
    # column_mapping_2 = {
    #     "Column_A": "A",
    #     "Column_B": "B"
    # }
    # db_importer.process_data(
    #     df,
    #     column_mapping_2,
    #     table_name="another_table"
    # )

    # Закрываем соединение
