import os
import numpy as np
import pandas as pd
import psycopg2

from config import *


def main():
    # Чтение файла Excel
    df = pd.read_excel('C:\\prog\\wildberries_reviews.xlsx')
    df.head()

    # Очистка имени таблицы
    file = 'wildberries reviews'
    clean_data_table = file.lower().replace(" ", "_") \
                                   .replace("?", "") \
                                   .replace("-", "_") \
                                   .replace(r"/", "_") \
                                   .replace("\\", "_") \
                                   .replace("%", "") \
                                   .replace(")", "") \
                                   .replace(r"(", "") \
                                   .replace("$", "")

    # Очистка заголовков столбцов
    df.columns = [
        x.lower().replace(" ", "_") \
                 .replace("?", "") \
                 .replace("-", "_") \
                 .replace(r"/", "_") \
                 .replace("\\", "_") \
                 .replace("%", "") \
                 .replace(")", "") \
                 .replace(r"(", "") \
                 .replace("$", "")
        for x in df.columns
    ]

    # Маппинг типов данных
    replacements = {
        'float64': 'decimal',
        'object': 'varchar',
        'int64': 'int',
        'datetime64': 'timestamp',
        'timedelta64[ns]': 'varchar'
    }

    col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(replacements)))

    try:
        # Подключение к базе данных
        conn = psycopg2.connect(host="localhost",
                                dbname="",
                                user="",
                                password="")
        
        print("Соединение с базой данных установлено")

        cursor = conn.cursor()

        # Удаляем таблицу с таким же именем, если она существует
        cursor.execute(f"DROP TABLE IF EXISTS {clean_data_table};")

        # Создаем новую таблицу
        cursor.execute(f"CREATE TABLE {clean_data_table} ({col_str});")

        # Сохраняем DataFrame в CSV-файл с указанием кодировки UTF-8
        temp_csv_path = f"{clean_data_table}.csv"
        df.to_csv(temp_csv_path, encoding='utf-8', header=True, index=False)

        # Открываем файл для копирования в базу данных с той же кодировкой
        with open(temp_csv_path, 'r', encoding='utf-8') as my_file:
            print("Файл открыт в памяти")
            
            # Копируем данные из CSV-файла в таблицу
            sql_statement = f"""COPY {clean_data_table} FROM STDIN WITH
                                CSV
                                ENCODING 'UTF8'
                                HEADER
                                DELIMITER AS ',';"""
            cursor.copy_expert(sql=sql_statement, file=my_file)

        # Предоставляем права на выборку всем пользователям
        cursor.execute(f"GRANT SELECT ON TABLE {clean_data_table} TO PUBLIC;")

        # Фиксируем изменения
        conn.commit()

        # Закрытие соединения
        cursor.close()
        conn.close()

        # Удаляем временный CSV-файл
        os.remove(temp_csv_path)

        print(f"Таблица {clean_data_table} импортирована в базу данных успешно")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Произошла ошибка: {error}")
        if conn is not None:
            conn.rollback()
            conn.close()


if __name__ == "__main__":
    main()