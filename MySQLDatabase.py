import mysql.connector
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MySQLConnector:
    def __init__(self, user, password, host, database):
        self.user = user
        self.password = password
        self.host = host
        self.database = database

    def connect(self):
        try:
            self.conn = mysql.connector.connect(user=self.user, password=self.password, host=self.host, database=self.database)
            logger.info("Подключение к MySQL установлено.")
        except mysql.connector.Error as e:
            logger.error(f"Ошибка подключения к базе данных: {e}")

    def close_connection(self):
        if hasattr(self, 'conn') and self.conn.is_connected():
            self.conn.close()
            logger.info("Подключение к MySQL закрыто.")


connector = MySQLConnector('root', 'Fiksik177!', 'localhost', 'mysql_test')
connector.connect()
connector.close_connection()