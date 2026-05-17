import mysql.connector
from mysql.connector import errorcode
import logging
from collections import Counter

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Данные для подключения к базе данных MySQL (такие же, как в parserWebhook.py)
DB_CONFIG = {
    'user': 'mufer',
    'password': 'vRZVgh6c',
    'host': 'localhost',
    'database': 'Data_Science',
}

TABLE_NAME = 'b24_leads'

def count_leads_by_region():
    """
    Подключается к БД, подсчитывает количество лидов по каждому региону
    и выводит результат в консоль.
    """
    try:
        # Устанавливаем соединение с БД
        db_connection = mysql.connector.connect(**DB_CONFIG)
        logging.info("Успешное подключение к базе данных MySQL.")
        cursor = db_connection.cursor()

        # SQL-запрос для подсчета лидов по регионам
        query = f"SELECT region, COUNT(*) FROM {TABLE_NAME} GROUP BY region ORDER BY COUNT(*) DESC"
        
        logging.info("Выполнение запроса для подсчета лидов по регионам...")
        cursor.execute(query)
        
        results = cursor.fetchall()
        
        if not results:
            logging.info("В таблице нет данных о регионах для подсчета.")
            return

        # Вывод результатов
        print("\n--- Количество лидов по регионам ---")
        for region, count in results:
            # Для "пустых" регионов (None) выводим более понятное название
            region_name = region if region is not None else "Не определен"
            print(f"{region_name}: {count}")
        print("--------------------------------------\n")


    except mysql.connector.Error as err:
        logging.error(f"Ошибка при работе с базой данных: {err}")
    finally:
        if 'db_connection' in locals() and db_connection.is_connected():
            cursor.close()
            db_connection.close()
            logging.info("Соединение с БД закрыто.")

if __name__ == "__main__":
    count_leads_by_region()
