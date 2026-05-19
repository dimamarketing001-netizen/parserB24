import csv
import os
import logging
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv

# --- ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ---
load_dotenv()

# --- НАСТРОЙКИ ---

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('export_phones.log'),
        logging.StreamHandler()
    ],
    force=True
)

# Данные для подключения к базе данных MySQL
DB_CONFIG = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'database': os.getenv('DB_NAME'),
}

TABLE_NAME = 'b24_leads'

# Регионы для выгрузки
TARGET_REGIONS = [
    'Челябинская обл.',
    'Челябинская область',
]

# Имя выходного файла
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(OUTPUT_DIR, f"phones_chelyabinsk_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")


if __name__ == "__main__":
    logging.info("=" * 60)
    logging.info("Запуск экспорта телефонов из БД")

    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        logging.info("Подключение к БД успешно.")

        # Формируем запрос
        region_placeholders = ', '.join(['%s'] * len(TARGET_REGIONS))
        query = f"""
            SELECT DISTINCT phone
            FROM {TABLE_NAME}
            WHERE region IN ({region_placeholders})
              AND phone IS NOT NULL
              AND phone != ''
            ORDER BY phone
        """

        cursor.execute(query, tuple(TARGET_REGIONS))
        rows = cursor.fetchall()

        logging.info(f"Найдено {len(rows)} уникальных номеров.")

        if rows:
            with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=';')
                writer.writerow(['phone'])
                for row in rows:
                    writer.writerow([row[0]])

            logging.info(f"Файл сохранён: {OUTPUT_FILE}")
            logging.info(f"Всего записей: {len(rows)}")
        else:
            logging.warning("Нет данных для экспорта.")

    except mysql.connector.Error as err:
        logging.error(f"Ошибка БД: {err}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            logging.info("Соединение с БД закрыто.")

    logging.info("Экспорт завершён.")