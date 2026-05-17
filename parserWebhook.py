import requests
import mysql.connector
import logging
import re
import sys
import time
from datetime import date, datetime, timedelta
import fcntl
import os
from dotenv import load_dotenv

# --- ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ---
load_dotenv()

# --- НАСТРОЙКИ ---

# Настройка логирования для вывода информации в консоль
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='bb.log', force=True)

# Укажите дату, с которой начинать выгрузку лидов, в формате ГГГГ-ММ-ДД
START_DATE = "2026-03-01"

# Список вебхуков Bitrix24 (из переменных окружения)
_raw_webhooks = os.getenv('BITRIX_WEBHOOKS', '')
WEBHOOKS = sorted(list(set([
    webhook.strip() for webhook in _raw_webhooks.split(',') if webhook.strip()
])))

# Данные для подключения к базе данных MySQL (из переменных окружения)
DB_CONFIG = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'database': os.getenv('DB_NAME'),
}

TABLE_NAME = 'b24_leads'

# --- ЛОГИКА СКРИПТА ---

def create_leads_table(conn):
    """Создает таблицу для лидов и добавляет необходимые колонки, если они не существуют."""
    cursor = conn.cursor()

    # 1. Создаём таблицу если не существует
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
        `webhook_source` VARCHAR(255) NOT NULL,
        `lead_id` INT UNSIGNED NOT NULL,
        `title` VARCHAR(255),
        `name` VARCHAR(255),
        `last_name` VARCHAR(255),
        `second_name` VARCHAR(255),
        `status_id` VARCHAR(50),
        `source_id` VARCHAR(50),
        `assigned_by_id` INT,
        `date_create` DATETIME,
        `date_modify` DATETIME,
        `comments` TEXT,
        `company_title` VARCHAR(255),
        `phone` VARCHAR(255),
        `email` VARCHAR(255),
        `web` VARCHAR(255),
        `address` TEXT,
        `utm_source` VARCHAR(255),
        `utm_medium` VARCHAR(255),
        `utm_campaign` VARCHAR(255),
        `utm_content` VARCHAR(255),
        `utm_term` VARCHAR(255),
        `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (`webhook_source`, `lead_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        logging.info(f"Проверка и создание таблицы `{TABLE_NAME}`...")
        cursor.execute(create_table_query)
        conn.commit()

        # 2. Определяем полный эталонный список колонок и их определений
        # Формат: 'имя_колонки': ('ОПРЕДЕЛЕНИЕ ТИПА', 'AFTER `колонка`')
        required_columns = {
            'webhook_source': ('VARCHAR(255) NOT NULL', None),
            'lead_id':        ('INT UNSIGNED NOT NULL', None),
            'title':          ('VARCHAR(255)', None),
            'name':           ('VARCHAR(255)', None),
            'last_name':      ('VARCHAR(255)', None),
            'second_name':    ('VARCHAR(255)', None),
            'status_id':      ('VARCHAR(50)', None),
            'source_id':      ('VARCHAR(50)', None),
            'assigned_by_id': ('INT', None),
            'date_create':    ('DATETIME', None),
            'date_modify':    ('DATETIME', None),
            'comments':       ('TEXT', None),
            'company_title':  ('VARCHAR(255)', None),
            'phone':          ('VARCHAR(255)', None),
            'email':          ('VARCHAR(255)', None),
            'web':            ('VARCHAR(255)', None),
            'address':        ('TEXT', None),
            'region':         ('VARCHAR(255)', 'address'),
            'phone_carrier':  ('VARCHAR(100)', 'region'),
            'old_operator':   ('VARCHAR(100)', 'phone_carrier'),
            'utm_source':     ('VARCHAR(255)', None),
            'utm_medium':     ('VARCHAR(255)', None),
            'utm_campaign':   ('VARCHAR(255)', None),
            'utm_content':    ('VARCHAR(255)', None),
            'utm_term':       ('VARCHAR(255)', None),
            'last_updated':   ('TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP', None),
        }

        # 3. Получаем список существующих колонок из БД одним запросом
        cursor.execute(f"SHOW COLUMNS FROM `{TABLE_NAME}`;")
        existing_columns = {row[0] for row in cursor.fetchall()}
        logging.info(f"Существующие колонки в таблице: {existing_columns}")

        # 4. Проверяем и добавляем отсутствующие колонки
        for col_name, (col_definition, after_col) in required_columns.items():
            if col_name not in existing_columns:
                logging.warning(f"Колонка '{col_name}' не найдена в таблице `{TABLE_NAME}`. Добавляю...")
                after_clause = f"AFTER `{after_col}`" if after_col else ""
                alter_query = f"ALTER TABLE `{TABLE_NAME}` ADD COLUMN `{col_name}` {col_definition} {after_clause};"
                cursor.execute(alter_query)
                conn.commit()
                logging.info(f"Колонка '{col_name}' успешно добавлена.")

        # 5. Удаляем устаревшие колонки
        old_phone_columns = ['phone_timezone', 'phone_region_geocoder', 'phone_region_tz']
        for col in old_phone_columns:
            if col in existing_columns:
                logging.warning(f"Найдена устаревшая колонка '{col}'. Удаляю...")
                cursor.execute(f"ALTER TABLE `{TABLE_NAME}` DROP COLUMN `{col}`;")
                conn.commit()
                logging.info(f"Устаревшая колонка '{col}' удалена.")

        logging.info(f"Таблица `{TABLE_NAME}` полностью готова к работе.")

    except mysql.connector.Error as err:
        logging.error(f"Не удалось подготовить таблицу: {err}")
        sys.exit(1)
    finally:
        cursor.close()


def normalize_phone(raw_phone: str):
    """
    Нормализует и валидирует номер телефона.
    Возвращает номер в формате 79999999999 или None, если номер некорректный.
    """
    if not raw_phone or not isinstance(raw_phone, str):
        return None

    digits = re.sub(r'\D', '', raw_phone)

    if len(digits) == 11 and digits.startswith('8'):
        return '7' + digits[1:]
    if len(digits) == 10 and digits.startswith('9'):
        return '7' + digits
    if len(digits) == 11 and digits.startswith('7'):
        return digits

    return None


def get_phone_info(phone: str) -> dict:
    """
    Получает информацию о регионе и операторе по номеру телефона.
    """
    info = {
        "region": None,
        "operator": None,
        "old_operator": None
    }
    if not phone:
        return info

    url = "http://num.voxlink.ru/get/"
    params = {"num": phone}

    try:
        response = requests.get(url, params=params, timeout=5)
        response.raise_for_status()

        data = response.json()
        logging.info(f"Для номера {phone} получен ответ от num.voxlink.ru: {data}")

        info["region"] = data.get("region")
        info["operator"] = data.get("operator")
        info["old_operator"] = data.get("old_operator")

    except (requests.exceptions.RequestException, ValueError) as e:
        logging.error(f"Ошибка при запросе к num.voxlink.ru для номера {phone}: {e}. Запись будет создана без данных о регионе.")

    return info


def get_existing_lead_dates(conn, webhook_source: str, start_date: date) -> set[date]:
    """
    Получает из БД множество дат, за которые уже есть лиды для данного портала.
    """
    cursor = conn.cursor()

    query = """
        SELECT DISTINCT DATE(date_create)
        FROM b24_leads
        WHERE webhook_source = %s AND date_create >= %s
    """

    try:
        logging.info(f"Получение существующих дат для '{webhook_source}'...")
        cursor.execute(query, (webhook_source, start_date.strftime('%Y-%m-%d')))
        existing_dates = {row[0] for row in cursor.fetchall() if row[0]}
        logging.info(f"Найдено {len(existing_dates)} уникальных дат в БД.")
        return existing_dates

    except mysql.connector.Error as err:
        logging.error(f"Ошибка при получении дат из БД: {err}")
        return set()

    finally:
        cursor.close()


def calculate_missing_date_ranges(existing_dates: set[date], start_date: date) -> list[dict]:
    """
    Вычисляет НЕПРЕРЫВНЫЕ ДИАПАЗОНЫ дат, отсутствующие в БД, и создает для них фильтры.
    """
    today = date.today()
    if start_date > today:
        return []

    all_possible_dates = {start_date + timedelta(days=x) for x in range((today - start_date).days + 1)}
    missing_dates = sorted(list(all_possible_dates - existing_dates))

    if not missing_dates:
        return []

    filters = []
    range_start = missing_dates[0]

    for i in range(1, len(missing_dates)):
        if missing_dates[i] != missing_dates[i-1] + timedelta(days=1):
            range_end = missing_dates[i-1]
            filters.append({
                ">=DATE_CREATE": range_start.strftime('%Y-%m-%dT00:00:00'),
                "<=DATE_CREATE": range_end.strftime('%Y-%m-%dT23:59:59')
            })
            range_start = missing_dates[i]

    filters.append({
        ">=DATE_CREATE": range_start.strftime('%Y-%m-%dT00:00:00'),
        "<=DATE_CREATE": missing_dates[-1].strftime('%Y-%m-%dT23:59:59')
    })

    logging.info(f"Сформировано {len(filters)} фильтров по диапазонам дат вместо {len(missing_dates)} отдельных дней.")
    return filters


def get_all_leads(webhook_url, date_filter):
    """Получает все лиды с портала Bitrix24, используя заданный фильтр по дате."""
    leads_for_period = []
    start = 0
    method = "crm.lead.list"

    while True:
        params = {
            'order': {"DATE_CREATE": "ASC"},
            'filter': date_filter,
            'select': [
                "ID", "TITLE", "NAME", "LAST_NAME", "SECOND_NAME",
                "STATUS_ID", "SOURCE_ID", "ASSIGNED_BY_ID",
                "DATE_CREATE", "DATE_MODIFY", "COMMENTS", "COMPANY_TITLE",
                "ADDRESS",
                "PHONE", "EMAIL", "WEB",
                "UTM*",
                "phoneWork", "phoneMobile", "emailWork", "emailHome"
            ],
            'start': start
        }
        try:
            logging.info(f"Запрос к Bitrix24 API: метод={method}, start={start}")
            response = requests.post(f"{webhook_url}{method}", json=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if 'result' in data and data['result']:
                fetched_count = len(data['result'])
                leads_for_period.extend(data['result'])
                logging.info(f"Получено {fetched_count} лидов. Всего за этот вызов: {len(leads_for_period)}")

                if 'next' in data:
                    start = data['next']
                else:
                    logging.info("Все страницы для данного периода загружены.")
                    break
            else:
                logging.info("На этой странице нет результатов. Завершение для данного периода.")
                break

        except requests.exceptions.Timeout:
            logging.error(f"Тайм-аут запроса к {webhook_url} (превышено 30 секунд).")
            break
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка при запросе к {webhook_url}: {e}")
            break

    return leads_for_period


def save_leads_to_db(conn, leads, webhook_source, phone_info_cache):
    """Сохраняет или обновляет лиды в базе данных, пропуская дубликаты по номеру телефона."""
    if not leads:
        return 0

    cursor = conn.cursor()

    # --- Шаг 1: Извлечь все корректные номера телефонов из пачки лидов ---
    lead_phone_map = {}
    for lead in leads:
        raw_phone = None
        if lead.get('phoneWork'):
            raw_phone = lead.get('phoneWork')
        elif lead.get('phoneMobile'):
            raw_phone = lead.get('phoneMobile')
        elif lead.get('phone') and isinstance(lead.get('phone'), str):
            raw_phone = lead.get('phone')
        elif lead.get('PHONE') and isinstance(lead.get('PHONE'), list) and len(lead.get('PHONE')) > 0:
            for p_entry in lead['PHONE']:
                if p_entry.get('VALUE_TYPE') in ('WORK', 'MOBILE') and p_entry.get('VALUE'):
                    raw_phone = p_entry['VALUE']
                    break
            if not raw_phone and lead['PHONE'][0].get('VALUE'):
                raw_phone = lead['PHONE'][0]['VALUE']

        normalized_phone = normalize_phone(raw_phone)
        if normalized_phone:
            if normalized_phone not in lead_phone_map:
                lead_phone_map[normalized_phone] = []
            lead_phone_map[normalized_phone].append(lead)

    if not lead_phone_map:
        logging.info("В полученной пачке нет лидов с корректными номерами телефонов.")
        cursor.close()
        return 0

    # --- Шаг 2: Проверить, какие из этих номеров уже существуют в БД ---
    existing_phones = set()
    query_placeholders = ', '.join(['%s'] * len(lead_phone_map))
    check_query = f"SELECT DISTINCT phone FROM {TABLE_NAME} WHERE phone IN ({query_placeholders})"

    try:
        cursor.execute(check_query, tuple(lead_phone_map.keys()))
        for (phone,) in cursor.fetchall():
            existing_phones.add(phone)
        if existing_phones:
            logging.info(f"Найдено {len(existing_phones)} уже существующих номеров в БД из {len(lead_phone_map)} уникальных номеров в этой пачке.")
    except mysql.connector.Error as err:
        logging.error(f"Ошибка при проверке существующих номеров в БД: {err}")
        cursor.close()
        return 0

    # --- Шаг 3: Подготовить данные только для новых, уникальных лидов ---
    data_to_insert = []
    new_phones = set(lead_phone_map.keys()) - existing_phones

    if not new_phones:
        logging.info(f"Нет новых уникальных номеров для добавления. Пропущено {len(leads)} лидов.")
        cursor.close()
        return 0

    logging.info(f"Будет обработано {len(new_phones)} новых уникальных номеров.")

    for phone_number in new_phones:
        lead = lead_phone_map[phone_number][0]

        email = None
        if lead.get('emailWork'):
            email = lead.get('emailWork')
        elif lead.get('emailHome'):
            email = lead.get('emailHome')
        elif lead.get('email'):
            email = lead.get('email')
        elif lead.get('EMAIL') and isinstance(lead.get('EMAIL'), list) and len(lead.get('EMAIL')) > 0:
            for e_entry in lead['EMAIL']:
                if e_entry.get('VALUE_TYPE') == 'WORK' and e_entry.get('VALUE'):
                    email = e_entry['VALUE']
                    break
            if not email and lead['EMAIL'][0].get('VALUE'):
                email = lead['EMAIL'][0]['VALUE']

        web = (
            lead.get('web') if lead.get('web')
            else (
                lead.get('WEB')[0]['VALUE']
                if lead.get('WEB') and isinstance(lead.get('WEB'), list) and len(lead.get('WEB')) > 0
                else None
            )
        )

        if phone_number in phone_info_cache:
            phone_info = phone_info_cache[phone_number]
            logging.info(f"Информация для номера {phone_number} взята из кэша.")
        else:
            phone_info = get_phone_info(phone_number)
            phone_info_cache[phone_number] = phone_info

        data_to_insert.append((
            webhook_source, lead.get('ID'), lead.get('TITLE'), lead.get('NAME'),
            lead.get('LAST_NAME'), lead.get('SECOND_NAME'), lead.get('STATUS_ID'),
            lead.get('SOURCE_ID'), lead.get('ASSIGNED_BY_ID'), lead.get('DATE_CREATE'),
            lead.get('DATE_MODIFY'), lead.get('COMMENTS'), lead.get('COMPANY_TITLE'),
            phone_number, email, web, lead.get('ADDRESS'),
            phone_info.get('region'), phone_info.get('operator'), phone_info.get('old_operator'),
            lead.get('UTM_SOURCE'), lead.get('UTM_MEDIUM'), lead.get('UTM_CAMPAIGN'),
            lead.get('UTM_CONTENT'), lead.get('UTM_TERM')
        ))

    # --- Шаг 4: Сохранение данных в БД ---
    if not data_to_insert:
        logging.info("Нет данных для вставки в БД.")
        cursor.close()
        return 0

    insert_query = f"""
        INSERT INTO {TABLE_NAME} (
            webhook_source, lead_id, title, name, last_name, second_name,
            status_id, source_id, assigned_by_id, date_create, date_modify,
            comments, company_title, phone, email, web, address,
            region, phone_carrier, old_operator,
            utm_source, utm_medium, utm_campaign, utm_content, utm_term
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            title=VALUES(title), name=VALUES(name), last_name=VALUES(last_name),
            second_name=VALUES(second_name), status_id=VALUES(status_id),
            source_id=VALUES(source_id), assigned_by_id=VALUES(assigned_by_id),
            date_create=VALUES(date_create), date_modify=VALUES(date_modify),
            comments=VALUES(comments), company_title=VALUES(company_title),
            phone=VALUES(phone), email=VALUES(email), web=VALUES(web),
            address=VALUES(address), region=VALUES(region),
            phone_carrier=VALUES(phone_carrier), old_operator=VALUES(old_operator),
            utm_source=VALUES(utm_source), utm_medium=VALUES(utm_medium),
            utm_campaign=VALUES(utm_campaign), utm_content=VALUES(utm_content),
            utm_term=VALUES(utm_term);
    """

    try:
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        return cursor.rowcount
    except mysql.connector.Error as err:
        if err.errno == 1213:
            logging.error(f"ОБНАРУЖЕН DEADLOCK при сохранении данных. Пропускаем эту пачку. Ошибка: {err}")
            conn.rollback()
        else:
            logging.error(f"Ошибка при сохранении данных в БД: {err}")
        return 0
    finally:
        cursor.close()


if __name__ == "__main__":
    # --- Механизм блокировки для предотвращения одновременного запуска ---
    lock_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bb_script.lock')
    lock_file = open(lock_file_path, 'w')

    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (IOError, BlockingIOError):
        logging.warning("Скрипт уже запущен. Выход.")
        sys.exit(1)

    logging.info("Блокировка успешно установлена. Начинаю выполнение скрипта.")

    db_connection = None
    try:
        db_connection = mysql.connector.connect(**DB_CONFIG)
        logging.info("Успешное подключение к базе данных MySQL.")
        create_leads_table(db_connection)

        start_date_obj = datetime.strptime(START_DATE, "%Y-%m-%d").date()
        phone_cache = {}

        for webhook in WEBHOOKS:
            portal_url = webhook.split('/rest/')[0]
            logging.info(f"--- Начинаю обработку портала: {portal_url} ---")

            existing_dates = get_existing_lead_dates(db_connection, portal_url, start_date_obj)
            missing_date_filters = calculate_missing_date_ranges(existing_dates, start_date_obj)

            if not missing_date_filters:
                logging.info(f"Нет пропущенных дат для загрузки для портала {portal_url}. Все актуально.")
                logging.info(f"--- Завершил обработку: {portal_url} ---\n")
                continue

            total_leads_for_portal = 0

            for date_filter in missing_date_filters:
                filter_str = next(iter(date_filter.values()))
                logging.info(f"Загрузка лидов для периода: {filter_str}...")

                leads_for_period = get_all_leads(webhook, date_filter)
                logging.info(f"Найдено {len(leads_for_period)} лидов для выгрузки в этом периоде.")

                if leads_for_period:
                    saved_count = save_leads_to_db(db_connection, leads_for_period, portal_url, phone_cache)
                    logging.info(f"Сохранено/обновлено {saved_count} записей в БД.")
                    total_leads_for_portal += saved_count

            logging.info(f"Всего сохранено/обновлено для портала {portal_url}: {total_leads_for_portal} записей.")
            logging.info(f"--- Завершил обработку: {portal_url} ---\n")

    except mysql.connector.Error as err:
        logging.error(f"Ошибка подключения к БД: {err}")
    finally:
        if db_connection and db_connection.is_connected():
            db_connection.close()
            logging.info("Соединение с БД закрыто.")

        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()
        logging.info("Блокировка снята. Завершение работы.")