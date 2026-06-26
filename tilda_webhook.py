import csv
import bisect
import requests
import json
import logging
import re
import os
import threading
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# --- ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ---
load_dotenv()

# --- НАСТРОЙКИ ---

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tilda_webhook.log'),
        logging.StreamHandler()
    ],
    force=True
)

# --- Bitrix24 Configuration ---
webhook = os.getenv('BITRIX_WEBHOOK')
BITRIX_BASE_URL = os.getenv('BITRIX_BASE_URL', 'https://b24-p41gmg.bitrix24.ru')  # Базовый URL портала без /rest/
SPAM_STATUS_IDS = ["JUNK", "SPAM", "10", "9", "8", "7", "6", "5", "4", "3", "2", "1"]

# --- Telegram Logging Configuration ---
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# --- Server Configuration ---
SERVER_HOST = os.getenv('TILDA_SERVER_HOST', '0.0.0.0')
SERVER_PORT = int(os.getenv('TILDA_SERVER_PORT', 5000))

# --- UF_CRM поле для отдела продаж ---
UF_CRM_FIELD = 'UF_CRM_1779024295'
UF_CRM_VALUE_EKATERINBURG = 58
UF_CRM_VALUE_CHELYABINSK = 60

# Ответственные по отделам (fallback, если не удалось найти онлайн-сотрудника)
ASSIGNED_CHELYABINSK = 64
ASSIGNED_DEFAULT = 1  # Екатеринбург fallback

# --- Рабочее время (МСК, UTC+3) ---
WORK_START_HOUR = 7   # 07:00 МСК
WORK_END_HOUR = 16    # до 16:00 МСК (не включительно)

# --- Таймауты уведомлений о принятии лида (в секундах) ---
NOTIFY_TIMEOUT_1 = 5 * 60    # 5 минут
NOTIFY_TIMEOUT_2 = 10 * 60   # 10 минут
NOTIFY_TIMEOUT_3 = 15 * 60   # 15 минут — уведомить руководителя
NOTIFY_TIMEOUT_4 = 30 * 60   # 30 минут — повторное руководителю

# --- Статус "В работе" ---
STATUS_IN_PROCESS = "IN_PROCESS"
STATUS_CONVERTED = "CONVERTED"
STATUS_NEW = "NEW"

# --- Маппинг UF_CRM значения -> ID отдела в Б24 ---
DEPARTMENT_B24_ID = {
    UF_CRM_VALUE_CHELYABINSK: "16",
    UF_CRM_VALUE_EKATERINBURG: "10",
}

# --- MySQL Database Configuration (закомментировано до готовности БД) ---
# DB_CONFIG = {
#     'host': '5.141.91.138',
#     'port': 3001,
#     'user': 'dima',
#     'password': 'vRZVgh6c@@.',
#     'database': 'b24_data'
# }

# --- Схема таблиц MySQL (для справки, создать когда БД будет доступна) ---
# CREATE TABLE lead_assignments (
#     id              INT AUTO_INCREMENT PRIMARY KEY,
#     lead_id         INT NOT NULL,
#     assigned_user   INT NOT NULL,
#     department      VARCHAR(50),
#     phone           VARCHAR(20),
#     created_at      DATETIME NOT NULL,
#     work_hours      TINYINT(1) DEFAULT 1,   -- 1=рабочее время, 0=нерабочее
#     INDEX (lead_id),
#     INDEX (assigned_user),
#     INDEX (created_at)
# );
#
# CREATE TABLE lead_response_events (
#     id              INT AUTO_INCREMENT PRIMARY KEY,
#     lead_id         INT NOT NULL,
#     user_id         INT NOT NULL,
#     event_type      ENUM(
#                         'assigned',       -- лид назначен
#                         'notified_1',     -- уведомление на 5 мин
#                         'notified_2',     -- уведомление на 10 мин
#                         'notified_3',     -- уведомление на 15 мин (+ руководитель)
#                         'notified_4',     -- уведомление на 30 мин (+ руководитель)
#                         'accepted',       -- лид принят в работу (статус IN_PROCESS)
#                         'reassigned'      -- лид переназначен другому
#                     ) NOT NULL,
#     event_at        DATETIME NOT NULL,
#     seconds_elapsed INT,                  -- секунд с момента назначения до события
#     INDEX (lead_id),
#     INDEX (user_id),
#     INDEX (event_at)
# );
#
# Метрики для аналитики сотрудников:
# - Среднее время принятия лида: AVG(seconds_elapsed) WHERE event_type='accepted'
# - Количество лидов за период: COUNT(*) WHERE event_type='assigned'
# - Лиды, принятые вовремя (<5 мин): COUNT(*) WHERE event_type='accepted' AND seconds_elapsed < 300
# - Лиды с эскалацией к руководителю: COUNT(*) WHERE event_type='notified_3' OR event_type='notified_4'
# - Лучший сотрудник: MIN(AVG(seconds_elapsed)) GROUP BY user_id
# - Худший сотрудник: MAX(AVG(seconds_elapsed)) GROUP BY user_id

# ============================================================
# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ MySQL (закомментированы) ---
# ============================================================

# import mysql.connector
#
# def get_db_connection():
#     """Возвращает соединение с MySQL."""
#     try:
#         conn = mysql.connector.connect(**DB_CONFIG)
#         return conn
#     except mysql.connector.Error as e:
#         logging.error(f"[DB] Ошибка подключения к MySQL: {e}")
#         return None
#
#
# def db_save_assignment(lead_id: int, user_id: int, department: str,
#                        phone: str, work_hours: bool):
#     """
#     Сохраняет факт назначения лида сотруднику.
#     Метрика: кто, когда и в какой час получил лид.
#     """
#     conn = get_db_connection()
#     if not conn:
#         return
#     try:
#         cursor = conn.cursor()
#         cursor.execute(
#             """INSERT INTO lead_assignments
#                (lead_id, assigned_user, department, phone, created_at, work_hours)
#                VALUES (%s, %s, %s, %s, %s, %s)""",
#             (lead_id, user_id, department, phone,
#              datetime.now(), 1 if work_hours else 0)
#         )
#         conn.commit()
#         logging.info(f"[DB] Назначение лида {lead_id} -> user {user_id} сохранено.")
#     except mysql.connector.Error as e:
#         logging.error(f"[DB] Ошибка сохранения назначения: {e}")
#     finally:
#         conn.close()
#
#
# def db_save_event(lead_id: int, user_id: int, event_type: str,
#                   seconds_elapsed: int = None):
#     """
#     Сохраняет событие жизненного цикла лида.
#     event_type: assigned / notified_1..4 / accepted / reassigned
#     seconds_elapsed: секунд с момента назначения (None если неизвестно)
#     """
#     conn = get_db_connection()
#     if not conn:
#         return
#     try:
#         cursor = conn.cursor()
#         cursor.execute(
#             """INSERT INTO lead_response_events
#                (lead_id, user_id, event_type, event_at, seconds_elapsed)
#                VALUES (%s, %s, %s, %s, %s)""",
#             (lead_id, user_id, event_type, datetime.now(), seconds_elapsed)
#         )
#         conn.commit()
#         logging.info(f"[DB] Событие '{event_type}' для лида {lead_id} сохранено.")
#     except mysql.connector.Error as e:
#         logging.error(f"[DB] Ошибка сохранения события: {e}")
#     finally:
#         conn.close()


# ============================================================
# --- Списки регионов ---
# ============================================================

SVERDLOVSK_REGIONS = {
    'свердловская область', 'свердловская обл.', 'свердловская обл',
    'алапаевск', 'арамиль', 'артёмовский', 'артемовский', 'асбест',
    'берёзовский', 'березовский', 'богданович', 'верхний тагил',
    'верхняя пышма', 'верхняя салда', 'верхняя тура', 'верхотурье',
    'волчанск', 'дегтярск', 'екатеринбург', 'заречный', 'ивдель',
    'ирбит', 'каменск-уральский', 'камышлов', 'карпинск', 'качканар',
    'кировград', 'краснотурьинск', 'красноуральск', 'красноуфимск',
    'кушва', 'лесной', 'михайловск', 'невьянск', 'нижние серги',
    'нижний тагил', 'нижняя салда', 'нижняя тура', 'новая ляля',
    'новоуральск', 'первоуральск', 'полевской', 'ревда', 'реж',
    'североуральск', 'серов', 'среднеуральск', 'сухой лог', 'сысерть',
    'тавда', 'талица', 'туринск'
}

CHELYABINSK_REGIONS = {
    'челябинская область', 'челябинская обл.', 'челябинская обл',
    'челябинск', 'магнитогорск', 'златоуст', 'миасс', 'копейск',
    'озёрск', 'озерск', 'троицк', 'снежинск', 'чебаркуль', 'сатка',
    'южноуральск', 'коркино', 'кыштым', 'трёхгорный', 'трехгорный',
    'еманжелинск', 'аша', 'карталы', 'верхний уфалей', 'усть-катав',
    'пласт', 'куса', 'бакал', 'катав-ивановск', 'касли', 'сим',
    'карабаш', 'нязепетровск', 'юрюзань', 'верхнеуральск', 'миньяр'
}

EXCLUDED_COMMENT_FIELDS = {
    'phone', 'Phone', 'PHONE',
    'Телефон', 'телефон', 'ТЕЛЕФОН',
    'Ваш телефон', 'ваш телефон',
    'Номер телефона', 'номер телефона',
    'contact_phone', 'inputPhone',
    'ct_phone',
    'dep_id',
    'source_id',
    'utm_source', 'UTM_SOURCE', 'utm_medium', 'UTM_MEDIUM',
    'utm_campaign', 'UTM_CAMPAIGN', 'utm_content', 'UTM_CONTENT',
    'utm_term', 'UTM_TERM', 'utm_region', 'utm_region_id', 'utm_yclid',
    'formid', 'FORM_ID', 'formname', 'FORM_NAME'
}


# ============================================================
# --- КЛАСС ДЛЯ ОПРЕДЕЛЕНИЯ РЕГИОНА ПО БАЗЕ РОССВЯЗИ ---
# ============================================================

class RossvyazMobile:
    def __init__(self, csv_file):
        self.ranges = {}
        self.starts = {}
        self.load(csv_file)

    def load(self, csv_file):
        with open(csv_file, encoding='utf-8-sig', newline='') as f:
            sample = f.read(4096)
            f.seek(0)

            delimiter = ';' if sample.count(';') > sample.count('\t') else '\t'

            reader = csv.DictReader(f, delimiter=delimiter)

            if not reader.fieldnames:
                raise ValueError(f"Файл {csv_file} пустой или не удалось прочитать заголовки.")

            reader.fieldnames = [name.replace('\ufeff', '').strip() for name in reader.fieldnames]

            logging.info(f"[ROSSVYAZ] Используем разделитель: '{delimiter}'")
            logging.info(f"[ROSSVYAZ] Заголовки CSV: {reader.fieldnames}")

            required_columns = {'АВС/ DEF', 'От', 'До', 'Регион', 'Оператор'}
            missing_columns = required_columns - set(reader.fieldnames)
            if missing_columns:
                raise KeyError(
                    f"В файле {csv_file} отсутствуют обязательные колонки: {missing_columns}. "
                    f"Фактические заголовки: {reader.fieldnames}"
                )

            for row in reader:
                clean_row = {
                    (k.replace('\ufeff', '').strip() if k else k): (v.strip() if isinstance(v, str) else v)
                    for k, v in row.items()
                }
                code = clean_row['АВС/ DEF']
                start = int(clean_row['От'])
                end = int(clean_row['До'])
                region = clean_row['Регион']
                operator = clean_row['Оператор']

                if code not in self.ranges:
                    self.ranges[code] = []
                self.ranges[code].append((start, end, region, operator))

        for code in self.ranges:
            self.ranges[code].sort(key=lambda x: x[0])
            self.starts[code] = [item[0] for item in self.ranges[code]]

        logging.info(f"[ROSSVYAZ] Загружено {sum(len(v) for v in self.ranges.values())} диапазонов.")

    def normalize(self, phone):
        if not phone:
            return None
        digits = re.sub(r'\D', '', str(phone))
        if len(digits) == 11 and digits.startswith('8'):
            digits = '7' + digits[1:]
        elif len(digits) == 10 and digits.startswith('9'):
            digits = '7' + digits
        if len(digits) != 11 or not digits.startswith('7'):
            return None
        return digits[1:]

    def find(self, phone):
        phone = self.normalize(phone)
        if not phone or len(phone) != 10:
            return None

        code = phone[:3]
        number_part = int(phone[3:])

        if code not in self.ranges:
            return None

        idx = bisect.bisect_right(self.starts[code], number_part) - 1
        if idx < 0:
            return None

        start, end, region, operator = self.ranges[code][idx]
        if start <= number_part <= end:
            return {"region": region, "operator": operator}

        return None


# ============================================================
# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
# ============================================================

def get_lead_url(lead_id: int) -> str:
    """Возвращает полную кликабельную ссылку на лид в Bitrix24."""
    base = BITRIX_BASE_URL.rstrip('/')
    return f"{base}/crm/lead/details/{lead_id}/"


def is_working_hours() -> bool:
    """
    Проверяет, является ли текущее время рабочим (7:00–16:00 МСК).
    МСК = UTC+3.
    """
    msk_offset = timezone(timedelta(hours=3))
    now_msk = datetime.now(msk_offset)
    return WORK_START_HOUR <= now_msk.hour < WORK_END_HOUR


def normalize_phone(raw_phone: str):
    """Нормализует номер телефона в формат 79999999999."""
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


def extract_phone(data: dict):
    """Пытается найти телефон в данных Tilda по разным вариантам названий поля."""
    possible_keys = [
        'Phone', 'phone', 'PHONE',
        'Телефон', 'телефон', 'ТЕЛЕФОН',
        'Ваш телефон', 'ваш телефон',
        'Номер телефона', 'номер телефона',
        'contact_phone', 'inputPhone'
    ]
    for key in possible_keys:
        value = data.get(key)
        if value and str(value).strip():
            logging.info(f"[WEBHOOK] Телефон найден по ключу '{key}': {value}")
            return str(value).strip()

    for key, value in data.items():
        key_lower = str(key).lower().strip()
        if any(marker in key_lower for marker in ['phone', 'телефон', 'номер']):
            if value and str(value).strip():
                logging.info(f"[WEBHOOK] Телефон найден по похожему ключу '{key}': {value}")
                return str(value).strip()

    logging.warning("[WEBHOOK] Телефон не найден ни в одном поле.")
    return ''


def extract_form_region(data: dict) -> str:
    """Ищет поле с регионом проживания по разным вариантам названия."""
    possible_keys = [
        'Укажите регион проживания',
        'Укажите регион проживания:',
        'Укажите_регион_проживания',
        'Укажите_регион_проживания:',
        'region', 'Region', 'REGION',
        'Регион', 'регион',
        'Регион проживания', 'Регион_проживания'
    ]
    for key in possible_keys:
        value = data.get(key)
        if value and str(value).strip():
            logging.info(f"[WEBHOOK] Регион найден по ключу '{key}': {value}")
            return str(value).strip()

    for key, value in data.items():
        key_lower = str(key).lower().replace('_', ' ')
        if 'регион' in key_lower and 'utm' not in key_lower:
            if value and str(value).strip():
                logging.info(f"[WEBHOOK] Регион найден по похожему ключу '{key}': {value}")
                return str(value).strip()

    logging.info("[WEBHOOK] Регион проживания не найден в форме.")
    return ''


def determine_department(form_region: str, utm_region: str, phone: str, rossvyaz_finder) -> tuple:
    """
    Определяет отдел продаж по приоритету:
    1. Поле "Укажите регион проживания"
    2. UTM_region
    3. По номеру телефона (Россвязь)
    4. По умолчанию - Челябинск (60)
    """
    if form_region:
        form_region_lower = form_region.lower().strip()
        if 'челябинск' in form_region_lower or form_region_lower in CHELYABINSK_REGIONS:
            logging.info(f"[DEPARTMENT] По форме '{form_region}' -> Челябинск (60)")
            return UF_CRM_VALUE_CHELYABINSK, f"По региону из формы: {form_region}"
        if ('свердловск' in form_region_lower or 'екатеринбург' in form_region_lower
                or form_region_lower in SVERDLOVSK_REGIONS):
            logging.info(f"[DEPARTMENT] По форме '{form_region}' -> Екатеринбург (58)")
            return UF_CRM_VALUE_EKATERINBURG, f"По региону из формы: {form_region}"

    if utm_region:
        utm_region_lower = utm_region.lower().strip()
        if utm_region_lower in SVERDLOVSK_REGIONS:
            logging.info(f"[DEPARTMENT] По utm_region '{utm_region}' -> Екатеринбург (58)")
            return UF_CRM_VALUE_EKATERINBURG, f"По UTM региону: {utm_region}"
        if utm_region_lower in CHELYABINSK_REGIONS:
            logging.info(f"[DEPARTMENT] По utm_region '{utm_region}' -> Челябинск (60)")
            return UF_CRM_VALUE_CHELYABINSK, f"По UTM региону: {utm_region}"

    if rossvyaz_finder and phone:
        region_info = rossvyaz_finder.find(phone)
        if region_info and region_info.get('region'):
            phone_region = region_info['region'].lower().strip()
            if 'свердловск' in phone_region or 'екатеринбург' in phone_region:
                logging.info(f"[DEPARTMENT] По телефону '{region_info['region']}' -> Екатеринбург (58)")
                return UF_CRM_VALUE_EKATERINBURG, f"По региону телефона: {region_info['region']}"
            if 'челябинск' in phone_region:
                logging.info(f"[DEPARTMENT] По телефону '{region_info['region']}' -> Челябинск (60)")
                return UF_CRM_VALUE_CHELYABINSK, f"По региону телефона: {region_info['region']}"

    logging.info("[DEPARTMENT] Регион не определён -> Челябинск (60) по умолчанию")
    return UF_CRM_VALUE_CHELYABINSK, "Регион не определён (по умолчанию)"


def build_comments(data: dict) -> str:
    """Собирает все поля формы (кроме телефона и UTM) в текст для COMMENTS."""
    comments_lines = []
    for key, value in data.items():
        if key in EXCLUDED_COMMENT_FIELDS:
            continue
        if value and str(value).strip():
            comments_lines.append(f"{key}: {value}")
    return "\n".join(comments_lines) if comments_lines else ""


# ============================================================
# --- BITRIX24 API ФУНКЦИИ ---
# ============================================================

def get_department_users(dept_b24_id: str) -> list:
    """
    Возвращает список активных пользователей отдела.
    Каждый элемент: {'id': int, 'name': str}
    """
    url = webhook + "user.get"
    params = {
        "ACTIVE": True,
        "filter[UF_DEPARTMENT]": dept_b24_id,
    }
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        result = response.json().get('result', [])
        users = []
        for u in result:
            users.append({
                'id': int(u['ID']),
                'name': f"{u.get('NAME', '')} {u.get('LAST_NAME', '')}".strip()
            })
        logging.info(f"[USERS] Отдел {dept_b24_id}: найдено {len(users)} пользователей.")
        return users
    except requests.exceptions.RequestException as e:
        logging.error(f"[USERS] Ошибка получения пользователей отдела {dept_b24_id}: {e}")
        return []


def get_online_users(dept_b24_id: str) -> list:
    """
    Возвращает список пользователей отдела, которые сейчас онлайн.
    IS_ONLINE=Y — пользователь активен в Битрикс.
    """
    url = webhook + "user.get"
    params = {
        "ACTIVE": True,
        "filter[UF_DEPARTMENT]": dept_b24_id,
        "filter[IS_ONLINE]": "Y",
    }
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        result = response.json().get('result', [])
        users = []
        for u in result:
            users.append({
                'id': int(u['ID']),
                'name': f"{u.get('NAME', '')} {u.get('LAST_NAME', '')}".strip()
            })
        logging.info(f"[ONLINE] Отдел {dept_b24_id}: онлайн {len(users)} пользователей: "
                     f"{[u['id'] for u in users]}")
        return users
    except requests.exceptions.RequestException as e:
        logging.error(f"[ONLINE] Ошибка получения онлайн-пользователей отдела {dept_b24_id}: {e}")
        return []


def get_user_leads_today(user_id: int) -> int:
    """
    Считает количество лидов, назначенных на пользователя, созданных сегодня.
    Используется для равномерного распределения.
    """
    msk_offset = timezone(timedelta(hours=3))
    today_start = datetime.now(msk_offset).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    # Bitrix24 принимает дату в формате YYYY-MM-DD
    today_str = today_start.strftime('%Y-%m-%d')

    url = webhook + "crm.lead.list"
    params = {
        "filter[ASSIGNED_BY_ID]": user_id,
        "filter[>=DATE_CREATE]": today_str,
        "select[]": "ID",
    }
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        result = response.json().get('result', [])
        count = len(result)
        logging.info(f"[WORKLOAD] User {user_id}: лидов сегодня = {count}")
        return count
    except requests.exceptions.RequestException as e:
        logging.error(f"[WORKLOAD] Ошибка получения лидов user {user_id}: {e}")
        return 0


def select_assignee(uf_crm_value: int, head_id: int, fallback_id: int) -> dict:
    """
    Выбирает ответственного сотрудника:
    - В рабочее время: выбирает онлайн-сотрудника отдела с наименьшим
      количеством лидов сегодня. Если онлайн никого нет — берёт руководителя.
    - В нерабочее время: руководитель отдела.

    Возвращает {'id': int, 'name': str, 'reason': str}
    """
    dept_b24_id = DEPARTMENT_B24_ID.get(uf_crm_value)
    department_name = "Екатеринбург" if uf_crm_value == UF_CRM_VALUE_EKATERINBURG else "Челябинск"

    if not is_working_hours():
        logging.info(f"[ASSIGN] Нерабочее время -> руководитель отдела ID={head_id}")
        return {
            'id': head_id if head_id else fallback_id,
            'name': 'Руководитель отдела',
            'reason': 'Нерабочее время — назначен руководитель'
        }

    # Рабочее время: ищем онлайн-сотрудников
    online_users = get_online_users(dept_b24_id) if dept_b24_id else []

    # Исключаем руководителя из распределения (он резервный)
    if head_id:
        online_users = [u for u in online_users if u['id'] != head_id]

    if not online_users:
        logging.warning(
            f"[ASSIGN] Рабочее время, но онлайн-сотрудников отдела '{department_name}' нет. "
            f"Назначаем руководителя ID={head_id}."
        )
        return {
            'id': head_id if head_id else fallback_id,
            'name': 'Руководитель отдела',
            'reason': 'Рабочее время, но нет онлайн-сотрудников — назначен руководитель'
        }

    # Считаем нагрузку для каждого онлайн-сотрудника
    workload = []
    for user in online_users:
        leads_today = get_user_leads_today(user['id'])
        workload.append({
            'id': user['id'],
            'name': user['name'],
            'leads_today': leads_today
        })

    # Выбираем с минимальной нагрузкой
    workload.sort(key=lambda x: x['leads_today'])
    chosen = workload[0]

    logging.info(
        f"[ASSIGN] Выбран сотрудник: {chosen['name']} (ID={chosen['id']}), "
        f"лидов сегодня: {chosen['leads_today']}. "
        f"Нагрузка по отделу: {[(w['id'], w['leads_today']) for w in workload]}"
    )

    return {
        'id': chosen['id'],
        'name': chosen['name'],
        'reason': (
            f"Рабочее время — минимальная нагрузка: "
            f"{chosen['leads_today']} лидов сегодня"
        )
    }


def get_department_head(uf_crm_value: int):
    """
    Динамически запрашивает руководителя отдела из Б24.
    Возвращает int или None.
    """
    dept_id = DEPARTMENT_B24_ID.get(uf_crm_value)
    if not dept_id:
        logging.warning(f"[HEAD] Нет маппинга отдела для UF_CRM={uf_crm_value}")
        return None

    url = webhook + "department.get"
    params = {"ID": dept_id}
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        result = response.json().get('result', [])
        if not result:
            logging.warning(f"[HEAD] Отдел ID={dept_id} не найден в Б24")
            return None

        dept = result[0]
        uf_head = dept.get('UF_HEAD')
        if not uf_head or str(uf_head) == "0":
            logging.warning(
                f"[HEAD] У отдела '{dept.get('NAME')}' (ID={dept_id}) "
                f"руководитель не назначен (UF_HEAD={uf_head})"
            )
            return None

        head_id = int(uf_head)
        logging.info(f"[HEAD] Отдел '{dept.get('NAME')}' (ID={dept_id}) -> руководитель ID={head_id}")
        return head_id

    except requests.exceptions.RequestException as e:
        logging.error(f"[HEAD] Ошибка запроса department.get: {e}")
        return None
    except (ValueError, IndexError, KeyError) as e:
        logging.error(f"[HEAD] Ошибка обработки ответа department.get: {e}")
        return None


def send_im_message(to_user_id: int, message: str):
    """Отправляет личное сообщение пользователю через im.message.add."""
    url = webhook + "im.message.add"
    data = {
        "DIALOG_ID": str(to_user_id),
        "MESSAGE": message,
        "SYSTEM": "N",
        "URL_PREVIEW": "N"
    }
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(url, data=json.dumps(data), headers=headers, timeout=10)
        response.raise_for_status()
        result = response.json()
        if result.get('result'):
            msg_id = result['result']
            logging.info(f"[IM] Сообщение отправлено user ID={to_user_id}. Message ID={msg_id}")
            return msg_id
        elif result.get('error'):
            logging.error(
                f"[IM] Ошибка отправки user ID={to_user_id}: "
                f"{result.get('error')} — {result.get('error_description', '')}"
            )
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"[IM] Ошибка запроса im.message.add: {e}")
        return None


def send_telegram_message(message: str):
    """Отправляет сообщение в Telegram (принудительно IPv4, до 5 попыток)."""
    import time
    import socket

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("[TELEGRAM] Токен или chat_id не настроены.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'HTML'
    }

    max_retries = 5
    retry_delay = 3

    for attempt in range(1, max_retries + 1):
        try:
            old_getaddrinfo = socket.getaddrinfo

            def ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
                return old_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)

            socket.getaddrinfo = ipv4_only_getaddrinfo
            try:
                response = requests.post(url, json=payload, timeout=10)
            finally:
                socket.getaddrinfo = old_getaddrinfo

            if response.status_code >= 400:
                logging.warning(f"[TELEGRAM] Ответ API (попытка {attempt}/{max_retries}): {response.text}")

            response.raise_for_status()
            logging.info(f"[TELEGRAM] Сообщение отправлено (попытка {attempt}/{max_retries}).")
            return

        except requests.exceptions.RequestException as e:
            logging.error(f"[TELEGRAM] Ошибка отправки (попытка {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                import time as t
                t.sleep(retry_delay)

    logging.error(f"[TELEGRAM] Все {max_retries} попыток исчерпаны.")


def get_duplicate_lead_id(phone):
    """Проверяет наличие дубликата лида по номеру телефона."""
    url = webhook + "crm.duplicate.findbycomm"
    data = {
        "entity_type": "LEAD",
        "type": "PHONE",
        "values": [phone]
    }
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(url, data=json.dumps(data), headers=headers, timeout=30)
        response.raise_for_status()
        result = response.json()
        if result.get('result') and result['result'].get('LEAD'):
            lead_id = result['result']['LEAD'][0]
            logging.info(f"[DUPLICATE] Найден дубликат для {phone}. ID лида: {lead_id}")
            return lead_id
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"[DUPLICATE] Ошибка проверки: {e}")
        return None
    except (json.JSONDecodeError, IndexError) as e:
        logging.error(f"[DUPLICATE] Ошибка обработки ответа: {e}")
        return None


def get_source_name(source_id: str) -> str:
    """Получает человекочитаемое название источника из Bitrix24."""
    url = webhook + "crm.status.list"
    params = {
        "filter[ENTITY_ID]": "SOURCE",
        "filter[STATUS_ID]": source_id
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        result = response.json().get('result', [])
        if result:
            name = result[0].get('NAME', source_id)
            logging.info(f"[SOURCE] Источник '{source_id}' -> '{name}'")
            return name
        else:
            logging.warning(f"[SOURCE] Источник '{source_id}' не найден.")
            return source_id
    except requests.exceptions.RequestException as e:
        logging.error(f"[SOURCE] Ошибка получения имени источника: {e}")
        return source_id


def is_tilda_test_request(data: dict) -> bool:
    """Определяет тестовый запрос от Tilda."""
    raw_phone = extract_phone(data)
    meaningful_values = [
        str(v).strip() for v in data.values()
        if v is not None and str(v).strip()
    ]
    if not raw_phone and len(meaningful_values) <= 2:
        return True
    return False


def create_lead(
        title: str,
        name: str,
        phone: str,
        email: str,
        comments: str,
        uf_crm_value: int,
        utm_source: str = "",
        utm_medium: str = "",
        utm_campaign: str = "",
        utm_content: str = "",
        utm_term: str = "",
        source_id: str = "WEB",
        source_description: str = "",
        assigned_by_id: int = 1,
        status_id: str = "NEW",
        opened: str = "Y"
):
    """
    Создает новый лид в Bitrix24.
    При ошибке повторяет до 10 раз с интервалом 3 секунды.
    """
    import time

    url = webhook + "crm.lead.add"
    source_id = str(source_id).strip() if source_id else "WEB"
    max_retries = 10
    retry_delay = 3

    fields = {
        "TITLE": title,
        "NAME": name,
        "STATUS_ID": status_id,
        "OPENED": opened,
        "ASSIGNED_BY_ID": assigned_by_id,
        "SOURCE_ID": source_id,
        "SOURCE_DESCRIPTION": source_description,
        "COMMENTS": comments,
        UF_CRM_FIELD: uf_crm_value,
    }

    if utm_source:
        fields["UTM_SOURCE"] = utm_source
    if utm_medium:
        fields["UTM_MEDIUM"] = utm_medium
    if utm_campaign:
        fields["UTM_CAMPAIGN"] = utm_campaign
    if utm_content:
        fields["UTM_CONTENT"] = utm_content
    if utm_term:
        fields["UTM_TERM"] = utm_term
    if phone:
        fields["PHONE"] = [{"VALUE": phone, "VALUE_TYPE": "WORK"}]
    if email:
        fields["EMAIL"] = [{"VALUE": email, "VALUE_TYPE": "WORK"}]

    data = {
        "fields": fields,
        "params": {"REGISTER_SONET_EVENT": "Y"}
    }
    headers = {'Content-Type': 'application/json'}
    last_error = ""

    for attempt in range(1, max_retries + 1):
        try:
            logging.info(
                f"[CREATE] Попытка {attempt}/{max_retries}. "
                f"SOURCE_ID={source_id}, ASSIGNED_BY_ID={assigned_by_id}, "
                f"{UF_CRM_FIELD}={uf_crm_value}"
            )
            if attempt == 1:
                logging.info(f"[CREATE] Payload: {json.dumps(data, ensure_ascii=False)}")

            response = requests.post(url, data=json.dumps(data), headers=headers, timeout=30)
            response.raise_for_status()
            result = response.json()
            logging.info(f"[CREATE] Ответ Bitrix24: {result}")

            if 'result' in result:
                logging.info(f"[CREATE] Лид создан. ID: {result['result']}. Попытка: {attempt}/{max_retries}")
                return result['result']
            elif 'error' in result:
                last_error = f"{result['error']}: {result.get('error_description', '')}"
                logging.error(f"[CREATE] Ошибка Bitrix24 (попытка {attempt}/{max_retries}): {last_error}")
            else:
                last_error = f"Неизвестный ответ: {result}"
                logging.error(f"[CREATE] {last_error}")

        except requests.exceptions.RequestException as e:
            last_error = str(e)
            logging.error(f"[CREATE] Ошибка запроса (попытка {attempt}/{max_retries}): {e}")
        except json.JSONDecodeError as e:
            last_error = str(e)
            logging.error(f"[CREATE] Ошибка JSON (попытка {attempt}/{max_retries}): {e}")

        if attempt < max_retries:
            logging.info(f"[CREATE] Жду {retry_delay} сек перед попыткой {attempt + 1}...")
            time.sleep(retry_delay)

    logging.error(f"[CREATE] Все {max_retries} попыток исчерпаны. Лид НЕ создан.")

    department_name = "Екатеринбург" if uf_crm_value == UF_CRM_VALUE_EKATERINBURG else "Челябинск"
    tg_error_message = (
        f"🚨 <b>ОШИБКА: Не удалось создать лид в CRM</b>\n\n"
        f"Телефон: <code>{phone}</code>\n"
        f"Имя: {name if name else '-'}\n"
        f"Отдел: {department_name}\n"
        f"SOURCE_ID: {source_id}\n"
        f"Попыток: {max_retries}\n"
        f"Последняя ошибка: {last_error}\n\n"
        f"Лид нужно создать вручную!"
    )
    send_telegram_message(tg_error_message)
    return None


def get_lead_details(lead_id):
    """Получает данные лида по ID."""
    url = webhook + "crm.lead.get"
    params = {'ID': lead_id}
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        result = response.json().get('result')
        if result:
            return {
                "STATUS_ID": result.get("STATUS_ID"),
                "ASSIGNED_BY_ID": result.get("ASSIGNED_BY_ID")
            }
        else:
            logging.warning(f"[DETAILS] Лид {lead_id} не найден.")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"[DETAILS] Ошибка получения данных лида {lead_id}: {e}")
        return None


def create_b24_task(lead_id, responsible_id):
    """Создает задачу 'позвонить клиенту' для ответственного по лиду."""
    url = webhook + "tasks.task.add"
    lead_url = get_lead_url(lead_id)
    task_description = (
        f"Позвони, клиент оставил повторную заявку.\n"
        f"Лид: {lead_url}"
    )
    deadline = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S')

    data = {
        "fields": {
            "TITLE": "Повторная заявка",
            "DESCRIPTION": task_description,
            "RESPONSIBLE_ID": responsible_id,
            "UF_CRM_TASK": [f"L_{lead_id}"],
            "DEADLINE": deadline,
        }
    }
    try:
        response = requests.post(url, json=data, timeout=30)
        response.raise_for_status()
        result = response.json()
        if result.get('result') and result['result'].get('task'):
            task_id = result['result']['task']['id']
            logging.info(f"[TASK] Задача {task_id} создана для лида {lead_id}.")
            return task_id
        else:
            error_info = result.get('error_description') or result
            logging.error(f"[TASK] Не удалось создать задачу для лида {lead_id}: {error_info}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"[TASK] Ошибка создания задачи для лида {lead_id}: {e}")
        return None


def update_lead_status(lead_id, status_id="NEW"):
    """Обновляет статус лида."""
    url = webhook + "crm.lead.update"
    data = {
        "id": lead_id,
        "fields": {"STATUS_ID": status_id}
    }
    try:
        response = requests.post(url, json=data, timeout=30)
        response.raise_for_status()
        if response.json().get('result'):
            logging.info(f"[UPDATE] Статус лида {lead_id} обновлен на '{status_id}'.")
            return True
        return False
    except requests.exceptions.RequestException as e:
        logging.error(f"[UPDATE] Ошибка обновления статуса лида {lead_id}: {e}")
        return False


# ============================================================
# --- СИСТЕМА УВЕДОМЛЕНИЙ О ПРИНЯТИИ ЛИДА ---
# ============================================================

# Хранилище активных мониторингов лидов.
# Ключ: lead_id (int)
# Значение: dict с данными мониторинга
_lead_monitor_lock = threading.Lock()
_active_monitors: dict = {}


def start_lead_acceptance_monitor(
        lead_id: int,
        assigned_user_id: int,
        assigned_user_name: str,
        head_id: int,
        department_name: str,
        lead_name: str,
        phone: str
):
    """
    Запускает фоновый мониторинг принятия лида.
    Уведомляет по таймаутам 5/10/15/30 минут.
    Если статус IN_PROCESS — останавливает мониторинг.
    Если ответственный сменился — уведомляет нового ответственного.
    """
    lead_url = get_lead_url(lead_id)
    assigned_at = datetime.now()

    monitor_data = {
        'lead_id': lead_id,
        'assigned_user_id': assigned_user_id,
        'assigned_user_name': assigned_user_name,
        'head_id': head_id,
        'department_name': department_name,
        'lead_name': lead_name,
        'phone': phone,
        'assigned_at': assigned_at,
        'lead_url': lead_url,
        'stop': False,
        'notified_1': False,
        'notified_2': False,
        'notified_3': False,
        'notified_4': False,
    }

    with _lead_monitor_lock:
        # Останавливаем предыдущий мониторинг для этого лида (если был)
        if lead_id in _active_monitors:
            _active_monitors[lead_id]['stop'] = True
            logging.info(f"[MONITOR] Предыдущий мониторинг лида {lead_id} остановлен.")
        _active_monitors[lead_id] = monitor_data

    thread = threading.Thread(
        target=_monitor_lead_acceptance,
        args=(monitor_data,),
        daemon=True,
        name=f"monitor-lead-{lead_id}"
    )
    thread.start()
    logging.info(
        f"[MONITOR] Запущен мониторинг лида {lead_id} -> "
        f"user {assigned_user_id} ({assigned_user_name})"
    )

    # db_save_event(lead_id, assigned_user_id, 'assigned', seconds_elapsed=0)  # DB закомментировано


def _monitor_lead_acceptance(monitor_data: dict):
    """
    Фоновый поток: проверяет статус лида и отправляет уведомления.
    Шаги: 5 мин -> 10 мин -> 15 мин (+ руководитель) -> 30 мин (+ руководитель)
    """
    import time

    lead_id = monitor_data['lead_id']
    lead_url = monitor_data['lead_url']
    assigned_at = monitor_data['assigned_at']

    # Таймауты и соответствующие флаги
    schedule = [
        (NOTIFY_TIMEOUT_1, 'notified_1', False),   # 5 мин
        (NOTIFY_TIMEOUT_2, 'notified_2', False),   # 10 мин
        (NOTIFY_TIMEOUT_3, 'notified_3', True),    # 15 мин + руководитель
        (NOTIFY_TIMEOUT_4, 'notified_4', True),    # 30 мин + руководитель
    ]

    # Проверяем каждые 30 секунд
    CHECK_INTERVAL = 30

    while True:
        time.sleep(CHECK_INTERVAL)

        # Проверяем флаг остановки
        with _lead_monitor_lock:
            if monitor_data.get('stop'):
                logging.info(f"[MONITOR] Мониторинг лида {lead_id} остановлен по флагу.")
                return

        elapsed = (datetime.now() - assigned_at).total_seconds()

        # Получаем актуальные данные лида
        details = get_lead_details(lead_id)
        if not details:
            logging.warning(f"[MONITOR] Не удалось получить данные лида {lead_id}. Продолжаю...")
            continue

        current_status = details.get('STATUS_ID', '')
        current_responsible = int(details.get('ASSIGNED_BY_ID', 0))

        # Проверяем смену ответственного
        with _lead_monitor_lock:
            prev_assigned = monitor_data['assigned_user_id']

        if current_responsible and current_responsible != prev_assigned:
            logging.info(
                f"[MONITOR] Лид {lead_id}: ответственный сменился "
                f"{prev_assigned} -> {current_responsible}. Перезапускаем мониторинг."
            )
            # Уведомляем нового ответственного
            _notify_new_responsible(lead_id, current_responsible, lead_url, monitor_data)

            # Обновляем данные мониторинга и перезапускаем
            with _lead_monitor_lock:
                monitor_data['assigned_user_id'] = current_responsible
                monitor_data['assigned_at'] = datetime.now()
                monitor_data['notified_1'] = False
                monitor_data['notified_2'] = False
                monitor_data['notified_3'] = False
                monitor_data['notified_4'] = False
                assigned_at = monitor_data['assigned_at']

            # db_save_event(lead_id, current_responsible, 'reassigned')  # DB закомментировано
            continue

        # Если лид принят в работу — останавливаем мониторинг
        if current_status == STATUS_IN_PROCESS:
            elapsed_int = int(elapsed)
            logging.info(
                f"[MONITOR] Лид {lead_id} принят в работу через {elapsed_int} сек. "
                f"Мониторинг завершён."
            )
            # db_save_event(lead_id, prev_assigned, 'accepted', elapsed_int)  # DB закомментировано
            with _lead_monitor_lock:
                _active_monitors.pop(lead_id, None)
            return

        # Если лид конвертирован — тоже останавливаем (не меняем статус)
        if current_status == STATUS_CONVERTED:
            logging.info(f"[MONITOR] Лид {lead_id} конвертирован. Мониторинг завершён.")
            with _lead_monitor_lock:
                _active_monitors.pop(lead_id, None)
            return

        # Обрабатываем уведомления по расписанию
        with _lead_monitor_lock:
            assigned_user_id = monitor_data['assigned_user_id']
            head_id = monitor_data['head_id']
            department_name = monitor_data['department_name']
            lead_name = monitor_data['lead_name']

        for timeout_sec, flag_key, notify_head in schedule:
            if elapsed >= timeout_sec and not monitor_data.get(flag_key):
                minutes = timeout_sec // 60
                _send_acceptance_notification(
                    lead_id=lead_id,
                    user_id=assigned_user_id,
                    head_id=head_id,
                    department_name=department_name,
                    lead_name=lead_name,
                    lead_url=lead_url,
                    minutes_elapsed=minutes,
                    notify_head=notify_head,
                    flag_key=flag_key,
                    monitor_data=monitor_data
                )
                break  # Отправляем по одному уведомлению за итерацию

        # Если все 4 уведомления отправлены — завершаем мониторинг
        if all(monitor_data.get(f) for f in ['notified_1', 'notified_2', 'notified_3', 'notified_4']):
            logging.info(f"[MONITOR] Все уведомления по лиду {lead_id} отправлены. Мониторинг завершён.")
            with _lead_monitor_lock:
                _active_monitors.pop(lead_id, None)
            return


def _send_acceptance_notification(
        lead_id: int,
        user_id: int,
        head_id: int,
        department_name: str,
        lead_name: str,
        lead_url: str,
        minutes_elapsed: int,
        notify_head: bool,
        flag_key: str,
        monitor_data: dict
):
    """Отправляет уведомление о непринятом лиде ответственному (и руководителю если нужно)."""

    with _lead_monitor_lock:
        if monitor_data.get(flag_key):
            return  # Уже отправлено
        monitor_data[flag_key] = True

    # Уведомление сотруднику
    if minutes_elapsed < 15:
        user_message = (
            f"⚠️ Лид не принят в работу уже {minutes_elapsed} минут!\n\n"
            f"Вам назначен лид: {lead_name}\n"
            f"Срочно возьмите его в работу!\n"
            f"Ссылка на лид: {lead_url}"
        )
    else:
        user_message = (
            f"🚨 СРОЧНО! Лид не принят уже {minutes_elapsed} минут!\n\n"
            f"Вам назначен лид: {lead_name}\n"
            f"Ваш руководитель уже получил уведомление об этом!\n"
            f"Ссылка на лид: {lead_url}"
        )

    send_im_message(user_id, user_message)
    logging.info(f"[MONITOR] Уведомление {minutes_elapsed} мин -> user {user_id}, лид {lead_id}")

    # db_save_event(lead_id, user_id, f'notified_{flag_key[-1]}',
    #               seconds_elapsed=minutes_elapsed*60)  # DB закомментировано

    # Уведомление руководителю (только на 15 и 30 минутах)
    if notify_head and head_id:
        head_message = (
            f"🚨 Лид не принят в работу {minutes_elapsed} минут!\n\n"
            f"Отдел: {department_name}\n"
            f"Лид: {lead_name}\n"
            f"Ответственный (ID={user_id}) не реагирует.\n"
            f"Ссылка на лид: {lead_url}"
        )
        send_im_message(head_id, head_message)
        logging.info(
            f"[MONITOR] Уведомление {minutes_elapsed} мин -> руководитель {head_id}, лид {lead_id}"
        )


def _notify_new_responsible(
        lead_id: int,
        new_user_id: int,
        lead_url: str,
        monitor_data: dict
):
    """Отправляет уведомление новому ответственному за лид."""
    department_name = monitor_data.get('department_name', '')
    lead_name = monitor_data.get('lead_name', '')

    message = (
        f"📋 Вам назначен лид!\n\n"
        f"Лид: {lead_name}\n"
        f"Отдел: {department_name}\n"
        f"Возьмите его в работу как можно скорее.\n"
        f"Ссылка на лид: {lead_url}"
    )
    send_im_message(new_user_id, message)
    logging.info(f"[MONITOR] Уведомление новому ответственному {new_user_id} за лид {lead_id}")


def notify_assignee_new_lead(
        lead_id: int,
        user_id: int,
        user_name: str,
        lead_name: str,
        source_name: str,
        department_name: str,
        phone: str
):
    """
    Отправляет личное сообщение сотруднику сразу после создания лида.
    """
    lead_url = get_lead_url(lead_id)
    message = (
        f"🆕 На вас создан новый лид!\n\n"
        f"Имя клиента: {lead_name}\n"
        f"Отдел: {department_name}\n\n"
        f"Возьмите лид в работу как можно скорее!\n"
        f"Ссылка на лид: {lead_url}"
    )
    send_im_message(user_id, message)
    logging.info(f"[NOTIFY] Уведомление о новом лиде {lead_id} -> user {user_id} ({user_name})")


# ============================================================
# --- FLASK ПРИЛОЖЕНИЕ ---
# ============================================================

app = Flask(__name__)

# Загружаем базу Россвязи при старте
ranges_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ranges.csv')
if os.path.exists(ranges_file):
    rossvyaz_finder = RossvyazMobile(ranges_file)
else:
    logging.warning(f"[INIT] Файл ranges.csv не найден: {ranges_file}. Определение по телефону недоступно.")
    rossvyaz_finder = None


@app.route('/health', methods=['GET'])
def health_check():
    """Проверка работоспособности сервера."""
    return jsonify({"status": "ok", "message": "Tilda webhook server is running"}), 200


@app.route('/webhook/tilda', methods=['GET', 'POST'])
def tilda_webhook():
    """
    Принимает заявки от Tilda и создает/обрабатывает лиды в Bitrix24.
    """
    logging.info("=" * 60)
    logging.info("[WEBHOOK] Получен запрос от Tilda")

    # GET без ct_phone — просто проверка доступности
    if request.method == 'GET' and not request.args.get('ct_phone'):
        logging.info("[WEBHOOK] GET-запрос для проверки доступности.")
        return jsonify({"status": "ok", "message": "Webhook is available"}), 200

    # Получаем данные из запроса
    if request.method == 'GET':
        data = request.args.to_dict()
        logging.info("[WEBHOOK] GET-запрос с параметром ct_phone.")
    elif request.is_json:
        data = request.get_json()
    else:
        data = request.form.to_dict()

    logging.info(f"[WEBHOOK] Данные запроса: {data}")

    # Параметры из URL
    url_dep_id = request.args.get('dep_id')
    url_source_id = str(request.args.get('source_id', 'WEB')).strip()

    logging.info(f"[WEBHOOK] dep_id из URL: '{url_dep_id}'")
    logging.info(f"[WEBHOOK] source_id из URL: '{url_source_id}'")

    source_name = get_source_name(url_source_id)
    logging.info(f"[WEBHOOK] Имя источника: '{source_name}'")

    # Подробное логирование полей
    logging.info("[WEBHOOK] === РАЗБОР ПОЛЕЙ ===")
    for key, value in data.items():
        logging.info(f"[WEBHOOK]   '{key}' = '{value}'")
    logging.info("[WEBHOOK] === КОНЕЦ РАЗБОРА ПОЛЕЙ ===")

    # Тестовый запрос
    if is_tilda_test_request(data):
        logging.info("[WEBHOOK] Тестовый запрос от Tilda. Возвращаю OK.")
        return jsonify({"status": "ok", "message": "Tilda webhook test accepted"}), 200

    # --- Телефон ---
    url_ct_phone = request.args.get('ct_phone', '').strip()
    if url_ct_phone:
        raw_phone = url_ct_phone
        logging.info(f"[WEBHOOK] Телефон из URL ct_phone: {raw_phone}")
    else:
        raw_phone = extract_phone(data)

    phone = normalize_phone(raw_phone)

    if not phone:
        logging.warning(f"[WEBHOOK] Некорректный номер: {raw_phone}")
        return jsonify({
            "status": "ok",
            "message": "Request accepted, but lead was not created because phone is invalid"
        }), 200

    logging.info(f"[WEBHOOK] Нормализованный телефон: {phone}")

    # --- Поля формы ---
    name = data.get('Name') or data.get('name') or data.get('NAME') or ''
    email = data.get('Email') or data.get('email') or data.get('EMAIL') or ''

    utm_source = data.get('utm_source') or data.get('UTM_SOURCE') or ''
    utm_medium = data.get('utm_medium') or data.get('UTM_MEDIUM') or ''
    utm_campaign = data.get('utm_campaign') or data.get('UTM_CAMPAIGN') or ''
    utm_content = data.get('utm_content') or data.get('UTM_CONTENT') or ''
    utm_term = data.get('utm_term') or data.get('UTM_TERM') or ''

    raw_utm_region = data.get('utm_region') or ''
    if raw_utm_region.startswith('{') and raw_utm_region.endswith('}'):
        utm_region = ''
        logging.info(f"[WEBHOOK] utm_region содержит макрос '{raw_utm_region}', игнорируем.")
    else:
        utm_region = raw_utm_region

    form_region = extract_form_region(data)

    logging.info(f"[WEBHOOK] form_region='{form_region}', utm_region='{utm_region}'")

    # --- Определяем отдел ---
    if url_dep_id and url_dep_id in ('58', '60'):
        uf_crm_value = int(url_dep_id)
        department_source = f"По параметру dep_id из URL: {url_dep_id}"
        logging.info(f"[DEPARTMENT] Принудительно dep_id={url_dep_id}")
    else:
        uf_crm_value, department_source = determine_department(
            form_region=form_region,
            utm_region=utm_region,
            phone=phone,
            rossvyaz_finder=rossvyaz_finder
        )

    department_name = "Екатеринбург" if uf_crm_value == UF_CRM_VALUE_EKATERINBURG else "Челябинск"
    fallback_id = ASSIGNED_CHELYABINSK if uf_crm_value == UF_CRM_VALUE_CHELYABINSK else ASSIGNED_DEFAULT

    # --- Получаем руководителя отдела ---
    head_id = get_department_head(uf_crm_value)
    logging.info(
        f"[WEBHOOK] Руководитель отдела '{department_name}': "
        f"{'ID=' + str(head_id) if head_id else 'не назначен'}"
    )

    # --- Выбираем ответственного (умное распределение) ---
    assignee = select_assignee(
        uf_crm_value=uf_crm_value,
        head_id=head_id if head_id else fallback_id,
        fallback_id=fallback_id
    )
    assigned_by_id = assignee['id']
    logging.info(
        f"[WEBHOOK] Выбран ответственный: ID={assigned_by_id} ({assignee['name']}). "
        f"Причина: {assignee['reason']}"
    )

    # --- Комментарии ---
    comments = build_comments(data)

    # --- Проверяем дубликат ---
    duplicate_lead_id = get_duplicate_lead_id(phone)

    result_data = {
        "phone": phone,
        "department": department_name,
        "uf_crm_value": uf_crm_value,
        "department_source": department_source
    }

    if duplicate_lead_id:
        # =====================================================
        # ОБРАБОТКА ДУБЛИКАТА
        # =====================================================
        logging.info(f"[WEBHOOK] Дубликат. ID лида: {duplicate_lead_id}")
        result_data["action"] = "duplicate"
        result_data["lead_id"] = duplicate_lead_id

        lead_url = get_lead_url(duplicate_lead_id)
        details = get_lead_details(duplicate_lead_id)
        current_status = details.get('STATUS_ID', '') if details else ''

        status_updated = False
        task_id = None
        im_message_id = None

        # Если лид КОНВЕРТИРОВАН — не меняем статус, только уведомляем
        if current_status == STATUS_CONVERTED:
            logging.info(
                f"[WEBHOOK] Лид {duplicate_lead_id} имеет статус CONVERTED. "
                f"Статус не меняем."
            )
            result_data["status_reset_to_new"] = False
            result_data["note"] = "Лид конвертирован, статус не изменён"

            # Уведомление руководителю
            if head_id:
                converted_msg = (
                    f"ℹ️ Повторная заявка на конвертированный лид!\n\n"
                    f"Клиент снова оставил заявку, но лид уже конвертирован.\n"
                    f"Имя: {name if name else '-'}\n"
                    f"Отдел: {department_name}\n"
                    f"Ссылка на лид: {lead_url}"
                )
                im_message_id = send_im_message(head_id, converted_msg)
                result_data["im_message_id"] = im_message_id

        else:
            # Не конвертирован — обычная обработка дубликата
            # Всегда ставим статус NEW
            status_updated = update_lead_status(duplicate_lead_id, STATUS_NEW)
            result_data["status_reset_to_new"] = status_updated
            logging.info(
                f"[WEBHOOK] Статус лида {duplicate_lead_id} -> NEW: "
                f"{'OK' if status_updated else 'ОШИБКА'}"
            )

            if details and details.get("ASSIGNED_BY_ID"):
                responsible_id = int(details["ASSIGNED_BY_ID"])

                # Задача ответственному менеджеру
                task_id = create_b24_task(duplicate_lead_id, responsible_id)
                result_data["task_id"] = task_id

                # Личное сообщение руководителю
                if head_id:
                    im_text = (
                        f"🔔 Повторная заявка!\n\n"
                        f"Клиент снова оставил заявку.\n"
                        f"Имя: {name if name else '-'}\n"
                        f"Отдел: {department_name}\n"
                        f"Ссылка на лид: {lead_url}\n"
                        f"Задача: {'#' + str(task_id) if task_id else 'ошибка создания'}"
                    )
                    im_message_id = send_im_message(head_id, im_text)
                    result_data["im_message_id"] = im_message_id
            else:
                logging.warning(f"[WEBHOOK] Не удалось получить детали дубликата {duplicate_lead_id}")

        # Telegram — всегда
        tg_message = (
            f"<b>Повторная заявка (Рекламный лид)</b>\n\n"
            f"Телефон: <code>{phone}</code>\n"
            f"Имя: {name if name else '-'}\n"
            f"Отдел: {department_name}\n"
            f"Источник: {source_name}\n"
            f"Существующий лид: #{duplicate_lead_id}\n"
            f"Статус лида: {current_status}\n"
            f"Статус -> NEW: {'✅' if status_updated else ('⏩ Конвертирован, не изменён' if current_status == STATUS_CONVERTED else '❌')}\n"
            f"Задача менеджеру: {'#' + str(task_id) if task_id else ('—' if current_status == STATUS_CONVERTED else '❌')}\n"
            f"Уведомление руководителю: {'✅' if im_message_id else '❌'}\n"
            f"Ссылка: {lead_url}"
        )
        send_telegram_message(tg_message)

    else:
        # =====================================================
        # НОВЫЙ ЛИД
        # =====================================================
        logging.info(f"[WEBHOOK] Дубликат не найден. Создаём новый лид.")

        title = f"Рекламный лид: {name}" if name else "Рекламный лид"

        new_lead_id = create_lead(
            title=title,
            name=name,
            phone=phone,
            email=email,
            comments=comments,
            uf_crm_value=uf_crm_value,
            utm_source=utm_source,
            utm_medium=utm_medium,
            utm_campaign=utm_campaign,
            utm_content=utm_content,
            utm_term=utm_term,
            source_id=url_source_id,
            source_description=department_source,
            assigned_by_id=assigned_by_id
        )

        if new_lead_id:
            result_data["action"] = "created"
            result_data["lead_id"] = new_lead_id
            lead_url = get_lead_url(new_lead_id)

            # db_save_assignment(                                       # DB закомментировано
            #     lead_id=new_lead_id,
            #     user_id=assigned_by_id,
            #     department=department_name,
            #     phone=phone,
            #     work_hours=is_working_hours()
            # )

            # Уведомление ответственному о новом лиде
            notify_assignee_new_lead(
                lead_id=new_lead_id,
                user_id=assigned_by_id,
                user_name=assignee['name'],
                lead_name=title,
                source_name=source_name,
                department_name=department_name,
                phone=phone
            )

            # Запускаем мониторинг принятия лида
            start_lead_acceptance_monitor(
                lead_id=new_lead_id,
                assigned_user_id=assigned_by_id,
                assigned_user_name=assignee['name'],
                head_id=head_id if head_id else fallback_id,
                department_name=department_name,
                lead_name=title,
                phone=phone
            )

            # Telegram
            tg_message = (
                f"<b>Новый Рекламный лид</b>\n\n"
                f"Телефон: <code>{phone}</code>\n"
                f"Имя: {name if name else '-'}\n"
                f"Email: {email if email else '-'}\n"
                f"Отдел: {department_name}\n"
                f"Источник: {source_name}\n"
                f"Ответственный: {assignee['name']} (ID={assigned_by_id})\n"
                f"Распределение: {assignee['reason']}\n"
                f"UTM Source: {utm_source if utm_source else '-'}\n"
                f"ID лида: #{new_lead_id}\n"
                f"Ссылка: {lead_url}"
            )
            send_telegram_message(tg_message)

        else:
            result_data["action"] = "error"
            result_data["message"] = "Не удалось создать лид"
            return jsonify(result_data), 500

    logging.info(f"[WEBHOOK] Результат: {result_data}")
    return jsonify(result_data), 200


if __name__ == "__main__":
    logging.info(f"[SERVER] Запуск сервера на {SERVER_HOST}:{SERVER_PORT}")
    app.run(host=SERVER_HOST, port=SERVER_PORT, debug=False)