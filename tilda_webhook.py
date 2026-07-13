import csv
import bisect
import requests
import json
import logging
import re
import os
import threading
import mysql.connector
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# --- ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ---
load_dotenv()

# --- НАСТРОЙКИ ЛОГИРОВАНИЯ ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tilda_webhook.log'),
        logging.StreamHandler()
    ],
    force=True
)

# ============================================================
# --- КОНФИГУРАЦИЯ ---
# ============================================================

# Bitrix24
webhook = os.getenv('BITRIX_WEBHOOK')
BITRIX_BASE_URL = os.getenv('BITRIX_BASE_URL', 'https://b24-p41gmg.bitrix24.ru')

# MySQL
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
}

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Сервер
SERVER_HOST = os.getenv('TILDA_SERVER_HOST', '0.0.0.0')
SERVER_PORT = int(os.getenv('TILDA_SERVER_PORT', 5000))

# UF_CRM поле для отдела продаж
UF_CRM_FIELD = 'UF_CRM_1779024295'
UF_CRM_VALUE_EKATERINBURG = 58
UF_CRM_VALUE_CHELYABINSK = 60

# Fallback ответственные (если руководитель не найден в Б24)
ASSIGNED_CHELYABINSK = 64
ASSIGNED_EKATERINBURG = 1

# Рабочее время (МСК = UTC+3)
WORK_START_HOUR = 7   # 07:00 МСК включительно
WORK_END_HOUR = 16    # 16:00 МСК не включительно

# Таймауты мониторинга принятия лида (секунды)
NOTIFY_TIMEOUT_1 = 5 * 60
NOTIFY_TIMEOUT_2 = 10 * 60
NOTIFY_TIMEOUT_3 = 15 * 60
NOTIFY_TIMEOUT_4 = 30 * 60

# Статусы лидов
STATUS_IN_PROCESS = "IN_PROCESS"
STATUS_CONVERTED = "CONVERTED"
STATUS_NEW = "NEW"

# Маппинг: значение UF_CRM → ID отдела в Б24
DEPARTMENT_B24_ID = {
    UF_CRM_VALUE_CHELYABINSK: "16",
    UF_CRM_VALUE_EKATERINBURG: "10",
}

# ============================================================
# --- MySQL ---
# ============================================================

def get_db_connection():
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except mysql.connector.Error as e:
        logging.error(f"[DB] Ошибка подключения: {e}")
        return None


def db_save_assignment(lead_id, user_id, department, phone, work_hours):
    conn = get_db_connection()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO lead_assignments "
            "(lead_id, assigned_user, department, phone, created_at, work_hours) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (lead_id, user_id, department, phone, datetime.now(), 1 if work_hours else 0)
        )
        conn.commit()
    except mysql.connector.Error as e:
        logging.error(f"[DB] Ошибка сохранения назначения: {e}")
    finally:
        conn.close()


def db_save_event(lead_id, user_id, event_type, seconds_elapsed=None):
    conn = get_db_connection()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO lead_response_events "
            "(lead_id, user_id, event_type, event_at, seconds_elapsed) "
            "VALUES (%s,%s,%s,%s,%s)",
            (lead_id, user_id, event_type, datetime.now(), seconds_elapsed)
        )
        conn.commit()
    except mysql.connector.Error as e:
        logging.error(f"[DB] Ошибка сохранения события: {e}")
    finally:
        conn.close()

# ============================================================
# --- РЕГИОНЫ ---
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

# Поля которые не попадают в COMMENTS
EXCLUDED_COMMENT_FIELDS = {
    'phone', 'Phone', 'PHONE',
    'Телефон', 'телефон', 'ТЕЛЕФОН',
    'Ваш телефон', 'ваш телефон',
    'Номер телефона', 'номер телефона',
    'contact_phone', 'inputPhone', 'ct_phone',
    'dep_id', 'source_id',
    'utm_source', 'UTM_SOURCE', 'utm_medium', 'UTM_MEDIUM',
    'utm_campaign', 'UTM_CAMPAIGN', 'utm_content', 'UTM_CONTENT',
    'utm_term', 'UTM_TERM', 'utm_region', 'utm_region_id', 'utm_yclid',
    'formid', 'FORM_ID', 'formname', 'FORM_NAME'
}

# ============================================================
# --- КЭШ-СЧЁТЧИК ---
# ============================================================

_assignment_cache_lock = threading.Lock()
_assignment_cache: dict = {
    'date': datetime.now(timezone(timedelta(hours=3))).date(),
    'counts': {}
}


def _reset_cache_if_new_day():
    msk_offset = timezone(timedelta(hours=3))
    today = datetime.now(msk_offset).date()
    if _assignment_cache['date'] != today:
        logging.info(
            f"[CACHE] Новый день {today}. "
            f"Сбрасываем счётчики: {_assignment_cache['counts']}"
        )
        _assignment_cache['date'] = today
        _assignment_cache['counts'] = {}


def _init_cache_from_bitrix(user_ids: list):
    with _assignment_cache_lock:
        _reset_cache_if_new_day()
        if _assignment_cache['counts']:
            return

    real_counts = get_users_active_leads_today_batch(user_ids)

    with _assignment_cache_lock:
        if not _assignment_cache['counts']:
            _assignment_cache['counts'] = real_counts
            logging.info(f"[CACHE] Инициализирован из Bitrix24: {real_counts}")


def _atomic_select_and_increment(user_ids: list, bitrix_counts: dict) -> list:
    with _assignment_cache_lock:
        _reset_cache_if_new_day()

        workload = []
        for uid in user_ids:
            b24 = bitrix_counts.get(uid, 0)
            cache = _assignment_cache['counts'].get(uid, 0)
            workload.append({
                'id': uid,
                'b24_count': b24,
                'cache_count': cache,
                'total': b24 + cache,
            })

        workload.sort(key=lambda x: x['total'])
        chosen_id = workload[0]['id']

        prev = _assignment_cache['counts'].get(chosen_id, 0)
        _assignment_cache['counts'][chosen_id] = prev + 1

        logging.info(
            f"[CACHE] Атомарно выбран ID={chosen_id}: "
            f"кэш {prev} → {prev + 1}. "
            f"Нагрузка отдела: {[(w['id'], w['total']) for w in workload]}"
        )

    return workload


# ============================================================
# --- КЛАСС РОССВЯЗЬ ---
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
                raise ValueError(f"Файл {csv_file} пустой.")

            reader.fieldnames = [
                name.replace('\ufeff', '').strip()
                for name in reader.fieldnames
            ]
            logging.info(f"[ROSSVYAZ] Разделитель: '{delimiter}', заголовки: {reader.fieldnames}")

            required = {'АВС/ DEF', 'От', 'До', 'Регион', 'Оператор'}
            missing = required - set(reader.fieldnames)
            if missing:
                raise KeyError(f"Отсутствуют колонки: {missing}")

            for row in reader:
                clean = {
                    (k.replace('\ufeff', '').strip() if k else k):
                    (v.strip() if isinstance(v, str) else v)
                    for k, v in row.items()
                }
                code = clean['АВС/ DEF']
                if code not in self.ranges:
                    self.ranges[code] = []
                self.ranges[code].append((
                    int(clean['От']),
                    int(clean['До']),
                    clean['Регион'],
                    clean['Оператор']
                ))

        for code in self.ranges:
            self.ranges[code].sort(key=lambda x: x[0])
            self.starts[code] = [item[0] for item in self.ranges[code]]

        logging.info(
            f"[ROSSVYAZ] Загружено {sum(len(v) for v in self.ranges.values())} диапазонов."
        )

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
    return f"{BITRIX_BASE_URL.rstrip('/')}/crm/lead/details/{lead_id}/"


def is_working_hours() -> bool:
    now_msk = datetime.now(timezone(timedelta(hours=3)))
    return WORK_START_HOUR <= now_msk.hour < WORK_END_HOUR


def normalize_phone(raw_phone: str):
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


def extract_phone(data: dict) -> str:
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
            logging.info(f"[PHONE] Найден по ключу '{key}': {value}")
            return str(value).strip()

    for key, value in data.items():
        if any(m in str(key).lower() for m in ['phone', 'телефон', 'номер']):
            if value and str(value).strip():
                logging.info(f"[PHONE] Найден по похожему ключу '{key}': {value}")
                return str(value).strip()

    logging.warning("[PHONE] Телефон не найден.")
    return ''


def extract_form_region(data: dict) -> str:
    possible_keys = [
        'Укажите регион проживания', 'Укажите регион проживания:',
        'Укажите_регион_проживания', 'Укажите_регион_проживания:',
        'region', 'Region', 'REGION',
        'Регион', 'регион', 'Регион проживания', 'Регион_проживания'
    ]
    for key in possible_keys:
        value = data.get(key)
        if value and str(value).strip():
            logging.info(f"[REGION] Найден по ключу '{key}': {value}")
            return str(value).strip()

    for key, value in data.items():
        key_lower = str(key).lower().replace('_', ' ')
        if 'регион' in key_lower and 'utm' not in key_lower:
            if value and str(value).strip():
                logging.info(f"[REGION] Найден по похожему ключу '{key}': {value}")
                return str(value).strip()

    return ''


def determine_department(form_region: str, utm_region: str,
                         phone: str, rossvyaz_finder) -> tuple:
    if form_region:
        r = form_region.lower().strip()
        if 'челябинск' in r or r in CHELYABINSK_REGIONS:
            return UF_CRM_VALUE_CHELYABINSK, f"По региону из формы: {form_region}"
        if 'свердловск' in r or 'екатеринбург' in r or r in SVERDLOVSK_REGIONS:
            return UF_CRM_VALUE_EKATERINBURG, f"По региону из формы: {form_region}"

    if utm_region:
        r = utm_region.lower().strip()
        if r in SVERDLOVSK_REGIONS:
            return UF_CRM_VALUE_EKATERINBURG, f"По UTM региону: {utm_region}"
        if r in CHELYABINSK_REGIONS:
            return UF_CRM_VALUE_CHELYABINSK, f"По UTM региону: {utm_region}"

    if rossvyaz_finder and phone:
        info = rossvyaz_finder.find(phone)
        if info and info.get('region'):
            r = info['region'].lower()
            if 'свердловск' in r or 'екатеринбург' in r:
                return UF_CRM_VALUE_EKATERINBURG, f"По региону телефона: {info['region']}"
            if 'челябинск' in r:
                return UF_CRM_VALUE_CHELYABINSK, f"По региону телефона: {info['region']}"

    return UF_CRM_VALUE_CHELYABINSK, "Регион не определён (по умолчанию)"


def build_comments(data: dict) -> str:
    lines = [
        f"{key}: {value}"
        for key, value in data.items()
        if key not in EXCLUDED_COMMENT_FIELDS and value and str(value).strip()
    ]
    return "\n".join(lines)


def is_tilda_test_request(data: dict) -> bool:
    raw_phone = extract_phone(data)
    meaningful = [
        str(v).strip() for v in data.values()
        if v is not None and str(v).strip()
    ]
    return not raw_phone and len(meaningful) <= 2


# ============================================================
# --- BITRIX24 API ---
# ============================================================

def get_online_users(dept_b24_id: str) -> list:
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
        users = [
            {
                'id': int(u['ID']),
                'name': f"{u.get('NAME', '')} {u.get('LAST_NAME', '')}".strip()
            }
            for u in result
        ]
        logging.info(
            f"[ONLINE] Отдел {dept_b24_id}: онлайн {len(users)} чел. "
            f"ID: {[u['id'] for u in users]}"
        )
        return users
    except requests.exceptions.RequestException as e:
        logging.error(f"[ONLINE] Ошибка: {e}")
        return []


def get_users_active_leads_today_batch(user_ids: list) -> dict:
    if not user_ids:
        return {}

    today_str = datetime.now(timezone(timedelta(hours=3))).strftime('%Y-%m-%d')
    url = webhook + "crm.lead.list"

    params = {
        "select[]": ["ID", "ASSIGNED_BY_ID"],
        "filter[>=DATE_CREATE]": today_str,
    }
    for i, uid in enumerate(user_ids):
        params[f"filter[ASSIGNED_BY_ID][{i}]"] = uid
    for i, status in enumerate(["CONVERTED", "JUNK"]):
        params[f"filter[!STATUS_ID][{i}]"] = status

    counts = {uid: 0 for uid in user_ids}
    start = 0

    while True:
        params['start'] = start
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            leads = data.get('result', [])

            for lead in leads:
                uid = int(lead.get('ASSIGNED_BY_ID', 0))
                if uid in counts:
                    counts[uid] += 1

            total = data.get('total', 0)
            fetched = start + len(leads)
            logging.info(
                f"[BATCH] start={start}: получено {len(leads)}, "
                f"всего {total}. Счётчики: {counts}"
            )

            if fetched >= total or not leads:
                break
            start += 50

        except requests.exceptions.RequestException as e:
            logging.error(f"[BATCH] Ошибка запроса: {e}")
            break
        except (json.JSONDecodeError, ValueError) as e:
            logging.error(f"[BATCH] Ошибка ответа: {e}")
            break

    logging.info(f"[BATCH] Итог активных лидов за сегодня: {counts}")
    return counts


def select_assignee(uf_crm_value: int, head_id: int, fallback_id: int) -> dict:
    dept_b24_id = DEPARTMENT_B24_ID.get(uf_crm_value)
    dept_name = (
        "Екатеринбург" if uf_crm_value == UF_CRM_VALUE_EKATERINBURG
        else "Челябинск"
    )
    effective_head = head_id if head_id else fallback_id

    if not is_working_hours():
        logging.info(f"[ASSIGN] Нерабочее время → руководитель ID={effective_head}")
        return {
            'id': effective_head,
            'name': 'Руководитель отдела',
            'reason': 'Нерабочее время — назначен руководитель'
        }

    online_users = get_online_users(dept_b24_id) if dept_b24_id else []

    if not online_users:
        logging.warning(
            f"[ASSIGN] Отдел '{dept_name}': никого нет онлайн "
            f"→ руководитель ID={effective_head}"
        )
        return {
            'id': effective_head,
            'name': 'Руководитель отдела',
            'reason': 'Рабочее время, нет онлайн-сотрудников — назначен руководитель'
        }

    user_ids = [u['id'] for u in online_users]
    users_by_id = {u['id']: u['name'] for u in online_users}

    _init_cache_from_bitrix(user_ids)
    bitrix_counts = get_users_active_leads_today_batch(user_ids)
    workload = _atomic_select_and_increment(user_ids, bitrix_counts)

    for w in workload:
        w['name'] = users_by_id.get(w['id'], f"User {w['id']}")

    chosen = workload[0]

    logging.info(
        f"[ASSIGN] Выбран: {chosen['name']} (ID={chosen['id']}), "
        f"нагрузка: Б24={chosen['b24_count']} + кэш={chosen['cache_count']} "
        f"= итого {chosen['total']}. "
        f"Все: {[(w['name'], w['total']) for w in workload]}"
    )

    return {
        'id': chosen['id'],
        'name': chosen['name'],
        'reason': (
            f"Рабочее время — нагрузка {chosen['total']} активных лидов "
            f"(Б24={chosen['b24_count']}, кэш={chosen['cache_count']})"
        )
    }


def get_department_head(uf_crm_value: int):
    dept_id = DEPARTMENT_B24_ID.get(uf_crm_value)
    if not dept_id:
        logging.warning(f"[HEAD] Нет маппинга для UF_CRM={uf_crm_value}")
        return None

    try:
        response = requests.get(
            webhook + "department.get",
            params={"ID": dept_id},
            timeout=10
        )
        response.raise_for_status()
        result = response.json().get('result', [])

        if not result:
            logging.warning(f"[HEAD] Отдел ID={dept_id} не найден")
            return None

        dept = result[0]
        uf_head = dept.get('UF_HEAD')
        if not uf_head or str(uf_head) == "0":
            logging.warning(
                f"[HEAD] Отдел '{dept.get('NAME')}' (ID={dept_id}): "
                f"руководитель не назначен"
            )
            return None

        head_id = int(uf_head)
        logging.info(f"[HEAD] Отдел '{dept.get('NAME')}' → руководитель ID={head_id}")
        return head_id

    except requests.exceptions.RequestException as e:
        logging.error(f"[HEAD] Ошибка department.get: {e}")
        return None
    except (ValueError, IndexError, KeyError) as e:
        logging.error(f"[HEAD] Ошибка обработки ответа: {e}")
        return None


def send_im_message(to_user_id: int, message: str):
    try:
        response = requests.post(
            webhook + "im.message.add",
            data=json.dumps({
                "DIALOG_ID": str(to_user_id),
                "MESSAGE": message,
                "SYSTEM": "N",
                "URL_PREVIEW": "N"
            }),
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        response.raise_for_status()
        result = response.json()

        if result.get('result'):
            logging.info(f"[IM] → user {to_user_id}, msg_id={result['result']}")
            return result['result']
        else:
            logging.error(
                f"[IM] Ошибка → user {to_user_id}: "
                f"{result.get('error')} {result.get('error_description', '')}"
            )
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"[IM] Ошибка запроса: {e}")
        return None


def send_telegram_message(message: str):
    import time
    import socket

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("[TG] Токен или chat_id не настроены.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'HTML'}

    for attempt in range(1, 6):
        try:
            old = socket.getaddrinfo

            def ipv4_only(host, port, family=0, type=0, proto=0, flags=0):
                return old(host, port, socket.AF_INET, type, proto, flags)

            socket.getaddrinfo = ipv4_only
            try:
                resp = requests.post(url, json=payload, timeout=10)
            finally:
                socket.getaddrinfo = old

            resp.raise_for_status()
            logging.info(f"[TG] Отправлено (попытка {attempt}/5).")
            return
        except requests.exceptions.RequestException as e:
            logging.error(f"[TG] Ошибка (попытка {attempt}/5): {e}")
            if attempt < 5:
                time.sleep(3)

    logging.error("[TG] Все попытки исчерпаны.")


def get_duplicate_lead_id(phone: str):
    try:
        response = requests.post(
            webhook + "crm.duplicate.findbycomm",
            data=json.dumps({
                "entity_type": "LEAD",
                "type": "PHONE",
                "values": [phone]
            }),
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        if result.get('result') and result['result'].get('LEAD'):
            lead_id = result['result']['LEAD'][0]
            logging.info(f"[DUPLICATE] Найден для {phone}: ID={lead_id}")
            return lead_id
        return None
    except (requests.exceptions.RequestException, json.JSONDecodeError, IndexError) as e:
        logging.error(f"[DUPLICATE] Ошибка: {e}")
        return None


def get_source_name(source_id: str) -> str:
    try:
        response = requests.get(
            webhook + "crm.status.list",
            params={"filter[ENTITY_ID]": "SOURCE", "filter[STATUS_ID]": source_id},
            timeout=10
        )
        response.raise_for_status()
        result = response.json().get('result', [])
        name = result[0].get('NAME', source_id) if result else source_id
        logging.info(f"[SOURCE] '{source_id}' → '{name}'")
        return name
    except requests.exceptions.RequestException as e:
        logging.error(f"[SOURCE] Ошибка: {e}")
        return source_id


def get_lead_details(lead_id: int):
    try:
        response = requests.get(
            webhook + "crm.lead.get",
            params={'ID': lead_id},
            timeout=30
        )
        response.raise_for_status()
        result = response.json().get('result')
        if result:
            return {
                "STATUS_ID": result.get("STATUS_ID"),
                "ASSIGNED_BY_ID": result.get("ASSIGNED_BY_ID")
            }
        logging.warning(f"[DETAILS] Лид {lead_id} не найден.")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"[DETAILS] Ошибка: {e}")
        return None


def update_lead_status(lead_id: int, status_id: str = "NEW") -> bool:
    try:
        response = requests.post(
            webhook + "crm.lead.update",
            json={"id": lead_id, "fields": {"STATUS_ID": status_id}},
            timeout=30
        )
        response.raise_for_status()
        ok = bool(response.json().get('result'))
        if ok:
            logging.info(f"[UPDATE] Лид {lead_id} → статус '{status_id}'.")
        return ok
    except requests.exceptions.RequestException as e:
        logging.error(f"[UPDATE] Ошибка: {e}")
        return False


def create_b24_task(lead_id: int, responsible_id: int):
    deadline = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S')
    try:
        response = requests.post(
            webhook + "tasks.task.add",
            json={
                "fields": {
                    "TITLE": "Повторная заявка",
                    "DESCRIPTION": (
                        f"Позвони, клиент оставил повторную заявку.\n"
                        f"Лид: {get_lead_url(lead_id)}"
                    ),
                    "RESPONSIBLE_ID": responsible_id,
                    "UF_CRM_TASK": [f"L_{lead_id}"],
                    "DEADLINE": deadline,
                }
            },
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        if result.get('result') and result['result'].get('task'):
            task_id = result['result']['task']['id']
            logging.info(f"[TASK] Создана задача {task_id} для лида {lead_id}.")
            return task_id
        logging.error(f"[TASK] Не удалось создать: {result.get('error_description', result)}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"[TASK] Ошибка: {e}")
        return None


def add_lead_timeline_comment(lead_id: int, comment: str) -> bool:
    """
    Добавляет запись в ленту лида через crm.timeline.comment.add
    """
    try:
        response = requests.post(
            webhook + "crm.timeline.comment.add",
            json={
                "fields": {
                    "ENTITY_ID": lead_id,
                    "ENTITY_TYPE": "lead",
                    "COMMENT": comment,
                }
            },
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        if result.get('result'):
            logging.info(f"[TIMELINE] Комментарий добавлен к лиду {lead_id}.")
            return True
        logging.error(f"[TIMELINE] Ошибка: {result.get('error_description', result)}")
        return False
    except requests.exceptions.RequestException as e:
        logging.error(f"[TIMELINE] Ошибка запроса: {e}")
        return False


def create_booking_task(
        lead_id: int,
        responsible_id: int,
        full_name: str,
        phone: str,
        booking_date: str,
        booking_time: str,
        booking_address: str
):
    """
    Создаёт задачу менеджеру для подтверждения записи.
    Дедлайн: за 2 часа до времени записи.
    """
    try:
        # Считаем дедлайн = booking_date + booking_time - 2 часа
        booking_dt = datetime.strptime(
            f"{booking_date} {booking_time}", "%Y-%m-%d %H:%M"
        )
        deadline_dt = booking_dt - timedelta(hours=2)
        deadline_str = deadline_dt.strftime('%Y-%m-%dT%H:%M:%S')
    except ValueError as e:
        logging.error(f"[BOOKING_TASK] Ошибка парсинга даты/времени: {e}")
        # Если дата некорректна — дедлайн через день
        deadline_str = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S')

    try:
        response = requests.post(
            webhook + "tasks.task.add",
            json={
                "fields": {
                    "TITLE": f"Подтвердить запись — {full_name}",
                    "DESCRIPTION": (
                        f"Клиент {full_name} ({phone}) записался на {booking_date} "
                        f"в {booking_time}\n"
                        f"по адресу: {booking_address}\n\n"
                        f"Необходимо позвонить и подтвердить запись.\n"
                        f"Лид: {get_lead_url(lead_id)}"
                    ),
                    "RESPONSIBLE_ID": responsible_id,
                    "UF_CRM_TASK": [f"L_{lead_id}"],
                    "DEADLINE": deadline_str,
                }
            },
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        if result.get('result') and result['result'].get('task'):
            task_id = result['result']['task']['id']
            logging.info(f"[BOOKING_TASK] Задача {task_id} создана для лида {lead_id}.")
            return task_id
        logging.error(
            f"[BOOKING_TASK] Не удалось создать: {result.get('error_description', result)}"
        )
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"[BOOKING_TASK] Ошибка запроса: {e}")
        return None


def find_lead_by_phone(phone: str):
    """
    Ищет лид по телефону через crm.lead.list.
    Возвращает ID первого найденного или None.
    """
    try:
        response = requests.get(
            webhook + "crm.lead.list",
            params={
                "filter[PHONE]": phone,
                "select[]": ["ID", "ASSIGNED_BY_ID"],
                "order[DATE_CREATE]": "DESC",
                "start": 0,
            },
            timeout=15
        )
        response.raise_for_status()
        result = response.json().get('result', [])
        if result:
            lead_id = int(result[0]['ID'])
            logging.info(f"[FIND_LEAD] По телефону {phone} найден лид ID={lead_id}")
            return lead_id
        logging.warning(f"[FIND_LEAD] Лид по телефону {phone} не найден.")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"[FIND_LEAD] Ошибка: {e}")
        return None


def create_lead(
        title: str, name: str, phone: str, email: str, comments: str,
        uf_crm_value: int, utm_source: str = "", utm_medium: str = "",
        utm_campaign: str = "", utm_content: str = "", utm_term: str = "",
        source_id: str = "WEB", source_description: str = "",
        assigned_by_id: int = 1, status_id: str = "NEW", opened: str = "Y"
):
    import time

    source_id = str(source_id).strip() if source_id else "WEB"
    fields = {
        "TITLE": title, "NAME": name,
        "STATUS_ID": status_id, "OPENED": opened,
        "ASSIGNED_BY_ID": assigned_by_id,
        "SOURCE_ID": source_id, "SOURCE_DESCRIPTION": source_description,
        "COMMENTS": comments, UF_CRM_FIELD: uf_crm_value,
    }
    if utm_source:   fields["UTM_SOURCE"] = utm_source
    if utm_medium:   fields["UTM_MEDIUM"] = utm_medium
    if utm_campaign: fields["UTM_CAMPAIGN"] = utm_campaign
    if utm_content:  fields["UTM_CONTENT"] = utm_content
    if utm_term:     fields["UTM_TERM"] = utm_term
    if phone:        fields["PHONE"] = [{"VALUE": phone, "VALUE_TYPE": "WORK"}]
    if email:        fields["EMAIL"] = [{"VALUE": email, "VALUE_TYPE": "WORK"}]

    payload = json.dumps(
        {"fields": fields, "params": {"REGISTER_SONET_EVENT": "Y"}},
        ensure_ascii=False
    )
    headers = {'Content-Type': 'application/json'}
    last_error = ""

    for attempt in range(1, 11):
        try:
            logging.info(
                f"[CREATE] Попытка {attempt}/10. "
                f"ASSIGNED={assigned_by_id}, {UF_CRM_FIELD}={uf_crm_value}"
            )
            if attempt == 1:
                logging.info(f"[CREATE] Payload: {payload}")

            response = requests.post(
                webhook + "crm.lead.add",
                data=payload, headers=headers, timeout=30
            )
            response.raise_for_status()
            result = response.json()

            if 'result' in result:
                logging.info(f"[CREATE] Лид создан ID={result['result']} (попытка {attempt})")
                return result['result']
            elif 'error' in result:
                last_error = f"{result['error']}: {result.get('error_description', '')}"
                logging.error(f"[CREATE] Ошибка Б24 (попытка {attempt}): {last_error}")
            else:
                last_error = f"Неизвестный ответ: {result}"
                logging.error(f"[CREATE] {last_error}")

        except requests.exceptions.RequestException as e:
            last_error = str(e)
            logging.error(f"[CREATE] Ошибка запроса (попытка {attempt}): {e}")
        except json.JSONDecodeError as e:
            last_error = str(e)
            logging.error(f"[CREATE] Ошибка JSON (попытка {attempt}): {e}")

        if attempt < 10:
            time.sleep(3)

    dept_name = "Екатеринбург" if uf_crm_value == UF_CRM_VALUE_EKATERINBURG else "Челябинск"
    send_telegram_message(
        f"🚨 <b>ОШИБКА: Не удалось создать лид в CRM</b>\n\n"
        f"Телефон: <code>{phone}</code>\n"
        f"Имя: {name or '-'}\nОтдел: {dept_name}\n"
        f"SOURCE_ID: {source_id}\nПопыток: 10\n"
        f"Ошибка: {last_error}\n\nЛид нужно создать вручную!"
    )
    return None


# ============================================================
# --- МОНИТОРИНГ ПРИНЯТИЯ ЛИДА ---
# ============================================================

_lead_monitor_lock = threading.Lock()
_active_monitors: dict = {}


def start_lead_acceptance_monitor(
        lead_id: int, assigned_user_id: int, assigned_user_name: str,
        head_id: int, department_name: str, lead_name: str, phone: str
):
    monitor_data = {
        'lead_id': lead_id,
        'assigned_user_id': assigned_user_id,
        'assigned_user_name': assigned_user_name,
        'head_id': head_id,
        'department_name': department_name,
        'lead_name': lead_name,
        'phone': phone,
        'lead_url': get_lead_url(lead_id),
        'assigned_at': datetime.now(),
        'stop': False,
        'notified_1': False,
        'notified_2': False,
        'notified_3': False,
        'notified_4': False,
    }

    with _lead_monitor_lock:
        if lead_id in _active_monitors:
            _active_monitors[lead_id]['stop'] = True
            logging.info(f"[MONITOR] Старый мониторинг лида {lead_id} остановлен.")
        _active_monitors[lead_id] = monitor_data

    thread = threading.Thread(
        target=_monitor_lead_acceptance,
        args=(monitor_data,),
        daemon=True,
        name=f"monitor-lead-{lead_id}"
    )
    thread.start()
    logging.info(
        f"[MONITOR] Запущен для лида {lead_id} → "
        f"user {assigned_user_id} ({assigned_user_name})"
    )


def _monitor_lead_acceptance(monitor_data: dict):
    import time

    lead_id = monitor_data['lead_id']
    CHECK_INTERVAL = 30

    schedule = [
        (NOTIFY_TIMEOUT_1, 'notified_1', False),
        (NOTIFY_TIMEOUT_2, 'notified_2', False),
        (NOTIFY_TIMEOUT_3, 'notified_3', True),
        (NOTIFY_TIMEOUT_4, 'notified_4', True),
    ]

    while True:
        time.sleep(CHECK_INTERVAL)

        with _lead_monitor_lock:
            if monitor_data.get('stop'):
                logging.info(f"[MONITOR] Лид {lead_id}: остановлен по флагу.")
                return

        with _lead_monitor_lock:
            assigned_at = monitor_data['assigned_at']
            current_assigned_id = monitor_data['assigned_user_id']
            head_id = monitor_data['head_id']
            department_name = monitor_data['department_name']
            lead_name = monitor_data['lead_name']
            lead_url = monitor_data['lead_url']

        elapsed = (datetime.now() - assigned_at).total_seconds()

        details = get_lead_details(lead_id)
        if not details:
            logging.warning(f"[MONITOR] Лид {lead_id}: нет данных, ждём следующей итерации.")
            continue

        b24_status = details.get('STATUS_ID', '')
        b24_assigned_id = int(details.get('ASSIGNED_BY_ID', 0))

        if b24_assigned_id and b24_assigned_id != current_assigned_id:
            logging.info(
                f"[MONITOR] Лид {lead_id}: ответственный сменился "
                f"{current_assigned_id} → {b24_assigned_id}."
            )
            send_im_message(
                b24_assigned_id,
                f"📋 Вам назначен лид!\n\n"
                f"Лид: {lead_name}\n"
                f"Отдел: {department_name}\n"
                f"Возьмите его в работу как можно скорее!\n"
                f"Ссылка: {lead_url}"
            )
            with _lead_monitor_lock:
                monitor_data['assigned_user_id'] = b24_assigned_id
                monitor_data['assigned_at'] = datetime.now()
                monitor_data['notified_1'] = False
                monitor_data['notified_2'] = False
                monitor_data['notified_3'] = False
                monitor_data['notified_4'] = False
            continue

        if b24_status == STATUS_IN_PROCESS:
            elapsed_int = int(elapsed)
            logging.info(
                f"[MONITOR] Лид {lead_id}: принят через "
                f"{elapsed_int // 60} мин {elapsed_int % 60} сек."
            )
            with _lead_monitor_lock:
                _active_monitors.pop(lead_id, None)
            return

        if b24_status == STATUS_CONVERTED:
            logging.info(f"[MONITOR] Лид {lead_id}: конвертирован. Мониторинг завершён.")
            with _lead_monitor_lock:
                _active_monitors.pop(lead_id, None)
            return

        for timeout_sec, flag_key, notify_head in schedule:
            if elapsed >= timeout_sec and not monitor_data.get(flag_key):
                minutes = timeout_sec // 60
                _send_acceptance_notification(
                    lead_id=lead_id,
                    user_id=current_assigned_id,
                    head_id=head_id,
                    department_name=department_name,
                    lead_name=lead_name,
                    lead_url=lead_url,
                    minutes_elapsed=minutes,
                    notify_head=notify_head,
                    flag_key=flag_key,
                    monitor_data=monitor_data
                )
                break

        if all(monitor_data.get(f) for f in ['notified_1', 'notified_2', 'notified_3', 'notified_4']):
            logging.info(f"[MONITOR] Лид {lead_id}: все уведомления отправлены. Завершаем.")
            with _lead_monitor_lock:
                _active_monitors.pop(lead_id, None)
            return


def _send_acceptance_notification(
        lead_id: int, user_id: int, head_id: int,
        department_name: str, lead_name: str, lead_url: str,
        minutes_elapsed: int, notify_head: bool,
        flag_key: str, monitor_data: dict
):
    with _lead_monitor_lock:
        if monitor_data.get(flag_key):
            return
        monitor_data[flag_key] = True

    if minutes_elapsed < 15:
        user_msg = (
            f"⚠️ Лид не принят уже {minutes_elapsed} мин!\n\n"
            f"Лид: {lead_name}\n"
            f"Срочно возьмите его в работу!\n"
            f"Ссылка: {lead_url}"
        )
    else:
        user_msg = (
            f"🚨 СРОЧНО! Лид не принят уже {minutes_elapsed} мин!\n\n"
            f"Лид: {lead_name}\n"
            f"Руководитель уже получил уведомление!\n"
            f"Ссылка: {lead_url}"
        )
    send_im_message(user_id, user_msg)
    logging.info(f"[MONITOR] {minutes_elapsed} мин → уведомление user {user_id}, лид {lead_id}")

    if notify_head and head_id:
        send_im_message(
            head_id,
            f"🚨 Лид не принят {minutes_elapsed} мин!\n\n"
            f"Отдел: {department_name}\n"
            f"Лид: {lead_name}\n"
            f"Ответственный (ID={user_id}) не реагирует.\n"
            f"Ссылка: {lead_url}"
        )
        logging.info(
            f"[MONITOR] {minutes_elapsed} мин → уведомление руководитель {head_id}, лид {lead_id}"
        )


def notify_assignee_new_lead(
        lead_id: int, user_id: int, user_name: str,
        lead_name: str, source_name: str, department_name: str, phone: str
):
    lead_url = get_lead_url(lead_id)
    send_im_message(
        user_id,
        f"🆕 На вас создан новый лид!\n\n"
        f"Клиент: {lead_name}\n"
        f"Отдел: {department_name}\n\n"
        f"Возьмите лид в работу как можно скорее!\n"
        f"Ссылка: {lead_url}"
    )
    logging.info(f"[NOTIFY] Новый лид {lead_id} → user {user_id} ({user_name})")


# ============================================================
# --- ФОНОВАЯ ОБРАБОТКА ДУБЛЯ ---
# ============================================================

def _process_duplicate(
        duplicate_lead_id: int, phone: str, name: str,
        department_name: str, source_name: str, head_id: int
):
    lead_url = get_lead_url(duplicate_lead_id)
    details = get_lead_details(duplicate_lead_id)
    current_status = details.get('STATUS_ID', '') if details else ''

    status_updated = False
    task_id = None
    im_msg_id = None

    if current_status == STATUS_CONVERTED:
        logging.info(f"[DUP] Лид {duplicate_lead_id}: CONVERTED, статус не меняем.")
        if head_id:
            im_msg_id = send_im_message(
                head_id,
                f"ℹ️ Повторная заявка на конвертированный лид!\n\n"
                f"Клиент снова оставил заявку, но лид уже конвертирован.\n"
                f"Имя: {name or '-'}\n"
                f"Отдел: {department_name}\n"
                f"Ссылка: {lead_url}"
            )
    else:
        status_updated = update_lead_status(duplicate_lead_id, STATUS_NEW)
        if details and details.get("ASSIGNED_BY_ID"):
            responsible_id = int(details["ASSIGNED_BY_ID"])
            task_id = create_b24_task(duplicate_lead_id, responsible_id)
            if head_id:
                im_msg_id = send_im_message(
                    head_id,
                    f"🔔 Повторная заявка!\n\n"
                    f"Клиент снова оставил заявку.\n"
                    f"Имя: {name or '-'}\n"
                    f"Отдел: {department_name}\n"
                    f"Задача: {'#' + str(task_id) if task_id else 'ошибка'}\n"
                    f"Ссылка: {lead_url}"
                )

    send_telegram_message(
        f"<b>Повторная заявка</b>\n\n"
        f"Телефон: <code>{phone}</code>\n"
        f"Имя: {name or '-'}\nОтдел: {department_name}\n"
        f"Источник: {source_name}\n"
        f"Лид: #{duplicate_lead_id} | Статус: {current_status}\n"
        f"→ NEW: {'✅' if status_updated else ('⏩ CONVERTED' if current_status == STATUS_CONVERTED else '❌')}\n"
        f"Задача: {'#' + str(task_id) if task_id else ('—' if current_status == STATUS_CONVERTED else '❌')}\n"
        f"Рук-ль уведомлён: {'✅' if im_msg_id else '❌'}\n"
        f"Ссылка: {lead_url}"
    )

    logging.info(
        f"[DUP] Фоновая обработка завершена: лид #{duplicate_lead_id}, "
        f"status_updated={status_updated}, task_id={task_id}, im_msg_id={im_msg_id}"
    )


# ============================================================
# --- ОБРАБОТЧИК BOOKING_UPDATE ---
# ============================================================

def handle_booking_update(data: dict):
    """
    Обрабатывает запрос доотправки записи клиента (action=booking_update).

    Алгоритм:
    1. Берём lead_id из запроса или ищем по телефону
    2. Добавляем комментарий в ленту лида
    3. Создаём задачу ответственному
    4. Возвращаем результат
    """
    logging.info(f"[BOOKING] Входящий запрос booking_update: {data}")

    # --- Извлекаем поля ---
    lead_id_raw = data.get('lead_id')
    ticket_id = data.get('ticket_id', '')
    phone_raw = data.get('phone', '')
    full_name = data.get('full_name', '')
    booking_date = data.get('booking_date', '')
    booking_time = data.get('booking_time', '')
    booking_address = data.get('booking_address', '')
    booked_at = data.get('booked_at', '')

    # Валидация обязательных полей
    missing = []
    if not booking_date:
        missing.append('booking_date')
    if not booking_time:
        missing.append('booking_time')
    if not booking_address:
        missing.append('booking_address')
    if not full_name:
        missing.append('full_name')

    if missing:
        logging.error(f"[BOOKING] Отсутствуют обязательные поля: {missing}")
        return jsonify({
            "success": False,
            "error": f"Отсутствуют обязательные поля: {', '.join(missing)}"
        }), 400

    phone = normalize_phone(str(phone_raw)) if phone_raw else None

    # --- Ищем лид ---
    lead_id = None

    # Сначала по lead_id из запроса
    if lead_id_raw:
        try:
            lead_id = int(lead_id_raw)
            logging.info(f"[BOOKING] Используем lead_id={lead_id} из запроса.")
        except (ValueError, TypeError):
            logging.warning(f"[BOOKING] Некорректный lead_id='{lead_id_raw}', ищем по телефону.")

    # Если lead_id нет или невалиден — ищем по телефону
    if not lead_id and phone:
        lead_id = get_duplicate_lead_id(phone) or find_lead_by_phone(phone)

    if not lead_id:
        logging.error(f"[BOOKING] Лид не найден: lead_id={lead_id_raw}, phone={phone}")
        return jsonify({
            "success": False,
            "error": "Лид не найден по lead_id и по телефону"
        }), 404

    # --- Получаем детали лида (ответственный) ---
    details = get_lead_details(lead_id)
    if not details:
        logging.error(f"[BOOKING] Не удалось получить данные лида {lead_id}")
        return jsonify({
            "success": False,
            "error": f"Не удалось получить данные лида {lead_id}"
        }), 500

    responsible_id = int(details.get('ASSIGNED_BY_ID', 0))
    if not responsible_id:
        logging.warning(f"[BOOKING] У лида {lead_id} нет ответственного.")
        return jsonify({
            "success": False,
            "error": f"У лида {lead_id} нет ответственного менеджера"
        }), 500

    # --- Добавляем комментарий в ленту лида ---
    comment_text = (
        f"Клиент записался на консультацию:\n"
        f"📅 Дата: {booking_date}\n"
        f"🕐 Время: {booking_time}\n"
        f"📍 Адрес: {booking_address}"
    )
    comment_added = add_lead_timeline_comment(lead_id, comment_text)

    if not comment_added:
        logging.warning(f"[BOOKING] Не удалось добавить комментарий к лиду {lead_id}.")

    # --- Создаём задачу ответственному ---
    task_id = create_booking_task(
        lead_id=lead_id,
        responsible_id=responsible_id,
        full_name=full_name,
        phone=str(phone_raw),
        booking_date=booking_date,
        booking_time=booking_time,
        booking_address=booking_address,
    )

    if not task_id:
        logging.error(f"[BOOKING] Не удалось создать задачу для лида {lead_id}.")
        return jsonify({
            "success": False,
            "error": "Не удалось создать задачу в Б24"
        }), 500

    # --- Уведомляем ответственного в Б24 ---
    send_im_message(
        responsible_id,
        f"📅 Клиент записался на консультацию!\n\n"
        f"Имя: {full_name}\n"
        f"Телефон: {phone_raw}\n"
        f"Дата: {booking_date} в {booking_time}\n"
        f"Адрес: {booking_address}\n\n"
        f"Задача создана: #{task_id}\n"
        f"Ссылка на лид: {get_lead_url(lead_id)}"
    )

    # --- Уведомляем в Telegram ---
    send_telegram_message(
        f"<b>Новая запись клиента</b>\n\n"
        f"Имя: {full_name}\n"
        f"Телефон: <code>{phone_raw}</code>\n"
        f"Дата: {booking_date} в {booking_time}\n"
        f"Адрес: {booking_address}\n"
        f"Лид: #{lead_id}\n"
        f"Задача: #{task_id}\n"
        f"Ссылка: {get_lead_url(lead_id)}"
    )

    logging.info(
        f"[BOOKING] Успешно: лид={lead_id}, задача={task_id}, "
        f"комментарий={'добавлен' if comment_added else 'ошибка'}"
    )

    return jsonify({
        "success": True,
        "task_id": str(task_id),
        "lead_id": lead_id,
        "comment_added": comment_added,
    }), 200


# ============================================================
# --- FLASK ---
# ============================================================

app = Flask(__name__)

ranges_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ranges.csv')
if os.path.exists(ranges_file):
    rossvyaz_finder = RossvyazMobile(ranges_file)
else:
    logging.warning(f"[INIT] ranges.csv не найден: {ranges_file}")
    rossvyaz_finder = None


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "ok", "message": "Tilda webhook server is running"}), 200


@app.route('/webhook/tilda', methods=['GET', 'POST'])
def tilda_webhook():
    logging.info("=" * 60)
    logging.info("[WEBHOOK] Входящий запрос")

    # GET без ct_phone — проверка доступности
    if request.method == 'GET' and not request.args.get('ct_phone'):
        return jsonify({"status": "ok", "message": "Webhook is available"}), 200

    # Парсим тело запроса
    if request.method == 'GET':
        data = request.args.to_dict()
    elif request.is_json:
        data = request.get_json()
    else:
        data = request.form.to_dict()

    logging.info(f"[WEBHOOK] Данные: {data}")

    # --- Проверяем action ДО любой другой логики ---
    # Если это booking_update — сразу передаём в обработчик
    action = request.headers.get('X-Action', '').strip().lower()
    if not action:
        action = str(data.get('action', '')).strip().lower()

    if action == 'booking_update':
        logging.info("[WEBHOOK] Обнаружен action=booking_update → передаём в handle_booking_update.")
        return handle_booking_update(data)

    # Параметры из URL
    url_dep_id = request.args.get('dep_id')
    url_source_id = str(request.args.get('source_id', 'WEB')).strip()
    source_name = get_source_name(url_source_id)

    logging.info(f"[WEBHOOK] dep_id='{url_dep_id}', source_id='{url_source_id}', source='{source_name}'")
    logging.info("[WEBHOOK] === ПОЛЯ ===")
    for k, v in data.items():
        logging.info(f"  '{k}' = '{v}'")

    # Тестовый запрос Tilda
    if is_tilda_test_request(data):
        logging.info("[WEBHOOK] Тестовый запрос — пропускаем.")
        return jsonify({"status": "ok", "message": "Tilda webhook test accepted"}), 200

    # Телефон
    raw_phone = request.args.get('ct_phone', '').strip() or extract_phone(data)
    phone = normalize_phone(raw_phone)
    if not phone:
        logging.warning(f"[WEBHOOK] Некорректный телефон: '{raw_phone}'")
        return jsonify({"status": "ok", "message": "Invalid phone"}), 200

    logging.info(f"[WEBHOOK] Телефон: {phone}")

    # Поля формы
    name = data.get('Name') or data.get('name') or data.get('NAME') or ''
    email = data.get('Email') or data.get('email') or data.get('EMAIL') or ''
    utm_source = data.get('utm_source') or data.get('UTM_SOURCE') or ''
    utm_medium = data.get('utm_medium') or data.get('UTM_MEDIUM') or ''
    utm_campaign = data.get('utm_campaign') or data.get('UTM_CAMPAIGN') or ''
    utm_content = data.get('utm_content') or data.get('UTM_CONTENT') or ''
    utm_term = data.get('utm_term') or data.get('UTM_TERM') or ''

    raw_utm_region = data.get('utm_region') or ''
    utm_region = '' if (raw_utm_region.startswith('{') and raw_utm_region.endswith('}')) else raw_utm_region
    form_region = extract_form_region(data)
    logging.info(f"[WEBHOOK] form_region='{form_region}', utm_region='{utm_region}'")

    # Определяем отдел
    if url_dep_id and url_dep_id in ('58', '60'):
        uf_crm_value = int(url_dep_id)
        department_source = f"По dep_id из URL: {url_dep_id}"
    else:
        uf_crm_value, department_source = determine_department(
            form_region, utm_region, phone, rossvyaz_finder
        )

    department_name = "Екатеринбург" if uf_crm_value == UF_CRM_VALUE_EKATERINBURG else "Челябинск"
    fallback_id = ASSIGNED_CHELYABINSK if uf_crm_value == UF_CRM_VALUE_CHELYABINSK else ASSIGNED_EKATERINBURG

    # Сначала проверяем дубль
    duplicate_lead_id = get_duplicate_lead_id(phone)

    # Руководитель нужен в обоих случаях
    head_id = get_department_head(uf_crm_value)
    logging.info(f"[WEBHOOK] Руководитель '{department_name}': {head_id or 'не назначен'}")

    comments = build_comments(data)

    # =================================================
    # ДУБЛИКАТ
    # =================================================
    if duplicate_lead_id:
        logging.info(
            f"[WEBHOOK] Дубликат: лид #{duplicate_lead_id}, "
            f"телефон {phone}. Отвечаем 200 немедленно."
        )
        threading.Thread(
            target=_process_duplicate,
            args=(duplicate_lead_id, phone, name, department_name,
                  source_name, head_id),
            daemon=True,
            name=f"dup-{duplicate_lead_id}"
        ).start()

        return jsonify({
            "status": "ok",
            "duplicate": True,
            "lead_id": duplicate_lead_id,
            "message": "Принято, дубликат"
        }), 200

    # =================================================
    # НОВЫЙ ЛИД
    # =================================================
    assignee = select_assignee(uf_crm_value, head_id or fallback_id, fallback_id)
    assigned_by_id = assignee['id']
    logging.info(
        f"[WEBHOOK] Ответственный: ID={assigned_by_id} ({assignee['name']}). "
        f"{assignee['reason']}"
    )

    title = f"Рекламный лид: {name}" if name else "Рекламный лид"

    new_lead_id = create_lead(
        title=title, name=name, phone=phone, email=email,
        comments=comments, uf_crm_value=uf_crm_value,
        utm_source=utm_source, utm_medium=utm_medium,
        utm_campaign=utm_campaign, utm_content=utm_content,
        utm_term=utm_term, source_id=url_source_id,
        source_description=department_source,
        assigned_by_id=assigned_by_id
    )

    if new_lead_id:
        lead_url = get_lead_url(new_lead_id)
        result_data = {
            "status": "ok",
            "action": "created",
            "lead_id": new_lead_id,
            "phone": phone,
            "department": department_name,
            "uf_crm_value": uf_crm_value,
            "department_source": department_source,
        }

        db_save_assignment(new_lead_id, assigned_by_id, department_name,
                           phone, is_working_hours())

        notify_assignee_new_lead(
            lead_id=new_lead_id, user_id=assigned_by_id,
            user_name=assignee['name'], lead_name=title,
            source_name=source_name, department_name=department_name,
            phone=phone
        )

        start_lead_acceptance_monitor(
            lead_id=new_lead_id, assigned_user_id=assigned_by_id,
            assigned_user_name=assignee['name'],
            head_id=head_id or fallback_id,
            department_name=department_name,
            lead_name=title, phone=phone
        )

        send_telegram_message(
            f"<b>Новый Рекламный лид</b>\n\n"
            f"Телефон: <code>{phone}</code>\n"
            f"Имя: {name or '-'}\n"
            f"Отдел: {department_name}\n"
            f"Источник: {source_name}\n"
            f"Ответственный: {assignee['name']} (ID={assigned_by_id})\n"
            f"Распределение: {assignee['reason']}\n"
            f"UTM: {utm_source or '-'}\n"
            f"ID: #{new_lead_id}\n"
            f"Ссылка: {lead_url}"
        )

        logging.info(f"[WEBHOOK] Готово: {result_data}")
        return jsonify(result_data), 200

    else:
        error_data = {
            "status": "error",
            "action": "error",
            "message": "Не удалось создать лид",
            "phone": phone,
            "department": department_name,
        }
        logging.error(f"[WEBHOOK] Ошибка создания лида: {error_data}")
        return jsonify(error_data), 500


# ============================================================
# --- МАРШРУТ ДЛЯ DEBT-QUIZ ---
# ============================================================

@app.route('/webhook/debt-quiz', methods=['POST'])
def debt_quiz_webhook():
    """
    Единая точка входа для debt-quiz.
    Тип запроса определяется по:
      1. Заголовку X-Action
      2. Полю action в теле запроса
    """
    logging.info("=" * 60)
    logging.info("[DEBT-QUIZ] Входящий запрос")

    # Определяем action
    action = request.headers.get('X-Action', '').strip().lower()

    if request.is_json:
        data = request.get_json() or {}
    else:
        data = request.form.to_dict()

    # Если заголовок не задан — берём из тела
    if not action:
        action = str(data.get('action', 'lead')).strip().lower()

    logging.info(f"[DEBT-QUIZ] action='{action}', данные: {data}")

    # -------------------------------------------------------
    # ЗАПРОС 2: Доотправка записи клиента
    # -------------------------------------------------------
    if action == 'booking_update':
        return handle_booking_update(data)

    # -------------------------------------------------------
    # ЗАПРОС 1: Создание/обновление лида (action=lead или любой другой)
    # -------------------------------------------------------
    logging.info("[DEBT-QUIZ] Обрабатываем как создание лида.")

    # Извлекаем поля из debt-quiz формата
    external_id = data.get('external_id', '')
    name = data.get('name', '')
    phone_raw = data.get('phone', '')
    region = data.get('region', '')
    city = data.get('city', '')
    debt_amount = data.get('debt_amount', '')
    has_debt = data.get('has_debt', '')
    utm_campaign = data.get('utm_campaign', '')

    phone = normalize_phone(str(phone_raw))
    if not phone:
        logging.warning(f"[DEBT-QUIZ] Некорректный телефон: '{phone_raw}'")
        return jsonify({"status": "ok", "message": "Invalid phone"}), 200

    logging.info(f"[DEBT-QUIZ] Телефон: {phone}, region='{region}', city='{city}'")

    # Определяем отдел по region/city
    form_region = region or city
    uf_crm_value, department_source = determine_department(
        form_region, '', phone, rossvyaz_finder
    )
    department_name = "Екатеринбург" if uf_crm_value == UF_CRM_VALUE_EKATERINBURG else "Челябинск"
    fallback_id = ASSIGNED_CHELYABINSK if uf_crm_value == UF_CRM_VALUE_CHELYABINSK else ASSIGNED_EKATERINBURG

    # Проверяем дубль
    duplicate_lead_id = get_duplicate_lead_id(phone)

    # Руководитель
    head_id = get_department_head(uf_crm_value)
    logging.info(f"[DEBT-QUIZ] Руководитель '{department_name}': {head_id or 'не назначен'}")

    # Собираем комментарий из полей квиза
    comments_parts = []
    if external_id:
        comments_parts.append(f"ID квиза: {external_id}")
    if debt_amount:
        comments_parts.append(f"Сумма долга: {debt_amount}")
    if has_debt is not None and has_debt != '':
        has_debt_str = "Да" if str(has_debt).lower() in ('true', '1', 'yes') else "Нет"
        comments_parts.append(f"Долг в ФССП: {has_debt_str}")
    if region:
        comments_parts.append(f"Регион: {region}")
    if city:
        comments_parts.append(f"Город: {city}")
    if utm_campaign:
        comments_parts.append(f"UTM кампания: {utm_campaign}")
    comments = "\n".join(comments_parts)

    # source_name для уведомлений
    source_name = "Debt Quiz"

    # =================================================
    # ДУБЛИКАТ
    # =================================================
    if duplicate_lead_id:
        logging.info(
            f"[DEBT-QUIZ] Дубликат: лид #{duplicate_lead_id}, "
            f"телефон {phone}. Отвечаем 200 немедленно."
        )
        threading.Thread(
            target=_process_duplicate,
            args=(duplicate_lead_id, phone, name, department_name,
                  source_name, head_id),
            daemon=True,
            name=f"dup-dq-{duplicate_lead_id}"
        ).start()

        return jsonify({
            "status": "ok",
            "duplicate": True,
            "lead_id": duplicate_lead_id,
            "message": "Принято, дубликат"
        }), 200

    # =================================================
    # НОВЫЙ ЛИД
    # =================================================
    assignee = select_assignee(uf_crm_value, head_id or fallback_id, fallback_id)
    assigned_by_id = assignee['id']
    logging.info(
        f"[DEBT-QUIZ] Ответственный: ID={assigned_by_id} ({assignee['name']}). "
        f"{assignee['reason']}"
    )

    title = f"Debt Quiz: {name}" if name else "Debt Quiz"

    new_lead_id = create_lead(
        title=title, name=name, phone=phone, email='',
        comments=comments, uf_crm_value=uf_crm_value,
        utm_campaign=utm_campaign,
        source_id="WEB",
        source_description=department_source,
        assigned_by_id=assigned_by_id
    )

    if new_lead_id:
        lead_url = get_lead_url(new_lead_id)
        result_data = {
            "status": "ok",
            "action": "created",
            "lead_id": new_lead_id,
            "phone": phone,
            "department": department_name,
            "external_id": external_id,
        }

        db_save_assignment(new_lead_id, assigned_by_id, department_name,
                           phone, is_working_hours())

        notify_assignee_new_lead(
            lead_id=new_lead_id, user_id=assigned_by_id,
            user_name=assignee['name'], lead_name=title,
            source_name=source_name, department_name=department_name,
            phone=phone
        )

        start_lead_acceptance_monitor(
            lead_id=new_lead_id, assigned_user_id=assigned_by_id,
            assigned_user_name=assignee['name'],
            head_id=head_id or fallback_id,
            department_name=department_name,
            lead_name=title, phone=phone
        )

        send_telegram_message(
            f"<b>Новый лид (Debt Quiz)</b>\n\n"
            f"Телефон: <code>{phone}</code>\n"
            f"Имя: {name or '-'}\n"
            f"Отдел: {department_name}\n"
            f"Сумма долга: {debt_amount or '-'}\n"
            f"ФССП: {has_debt or '-'}\n"
            f"Ответственный: {assignee['name']} (ID={assigned_by_id})\n"
            f"ID квиза: {external_id or '-'}\n"
            f"ID лида: #{new_lead_id}\n"
            f"Ссылка: {lead_url}"
        )

        logging.info(f"[DEBT-QUIZ] Готово: {result_data}")
        return jsonify(result_data), 200

    else:
        error_data = {
            "status": "error",
            "action": "error",
            "message": "Не удалось создать лид",
            "phone": phone,
            "department": department_name,
        }
        logging.error(f"[DEBT-QUIZ] Ошибка создания лида: {error_data}")
        return jsonify(error_data), 500

# START
if __name__ == "__main__":
    logging.info(f"[SERVER] Старт {SERVER_HOST}:{SERVER_PORT}")
    app.run(host=SERVER_HOST, port=SERVER_PORT, debug=False)