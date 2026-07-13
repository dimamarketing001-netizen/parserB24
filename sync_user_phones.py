"""
sync_user_phones.py

Синхронизирует телефоны сотрудников из АТС в MySQL.
Запускать по cron раз в час:
    0 * * * * /usr/bin/python3 /путь/к/sync_user_phones.py >> /var/log/sync_phones.log 2>&1
"""

import requests
import re
import os
import logging
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# НАСТРОЙКИ ЛОГИРОВАНИЯ
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync_user_phones.log'),
        logging.StreamHandler()
    ]
)

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================

ATS_BASE_URL = os.getenv('ATS_BASE_URL', 'https://exolve206555.vats.exolve.ru/crmapi/v1')
ATS_API_KEY  = os.getenv('ATS_API_KEY', '')
BITRIX_WEBHOOK = os.getenv('BITRIX_WEBHOOK', '')

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', '127.0.0.1'),
    'port':     int(os.getenv('DB_PORT', 3306)),
    'user':     os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
}


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def format_phone(raw: str):
    """Приводит номер к формату +7XXXXXXXXXX"""
    if not raw:
        return None
    digits = re.sub(r'\D', '', str(raw))
    if len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    if len(digits) == 11 and digits.startswith('7'):
        return '+' + digits
    if len(digits) == 10 and digits.startswith('9'):
        return '+7' + digits
    return None


def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        logging.info("[DB] Подключение успешно.")
        return conn
    except mysql.connector.Error as e:
        logging.error(f"[DB] Ошибка подключения: {e}")
        return None


def ensure_table_exists(conn):
    """Создаёт таблицу если её нет."""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_phones (
                b24_user_id       INT PRIMARY KEY,
                b24_name          VARCHAR(255),
                ats_login         VARCHAR(100),
                ats_ext           VARCHAR(20),
                responsible_phone VARCHAR(20),
                updated_at        DATETIME NOT NULL,
                INDEX (ats_ext),
                INDEX (ats_login)
            )
        """)
        conn.commit()
        cursor.close()
        logging.info("[DB] Таблица user_phones готова.")
    except mysql.connector.Error as e:
        logging.error(f"[DB] Ошибка создания таблицы: {e}")


# ============================================================
# ЗАПРОСЫ К АТС
# ============================================================

def fetch_caller_ids():
    """
    GET /caller-ids
    Возвращает: {login: telnum, ...}
    """
    try:
        response = requests.get(
            f"{ATS_BASE_URL}/caller-ids",
            headers={"X-API-KEY": ATS_API_KEY},
            timeout=15
        )
        response.raise_for_status()
        data = response.json()

        users = data.get('users', [])
        if isinstance(users, dict):
            users = list(users.values())

        result = {
            u['login']: u['telnum']
            for u in users
            if u.get('login') and u.get('telnum')
        }
        logging.info(f"[ATS] caller-ids: получено {len(result)} персональных номеров.")
        return result

    except requests.exceptions.RequestException as e:
        logging.error(f"[ATS] Ошибка GET /caller-ids: {e}")
        return {}


def fetch_ats_users():
    """
    GET /users (с пагинацией)
    Возвращает: {ext: login, ...}
    """
    ext_to_login = {}
    start = 0
    limit = 100

    while True:
        try:
            response = requests.get(
                f"{ATS_BASE_URL}/users",
                headers={"X-API-KEY": ATS_API_KEY},
                params={"limit": limit, "start": start},
                timeout=15
            )
            response.raise_for_status()
            data = response.json()
            items = data.get('items', [])

            for u in items:
                ext   = str(u.get('ext', '')).strip()
                login = u.get('login', '').strip()
                if ext and login:
                    ext_to_login[ext] = login

            info    = data.get('info', {})
            total   = info.get('total', 0)
            fetched = start + len(items)
            logging.info(f"[ATS] /users: start={start}, получено={len(items)}, всего={total}")

            if fetched >= total or not items:
                break
            start += limit

        except requests.exceptions.RequestException as e:
            logging.error(f"[ATS] Ошибка GET /users: {e}")
            break

    logging.info(f"[ATS] /users итого: {len(ext_to_login)} сотрудников с ext.")
    return ext_to_login


# ============================================================
# ЗАПРОСЫ К Б24
# ============================================================

def fetch_b24_users():
    """
    Получает ВСЕХ активных сотрудников из Б24 с UF_PHONE_INNER.
    Возвращает список: [{'id': int, 'name': str, 'ext': str}, ...]
    """
    users = []
    start = 0

    while True:
        try:
            response = requests.get(
                BITRIX_WEBHOOK + "user.get",
                params={
                    "ACTIVE": True,
                    "start": start,
                    "select[]": [
                        "ID", "NAME", "LAST_NAME",
                        "UF_PHONE_INNER", "PERSONAL_MOBILE"
                    ]
                },
                timeout=15
            )
            response.raise_for_status()
            data   = response.json()
            result = data.get('result', [])
            total  = data.get('total', 0)

            for u in result:
                ext = str(u.get('UF_PHONE_INNER') or '').strip()
                users.append({
                    'id':   int(u['ID']),
                    'name': f"{u.get('NAME', '')} {u.get('LAST_NAME', '')}".strip(),
                    'ext':  ext,
                })

            fetched = start + len(result)
            logging.info(
                f"[B24] user.get: start={start}, "
                f"получено={len(result)}, всего={total}"
            )

            if fetched >= total or not result:
                break
            start += 50

        except requests.exceptions.RequestException as e:
            logging.error(f"[B24] Ошибка user.get: {e}")
            break

    logging.info(f"[B24] Итого сотрудников: {len(users)}")
    return users


# ============================================================
# СОХРАНЕНИЕ В БД
# ============================================================

def upsert_user_phone(conn, b24_user_id: int, b24_name: str,
                       ats_login: str, ats_ext: str,
                       responsible_phone: str):
    """
    INSERT ... ON DUPLICATE KEY UPDATE — создаёт или обновляет запись.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO user_phones
                (b24_user_id, b24_name, ats_login, ats_ext, responsible_phone, updated_at)
            VALUES
                (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                b24_name          = VALUES(b24_name),
                ats_login         = VALUES(ats_login),
                ats_ext           = VALUES(ats_ext),
                responsible_phone = VALUES(responsible_phone),
                updated_at        = VALUES(updated_at)
        """, (
            b24_user_id,
            b24_name,
            ats_login or None,
            ats_ext or None,
            responsible_phone,
            datetime.now()
        ))
        conn.commit()
        cursor.close()
    except mysql.connector.Error as e:
        logging.error(f"[DB] Ошибка upsert user_id={b24_user_id}: {e}")


# ============================================================
# ОСНОВНАЯ ЛОГИКА СИНХРОНИЗАЦИИ
# ============================================================

def sync():
    logging.info("=" * 60)
    logging.info("СТАРТ синхронизации телефонов сотрудников")
    logging.info("=" * 60)

    # --- 1. Получаем данные из АТС ---
    login_to_telnum = fetch_caller_ids()   # {login: telnum}
    ext_to_login    = fetch_ats_users()    # {ext: login}

    if not login_to_telnum:
        logging.error("caller-ids пустой — прерываем синхронизацию.")
        return

    if not ext_to_login:
        logging.error("/users пустой — прерываем синхронизацию.")
        return

    # --- 2. Получаем сотрудников из Б24 ---
    b24_users = fetch_b24_users()

    if not b24_users:
        logging.error("Б24 вернул пустой список — прерываем.")
        return

    # --- 3. Подключаемся к БД ---
    conn = get_db_connection()
    if not conn:
        return

    ensure_table_exists(conn)

    # --- 4. Для каждого сотрудника Б24 ищем номер ---
    stats = {'found': 0, 'not_found': 0, 'no_ext': 0}

    for user in b24_users:
        b24_id   = user['id']
        b24_name = user['name']
        ext      = user['ext']

        if not ext:
            # У сотрудника нет внутреннего номера в Б24
            logging.warning(
                f"[SYNC] {b24_name} (ID={b24_id}): "
                f"UF_PHONE_INNER пустой — пропускаем."
            )
            upsert_user_phone(conn, b24_id, b24_name, None, None, None)
            stats['no_ext'] += 1
            continue

        # ext → login (из АТС /users)
        ats_login = ext_to_login.get(ext)
        if not ats_login:
            logging.warning(
                f"[SYNC] {b24_name} (ID={b24_id}): "
                f"ext='{ext}' не найден в АТС /users."
            )
            upsert_user_phone(conn, b24_id, b24_name, None, ext, None)
            stats['not_found'] += 1
            continue

        # login → telnum (из АТС /caller-ids)
        telnum = login_to_telnum.get(ats_login)
        responsible_phone = format_phone(telnum) if telnum else None

        upsert_user_phone(
            conn,
            b24_user_id=b24_id,
            b24_name=b24_name,
            ats_login=ats_login,
            ats_ext=ext,
            responsible_phone=responsible_phone
        )

        if responsible_phone:
            logging.info(
                f"[SYNC] ✅ {b24_name} (ID={b24_id}): "
                f"ext={ext} → {ats_login} → {responsible_phone}"
            )
            stats['found'] += 1
        else:
            logging.info(
                f"[SYNC] ⚠️  {b24_name} (ID={b24_id}): "
                f"ext={ext} → {ats_login} → нет персонального номера в caller-ids"
            )
            stats['not_found'] += 1

    conn.close()

    # --- 5. Итог ---
    logging.info("=" * 60)
    logging.info(
        f"ИТОГ: найдено номеров={stats['found']}, "
        f"не найдено={stats['not_found']}, "
        f"нет ext={stats['no_ext']}, "
        f"всего={len(b24_users)}"
    )
    logging.info("=" * 60)


# ============================================================
# ТОЧКА ВХОДА
# ============================================================

if __name__ == "__main__":
    sync()