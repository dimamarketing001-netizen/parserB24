import requests
import json
import mysql.connector
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# --- ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ---
load_dotenv()


# --- Bitrix24 Configuration ---
webhook = os.getenv('BITRIX_WEBHOOK')
SPAM_STATUS_IDS = ["JUNK", "SPAM", "10", "9", "8", "7", "6", "5", "4", "3", "2", "1"]

# --- State Management Files ---
PROCESSED_PHONES_FILE = 'processed_phones.txt'

# --- Database Configuration ---
DB_CONFIG = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'database': os.getenv('DB_NAME'),
}
TABLE_NAME = 'b24_leads'

# --- Telegram Logging Configuration ---
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# --- Регионы и маппинг на UF_CRM поле ---
EKATERINBURG_REGIONS = {'Свердловская обл.', 'Екатеринбург'}
CHELYABINSK_REGIONS  = {'Челябинская обл.', 'Челябинск'}
ALL_TARGET_REGIONS   = EKATERINBURG_REGIONS | CHELYABINSK_REGIONS

UF_CRM_FIELD = 'UF_CRM_1779024295'
UF_CRM_VALUE_EKATERINBURG = 58
UF_CRM_VALUE_CHELYABINSK  = 60


def get_uf_crm_value(region: str) -> int | None:
    """
    Возвращает значение кастомного поля UF_CRM_1779024295 в зависимости от региона.
    Екатеринбург / Свердловская обл. -> 58
    Челябинск / Челябинская обл.     -> 60
    """
    if region in EKATERINBURG_REGIONS:
        return UF_CRM_VALUE_EKATERINBURG
    if region in CHELYABINSK_REGIONS:
        return UF_CRM_VALUE_CHELYABINSK
    return None

def get_sales_department_counts_from_db(leads: list) -> dict:
    """
    Считает количество лидов по Отделу продаж (UF_CRM_1779024295)
    на основе региона у всех найденных лидов из БД.

    58 = Екатеринбург
    60 = Челябинск
    """
    counts = {
        58: 0,  # Екатеринбург
        60: 0,  # Челябинск
    }

    for lead in leads:
        region = lead.get('region')
        uf_crm_value = get_uf_crm_value(region)
        if uf_crm_value in counts:
            counts[uf_crm_value] += 1

    return counts

def get_processed_phones(file_path: str) -> set:
    """
    Читает уже обработанные телефоны из файла в множество.
    """
    if not os.path.exists(file_path):
        return set()
    try:
        with open(file_path, 'r') as f:
            return {line.strip() for line in f if line.strip()}
    except IOError as e:
        print(f"Ошибка чтения файла обработанных телефонов ({e}).")
        return set()


def save_processed_phones(file_path: str, phones_to_save: set):
    """
    Дописывает успешно обработанные телефоны в файл.
    """
    try:
        with open(file_path, 'a') as f:
            for phone in phones_to_save:
                f.write(f"{phone}\n")
        print(f"Сохранено {len(phones_to_save)} новых обработанных телефонов.")
    except IOError as e:
        print(f"Не удалось сохранить телефоны обработанных лидов: {e}")


def send_telegram_message(message: str):
    """
    Отправляет сообщение в телеграм группу.
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code >= 400:
            print(f"Подробный ответ от API Telegram: {response.text}")
        response.raise_for_status()
        print("Сообщение в телеграм успешно отправлено.")
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при отправке сообщения в телеграм: {e}")

def build_telegram_report(
    leads_found_db: int,
    total_to_process: int,
    added_count: int,
    duplicates_count: int,
    tasks_created_count: int,
    spam_rescued_count: int,
    added_b24_ids: list,
    sales_department_counts: dict
) -> str:
    """
    Формирует текст отчета для Telegram
    с разбивкой по полю UF_CRM_1779024295 (Отдел продаж):
    58 = Екатеринбург
    60 = Челябинск
    """
    ekb_count = sales_department_counts.get(58, 0)
    chel_count = sales_department_counts.get(60, 0)

    log_message = (
        f"*Отчет по обработке лидов*\n\n"
        f"Найдено в БД (за 24ч): *{leads_found_db}*\n"
        f"Новых для обработки: *{total_to_process}*\n\n"
        f"*Разбивка по Отделу продаж:*\n"
        f"Екатеринбург: *{ekb_count}*\n"
        f"Челябинск: *{chel_count}*\n\n"
        f"--- Результаты ---\n"
        f"Добавлено новых лидов в Б24: *{added_count}*\n"
        f"Найдено дубликатов (в Б24): *{duplicates_count}*\n"
    )

    if duplicates_count > 0:
        log_message += (
            f"Для дубликатов создано задач: *{tasks_created_count}*\n"
            f"Лидов выведено из спама: *{spam_rescued_count}*\n"
        )

    if added_b24_ids:
        log_message += f"\nID добавленных лидов: `{', '.join(added_b24_ids)}`"

    return log_message

def get_duplicate_lead_id(phone):
    """
    Проверяет наличие дубликата лида по номеру телефона и возвращает его ID.
    """
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
            print(f"Найден дубликат для телефона {phone}. ID лида: {lead_id}")
            return lead_id
        return None
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при проверке дубликата: {e}")
        return None
    except (json.JSONDecodeError, IndexError) as e:
        print(f"Ошибка при обработке ответа при проверке дубликата: {e}")
        return None


def create_lead(
    utm_source: str,
    source_description: str,
    title: str,
    status_id: str,
    assigned_by_id: int,
    phone: str,
    source_id: str,
    uf_crm_value: int,
    opened: str = "Y"
):
    """
    Создает новый лид в Bitrix24.
    Поле UF_CRM_1779024295 заполняется в зависимости от региона лида.
    """
    url = webhook + "crm.lead.add"
    data = {
        "fields": {
            "TITLE": title,
            "STATUS_ID": status_id,
            "OPENED": opened,
            "ASSIGNED_BY_ID": assigned_by_id,
            "SOURCE_ID": source_id,
            "PHONE": [{"VALUE": phone, "VALUE_TYPE": "WORK"}],
            "SOURCE_DESCRIPTION": source_description,
            "UTM_SOURCE": utm_source,
            UF_CRM_FIELD: uf_crm_value,
        },
        "params": {"REGISTER_SONET_EVENT": "Y"}
    }
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, data=json.dumps(data), headers=headers, timeout=30)
        response.raise_for_status()
        result = response.json()
        if 'result' in result:
            print(
                f"Лид '{title}' с телефоном {phone} успешно создан. "
                f"ID: {result['result']}. {UF_CRM_FIELD}={uf_crm_value}"
            )
            return result['result']
        elif 'error' in result:
            print(f"Ошибка от Bitrix24: {result['error']}: {result['error_description']}")
            return None
        else:
            print("Ошибка при создании лида: Неизвестная ошибка")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при выполнении запроса на создание лида: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Ошибка при разборе JSON-ответа при создании лида: {e}")
        return None


def get_lead_details(lead_id):
    """
    Получает данные лида (статус и ответственного) по его ID.
    """
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
            print(f"Лид с ID {lead_id} не найден или произошла ошибка.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при получении данных лида {lead_id}: {e}")
        return None


def create_b24_task(lead_id, responsible_id):
    """
    Создает задачу в Битрикс24 'позвонить клиенту' для ответственного по лиду.
    """
    url = webhook + "tasks.task.add"
    task_description = (
        f"Позвони, клиент оставил повторную заявку. "
        f"Лид: [URL=/crm/lead/details/{lead_id}/]#{lead_id}[/URL]"
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
            print(f"Задача {task_id} для лида {lead_id} успешно создана для пользователя {responsible_id}.")
            return task_id
        else:
            error_info = result.get('error_description') or result
            print(f"Не удалось создать задачу для лида {lead_id}. Ошибка: {error_info}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при создании задачи для лида {lead_id}: {e}")
        return None


def update_lead_status(lead_id, status_id="NEW"):
    """
    Обновляет статус лида в Битрикс24.
    """
    url = webhook + "crm.lead.update"
    data = {
        "id": lead_id,
        "fields": {"STATUS_ID": status_id}
    }
    try:
        response = requests.post(url, json=data, timeout=30)
        response.raise_for_status()
        if response.json().get('result'):
            print(f"Статус лида {lead_id} успешно обновлен на '{status_id}'.")
            return True
        return False
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при обновлении статуса лида {lead_id}: {e}")
        return False


if __name__ == "__main__":
    # 1. Загружаем телефоны уже обработанных лидов
    processed_phones = get_processed_phones(PROCESSED_PHONES_FILE)
    print(f"Загружено {len(processed_phones)} номеров телефонов, ранее обработанных.")

    # 2. Получаем лидов из БД за последние 24 часа по всем целевым регионам
    leads_from_db = []
    sales_department_counts = {58: 0, 60: 0}  # дефолт на случай ошибки БД
    start_date = datetime.now() - timedelta(days=1)

    # Формируем плейсхолдеры для IN-запроса динамически
    region_placeholders = ', '.join(['%s'] * len(ALL_TARGET_REGIONS))

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        query = f"""
            SELECT name, last_name, phone, email, region
            FROM {TABLE_NAME}
            WHERE region IN ({region_placeholders})
              AND date_create >= %s
        """
        params = tuple(ALL_TARGET_REGIONS) + (start_date.strftime('%Y-%m-%d %H:%M:%S'),)
        cursor.execute(query, params)
        leads_from_db = cursor.fetchall()
        print(f"Найдено в БД {len(leads_from_db)} лидов за последние 24 часа.")

        sales_department_counts = get_sales_department_counts_from_db(leads_from_db)

        print(
            f"Разбивка по Отделу продаж: "
            f"Екатеринбург (58) = {sales_department_counts[58]}, "
            f"Челябинск (60) = {sales_department_counts[60]}"
        )
    except mysql.connector.Error as err:
        print(f"Ошибка базы данных: {err}")
        exit()
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            print("Соединение с БД закрыто.")

    # 3. Фильтруем лиды, оставляя только те, чьи телефоны еще не были обработаны
    leads_to_process = [
        lead for lead in leads_from_db
        if lead.get('phone') and lead.get('phone') not in processed_phones
    ]

    total_to_process = len(leads_to_process)
    if total_to_process > 0:
        print(f"Из них {total_to_process} новых для обработки.")

        duplicates_count      = 0
        added_count           = 0
        tasks_created_count   = 0
        spam_rescued_count    = 0
        added_b24_ids         = []
        newly_processed_phones = set()

        for lead in leads_to_process:
            phone_number = lead.get('phone')
            if not phone_number:
                continue

            region       = lead.get('region', '')
            uf_crm_value = get_uf_crm_value(region)

            if uf_crm_value is None:
                # Регион не попал ни в одну из целевых групп — пропускаем
                print(f"Регион '{region}' не определён в маппинге. Пропускаем телефон {phone_number}.")
                continue

            duplicate_b24_id = get_duplicate_lead_id(phone_number)

            if not duplicate_b24_id:
                # Дубликата нет — создаём лид с нужным UF_CRM значением
                new_b24_lead_id = create_lead(
                    title="LeadGen2",
                    status_id="NEW",
                    assigned_by_id=1,
                    phone=phone_number,
                    opened="Y",
                    source_id="16",
                    source_description="https://какнеплатитькредит.рф/bfl",
                    utm_source="yandex",
                    uf_crm_value=uf_crm_value,
                )
                if new_b24_lead_id:
                    added_count += 1
                    added_b24_ids.append(str(new_b24_lead_id))
                    newly_processed_phones.add(phone_number)
            else:
                # Дубликат найден
                duplicates_count += 1
                details = get_lead_details(duplicate_b24_id)
                if details and details.get("ASSIGNED_BY_ID"):
                    if create_b24_task(duplicate_b24_id, details["ASSIGNED_BY_ID"]):
                        tasks_created_count += 1
                        newly_processed_phones.add(phone_number)

                    if details["STATUS_ID"] in SPAM_STATUS_IDS:
                        if update_lead_status(duplicate_b24_id, "NEW"):
                            spam_rescued_count += 1
                else:
                    print(f"Не удалось получить детали для лида-дубликата {duplicate_b24_id}.")

        # Формирование и отправка отчета в телеграм
        log_message = build_telegram_report(
            leads_found_db=len(leads_from_db),
            total_to_process=total_to_process,
            added_count=added_count,
            duplicates_count=duplicates_count,
            tasks_created_count=tasks_created_count,
            spam_rescued_count=spam_rescued_count,
            added_b24_ids=added_b24_ids,
            sales_department_counts=sales_department_counts
        )

        if added_count > 0 or tasks_created_count > 0:
            send_telegram_message(log_message)
        else:
            print("Не было успешных операций для отправки отчета.")

        # 4. Сохраняем телефоны только что успешно обработанных лидов
        if newly_processed_phones:
            save_processed_phones(PROCESSED_PHONES_FILE, newly_processed_phones)

    else:
        print("Не найдено новых лидов для обработки (либо все найденные уже были обработаны ранее).")