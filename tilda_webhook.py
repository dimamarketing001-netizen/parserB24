import csv
import bisect
import requests
import json
import logging
import re
import os
from datetime import datetime, timedelta
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

# --- Списки регионов для определения отдела ---

# Свердловская область и города
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

# Челябинская область и города
CHELYABINSK_REGIONS = {
    'челябинская область', 'челябинская обл.', 'челябинская обл',
    'челябинск', 'магнитогорск', 'златоуст', 'миасс', 'копейск',
    'озёрск', 'озерск', 'троицк', 'снежинск', 'чебаркуль', 'сатка',
    'южноуральск', 'коркино', 'кыштым', 'трёхгорный', 'трехгорный',
    'еманжелинск', 'аша', 'карталы', 'верхний уфалей', 'усть-катав',
    'пласт', 'куса', 'бакал', 'катав-ивановск', 'касли', 'сим',
    'карабаш', 'нязепетровск', 'юрюзань', 'верхнеуральск', 'миньяр'
}

# Поля которые НЕ нужно включать в комментарии (телефон и UTM)
EXCLUDED_COMMENT_FIELDS = {
    'phone', 'Phone', 'PHONE',
    'utm_source', 'UTM_SOURCE', 'utm_medium', 'UTM_MEDIUM',
    'utm_campaign', 'UTM_CAMPAIGN', 'utm_content', 'UTM_CONTENT',
    'utm_term', 'UTM_TERM', 'utm_region', 'utm_region_id', 'utm_yclid',
    'formid', 'FORM_ID', 'formname', 'FORM_NAME'
}


# --- КЛАСС ДЛЯ ОПРЕДЕЛЕНИЯ РЕГИОНА ПО БАЗЕ РОССВЯЗИ ---

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
            return {
                "region": region,
                "operator": operator
            }

        return None


# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

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


def determine_department(form_region: str, utm_region: str, phone: str, rossvyaz_finder) -> tuple:
    """
    Определяет отдел продаж по приоритету:
    1. Поле "Укажите регион проживания"
    2. UTM_region
    3. По номеру телефона (Россвязь)
    4. По умолчанию - Челябинск (60)

    Возвращает (uf_crm_value, source_description)
    """

    # 1. Проверяем поле "Укажите регион проживания"
    if form_region:
        form_region_lower = form_region.lower().strip()

        if 'челябинск' in form_region_lower or form_region_lower in CHELYABINSK_REGIONS:
            logging.info(f"[DEPARTMENT] Определено по форме '{form_region}' -> Челябинск (60)")
            return UF_CRM_VALUE_CHELYABINSK, f"По региону из формы: {form_region}"

        if 'свердловск' in form_region_lower or 'екатеринбург' in form_region_lower or form_region_lower in SVERDLOVSK_REGIONS:
            logging.info(f"[DEPARTMENT] Определено по форме '{form_region}' -> Екатеринбург (58)")
            return UF_CRM_VALUE_EKATERINBURG, f"По региону из формы: {form_region}"

    # 2. Проверяем utm_region
    if utm_region:
        utm_region_lower = utm_region.lower().strip()

        if utm_region_lower in SVERDLOVSK_REGIONS:
            logging.info(f"[DEPARTMENT] Определено по utm_region '{utm_region}' -> Екатеринбург (58)")
            return UF_CRM_VALUE_EKATERINBURG, f"По UTM региону: {utm_region}"

        if utm_region_lower in CHELYABINSK_REGIONS:
            logging.info(f"[DEPARTMENT] Определено по utm_region '{utm_region}' -> Челябинск (60)")
            return UF_CRM_VALUE_CHELYABINSK, f"По UTM региону: {utm_region}"

    # 3. Пробуем определить по номеру телефона
    if rossvyaz_finder and phone:
        region_info = rossvyaz_finder.find(phone)
        if region_info and region_info.get('region'):
            phone_region = region_info['region'].lower().strip()

            if 'свердловск' in phone_region or 'екатеринбург' in phone_region:
                logging.info(f"[DEPARTMENT] Определено по телефону '{region_info['region']}' -> Екатеринбург (58)")
                return UF_CRM_VALUE_EKATERINBURG, f"По региону телефона: {region_info['region']}"

            if 'челябинск' in phone_region:
                logging.info(f"[DEPARTMENT] Определено по телефону '{region_info['region']}' -> Челябинск (60)")
                return UF_CRM_VALUE_CHELYABINSK, f"По региону телефона: {region_info['region']}"

    # 4. По умолчанию - Челябинск
    logging.info("[DEPARTMENT] Регион не определён -> Челябинск (60) по умолчанию")
    return UF_CRM_VALUE_CHELYABINSK, "Регион не определён (по умолчанию)"


def build_comments(data: dict) -> str:
    """
    Собирает все поля формы (кроме телефона и UTM) в текст для COMMENTS.
    """
    comments_lines = []

    for key, value in data.items():
        if key in EXCLUDED_COMMENT_FIELDS:
            continue
        if value and str(value).strip():
            comments_lines.append(f"{key}: {value}")

    return "\n".join(comments_lines) if comments_lines else ""


def send_telegram_message(message: str):
    """Отправляет сообщение в телеграм группу."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("[TELEGRAM] Токен или chat_id не настроены.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code >= 400:
            logging.warning(f"[TELEGRAM] Ответ API: {response.text}")
        response.raise_for_status()
        logging.info("[TELEGRAM] Сообщение успешно отправлено.")
    except requests.exceptions.RequestException as e:
        logging.error(f"[TELEGRAM] Ошибка отправки: {e}")


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
    """Создает новый лид в Bitrix24."""
    url = webhook + "crm.lead.add"

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

    # UTM метки
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

    try:
        response = requests.post(url, data=json.dumps(data), headers=headers, timeout=30)
        response.raise_for_status()
        result = response.json()
        if 'result' in result:
            logging.info(f"[CREATE] Лид создан. ID: {result['result']}. {UF_CRM_FIELD}={uf_crm_value}")
            return result['result']
        elif 'error' in result:
            logging.error(f"[CREATE] Ошибка Bitrix24: {result['error']}: {result.get('error_description', '')}")
            return None
        else:
            logging.error("[CREATE] Неизвестная ошибка при создании лида.")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"[CREATE] Ошибка запроса: {e}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"[CREATE] Ошибка JSON: {e}")
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


# --- FLASK ПРИЛОЖЕНИЕ ---

app = Flask(__name__)

# Загружаем базу Россвязи при старте
ranges_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ranges.csv')
if os.path.exists(ranges_file):
    rossvyaz_finder = RossvyazMobile(ranges_file)
else:
    logging.warning(f"[INIT] Файл ranges.csv не найден: {ranges_file}. Определение по телефону будет недоступно.")
    rossvyaz_finder = None


@app.route('/health', methods=['GET'])
def health_check():
    """Проверка работоспособности сервера."""
    return jsonify({"status": "ok", "message": "Tilda webhook server is running"}), 200


@app.route('/webhook/tilda', methods=['POST'])
def tilda_webhook():
    """
    Принимает заявки от Tilda и создает/обрабатывает лиды в Bitrix24.
    """
    logging.info("=" * 60)
    logging.info("[WEBHOOK] Получен запрос от Tilda")

    # Получаем данные из запроса
    if request.is_json:
        data = request.get_json()
    else:
        data = request.form.to_dict()

    logging.info(f"[WEBHOOK] Данные запроса: {data}")

    # --- Извлекаем поля ---

    # Телефон
    raw_phone = data.get('Phone') or data.get('phone') or data.get('PHONE') or ''
    phone = normalize_phone(raw_phone)

    if not phone:
        logging.warning(f"[WEBHOOK] Некорректный номер телефона: {raw_phone}")
        return jsonify({
            "status": "error",
            "message": "Некорректный номер телефона"
        }), 400

    logging.info(f"[WEBHOOK] Нормализованный телефон: {phone}")

    # Имя
    name = data.get('Name') or data.get('name') or data.get('NAME') or ''

    # Email
    email = data.get('Email') or data.get('email') or data.get('EMAIL') or ''

    # UTM метки
    utm_source = data.get('utm_source') or data.get('UTM_SOURCE') or ''
    utm_medium = data.get('utm_medium') or data.get('UTM_MEDIUM') or ''
    utm_campaign = data.get('utm_campaign') or data.get('UTM_CAMPAIGN') or ''
    utm_content = data.get('utm_content') or data.get('UTM_CONTENT') or ''
    utm_term = data.get('utm_term') or data.get('UTM_TERM') or ''
    utm_region = data.get('utm_region') or ''

    # Регион из формы (поле "Укажите регион проживания")
    form_region = (
            data.get('Укажите регион проживания') or
            data.get('Укажите регион проживания:') or
            data.get('region') or
            ''
    )

    # --- Определяем отдел продаж ---
    uf_crm_value, department_source = determine_department(
        form_region=form_region,
        utm_region=utm_region,
        phone=phone,
        rossvyaz_finder=rossvyaz_finder
    )

    department_name = "Екатеринбург" if uf_crm_value == 58 else "Челябинск"
    logging.info(f"[WEBHOOK] Отдел: {department_name} ({uf_crm_value}). Источник: {department_source}")

    # --- Собираем комментарии из всех полей формы ---
    comments = build_comments(data)
    logging.info(
        f"[WEBHOOK] Комментарии: {comments[:200]}..." if len(comments) > 200 else f"[WEBHOOK] Комментарии: {comments}")

    # --- Проверяем дубликат ---
    duplicate_lead_id = get_duplicate_lead_id(phone)

    result_data = {
        "phone": phone,
        "department": department_name,
        "uf_crm_value": uf_crm_value,
        "department_source": department_source
    }

    if duplicate_lead_id:
        # Дубликат найден
        logging.info(f"[WEBHOOK] Обнаружен дубликат. ID лида: {duplicate_lead_id}")
        result_data["action"] = "duplicate"
        result_data["lead_id"] = duplicate_lead_id

        details = get_lead_details(duplicate_lead_id)

        if details and details.get("ASSIGNED_BY_ID"):
            # Создаем задачу
            task_id = create_b24_task(duplicate_lead_id, details["ASSIGNED_BY_ID"])
            result_data["task_id"] = task_id

            # Выводим из спама если нужно
            if details["STATUS_ID"] in SPAM_STATUS_IDS:
                if update_lead_status(duplicate_lead_id, "NEW"):
                    result_data["rescued_from_spam"] = True
                    logging.info(f"[WEBHOOK] Лид {duplicate_lead_id} выведен из спама.")

            # Отправляем уведомление в телеграм
            tg_message = (
                f"*Повторная заявка (Рекламный лид)*\n\n"
                f"Телефон: `{phone}`\n"
                f"Имя: {name if name else '-'}\n"
                f"Отдел: {department_name}\n"
                f"Источник: {department_source}\n"
                f"Существующий лид: #{duplicate_lead_id}\n"
                f"Создана задача: #{task_id if task_id else 'Ошибка'}"
            )
            send_telegram_message(tg_message)
        else:
            logging.warning(f"[WEBHOOK] Не удалось получить детали дубликата {duplicate_lead_id}")

    else:
        # Новый лид
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
            source_description=department_source
        )

        if new_lead_id:
            result_data["action"] = "created"
            result_data["lead_id"] = new_lead_id

            # Отправляем уведомление в телеграм
            tg_message = (
                f"*Новый Рекламный лид*\n\n"
                f"Телефон: `{phone}`\n"
                f"Имя: {name if name else '-'}\n"
                f"Email: {email if email else '-'}\n"
                f"Отдел: {department_name}\n"
                f"Источник: {department_source}\n"
                f"UTM Source: {utm_source if utm_source else '-'}\n"
                f"ID лида: #{new_lead_id}"
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