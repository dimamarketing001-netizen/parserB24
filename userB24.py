import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

webhook = os.getenv('BITRIX_WEBHOOK')


def pretty_print(data, title=""):
    if title:
        print(f"\n{'=' * 60}")
        print(f"  {title}")
        print('=' * 60)
    print(json.dumps(data, ensure_ascii=False, indent=2))


# ============================================================
# 1. СПИСОК ВСЕХ ПОЛЬЗОВАТЕЛЕЙ
# ============================================================
def get_all_users():
    url = webhook + "user.get"
    params = {
        "ACTIVE": True,
        "select[]": ["ID", "NAME", "LAST_NAME", "EMAIL", "WORK_POSITION", "UF_DEPARTMENT", "IS_ONLINE"]
    }
    response = requests.get(url, params=params, timeout=30)
    result = response.json().get('result', [])
    pretty_print(result, "ВСЕ ПОЛЬЗОВАТЕЛИ")
    return result


# ============================================================
# 2. СПИСОК ВСЕХ ОТДЕЛОВ (СТРУКТУРА КОМПАНИИ)
# ============================================================
def get_all_departments():
    url = webhook + "department.get"
    response = requests.get(url, timeout=30)
    result = response.json().get('result', [])
    pretty_print(result, "ВСЕ ОТДЕЛЫ")
    return result


# ============================================================
# 3. ДЕТАЛЬНО: ПОЛЬЗОВАТЕЛИ С ПОЛЯМИ ОБ ОТДЕЛЕ
# ============================================================
def get_users_with_departments():
    url = webhook + "user.get"
    params = {
        "ACTIVE": True,
    }
    response = requests.get(url, params=params, timeout=30)
    result = response.json().get('result', [])

    print(f"\n{'=' * 60}")
    print("  ПОЛЬЗОВАТЕЛИ + ИХ ОТДЕЛЫ")
    print('=' * 60)

    for user in result:
        print(f"\nID: {user.get('ID')}")
        print(f"  Имя: {user.get('NAME')} {user.get('LAST_NAME')}")
        print(f"  Email: {user.get('EMAIL')}")
        print(f"  Должность: {user.get('WORK_POSITION')}")
        print(f"  UF_DEPARTMENT (ID отделов): {user.get('UF_DEPARTMENT')}")
        print(f"  Все ключи: {list(user.keys())}")

    return result


# ============================================================
# 4. ПРОВЕРЯЕМ КОНКРЕТНЫХ ПОЛЬЗОВАТЕЛЕЙ (замени ID на нужные)
# ============================================================
def get_user_by_id(user_id):
    url = webhook + "user.get"
    params = {"ID": user_id}
    response = requests.get(url, params=params, timeout=30)
    result = response.json().get('result', [])
    pretty_print(result, f"ПОЛЬЗОВАТЕЛЬ ID={user_id}")
    return result


# ============================================================
# 5. ПРОВЕРЯЕМ ЕСТЬ ЛИ МЕТОД ДЛЯ ОТПРАВКИ УВЕДОМЛЕНИЙ
# ============================================================
def check_notification_methods():
    # Метод 1: im.notify.system.add - системное уведомление
    print(f"\n{'=' * 60}")
    print("  ДОСТУПНЫЕ МЕТОДЫ УВЕДОМЛЕНИЙ")
    print('=' * 60)

    url = webhook + "methods"
    response = requests.get(url, timeout=30)
    result = response.json().get('result', [])

    # Фильтруем методы связанные с уведомлениями и сообщениями
    notification_methods = [m for m in result if any(
        keyword in m.lower()
        for keyword in ['im.', 'notify', 'message', 'send']
    )]

    pretty_print(notification_methods, "МЕТОДЫ IM/NOTIFY/MESSAGE")
    return notification_methods


# ============================================================
# 6. ТЕСТ ОТПРАВКИ ЛИЧНОГО СООБЩЕНИЯ (замени USER_ID на нужный)
# ============================================================
def test_send_message(to_user_id, test_message="Тест уведомления от интеграции"):
    # Способ 1: im.message.add
    url = webhook + "im.message.add"
    data = {
        "DIALOG_ID": to_user_id,  # ID пользователя для личного сообщения
        "MESSAGE": test_message
    }
    response = requests.post(url, json=data, timeout=30)
    pretty_print(response.json(), f"im.message.add -> USER {to_user_id}")

    # Способ 2: im.notify.system.add
    url2 = webhook + "im.notify.system.add"
    data2 = {
        "USER_ID": to_user_id,
        "MESSAGE": test_message
    }
    response2 = requests.post(url2, json=data2, timeout=30)
    pretty_print(response2.json(), f"im.notify.system.add -> USER {to_user_id}")


# ============================================================
# 7. ПОЛУЧАЕМ РУКОВОДИТЕЛЕЙ ОТДЕЛОВ
# ============================================================
def get_department_heads():
    url = webhook + "department.get"
    response = requests.get(url, timeout=30)
    departments = response.json().get('result', [])

    print(f"\n{'=' * 60}")
    print("  ОТДЕЛЫ И РУКОВОДИТЕЛИ")
    print('=' * 60)

    for dept in departments:
        print(f"\nОтдел ID: {dept.get('ID')}")
        print(f"  Название: {dept.get('NAME')}")
        print(f"  Руководитель (UF_HEAD): {dept.get('UF_HEAD')}")
        print(f"  Родительский отдел: {dept.get('PARENT')}")
        print(f"  Все ключи: {list(dept.keys())}")
        print(f"  Полные данные: {dept}")

    return departments



# ============================================================
# ЗАПУСКАЕМ ВСЁ
# ============================================================
if __name__ == "__main__":
    print("🔍 Исследуем структуру Bitrix24...\n")

    print("\n📋 Шаг 1: Все отделы...")
    departments = get_all_departments()

    print("\n👥 Шаг 2: Пользователи с отделами...")
    users = get_users_with_departments()

    print("\n🔔 Шаг 3: Доступные методы уведомлений...")
    check_notification_methods()

    print("\n👑 Шаг 4: Руководители отделов...")
    get_department_heads()

    # Если хочешь проверить конкретного пользователя — раскомментируй:
    # get_user_by_id(64)  # ASSIGNED_CHELYABINSK
    # get_user_by_id(1)   # ASSIGNED_DEFAULT

    # Если хочешь протестировать отправку сообщения — раскомментируй:
    # test_send_message(64, "Тест: повторная заявка пришла")

    print("\n✅ Исследование завершено!")