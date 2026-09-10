################################################################################################
#   Retail Purchase Analytics Pipeline
#=============================================
#
#Данный скрипт представляет собой инструмент для автоматического сбора,
#очистки и категоризации истории заказов из личного кабинета x5Club.
#
#ОСНОВНОЙ ФУНКЦИОНАЛ:
#1. Автоматизация авторизации: Сохранение сессии в state.json для обхода повторного ввода SMS.
#2. Парсинг данных: Сбор истории заказов с использованием Playwright (включая работу с календарем).
#3. ETL-процесс (Extract, Transform, Load):
#   - Удаление дубликатов на основе уникальных ключей заказа.
#   - Нормализация названий товаров (приведение вариаций к единой сущности).
#   - Категоризация товаров по заданным словарям (Уровень 1: Сущность -> Уровень 2: Категория).
#   - Сортировка данных по времени и позиции в чеке.
#4. Подготовка данных: Экспорт в CSV для последующей визуализации в BI-системах (например, DataLens).
#
#РЕЖИМЫ РАБОТЫ (CLI):
#- python script.py login                    -> Авторизация и сохранение сессии.
#- python script.py parse [start] [end]       -> Сбор данных за период (гггг-мм-дд).
#- python script.py process                  -> Обработка, очистка и категоризация данных.
#
#ФАЙЛЫ:
#- config.json       : Настройки доступа (URL, телефон).
#- state.json        : Данные сессии браузера.
#- orders.csv        : Сырые данные из личного кабинета.
#- orders_sorted.csv : Очищенные и категоризированные данные для BI.
#
#===========================================
#   Developed with AI assistance
#   Навайбокодено delffine с помощью gemma4:31b
################################################################################################

import asyncio
import json
import os
import sys
import csv
import re
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

# --- КОНФИГУРАЦИЯ ---
STATE_FILE = "state.json"
CSV_FILE = "orders.csv"
SORTED_CSV_FILE = "orders_sorted.csv"

# --- ТАБЛИЦЫ КАТЕГОРИЗАЦИИ ---

# Уровень 1: Приведение разных названий к одному "сущностному" названию
# Мы используем поиск по ключевому слову. Если слово найдено в названии -> присваиваем единое имя.
PRODUCT_MAPPING = {
    # Овощи и фрукты
    "бананы": "Бананы",
    "яблоко": "Яблоки",
    "яблоки": "Яблоки",    
    "апельсин": "Апельсины",
    "мандарин": "Мандарины",
    "лимон": "Лимоны",
    "виноград": "Виноград",
    "дыня": "Дыня",
    "персик": "Персики",
    "нектарин": "Нектарины",
    "манго": "Манго",
    "лук": "Лук",
    "чеснок": "Чеснок",
    "картофель": "Картофель",
    "капуста": "Капуста",
    "перец": "Перец",
    "огурец": "Огурцы",
    "морковь": "Морковь",
    "шампиньоны": "Грибы",
    "курага": "Сухофрукты",
    "изюм": "Сухофрукты",
    "чернослив": "Сухофрукты",
    "смесь": "Смесь овощная",
    "горошек": "Горошек",
    "кукуруза": "Кукуруза",
    "огурцы": "Огурцы",
    "абрикос": "Абрикосы",
    "имбирь": "Имбирь",
    "клубник": "Клубника",
    
    #Орехи
    "фундук": "Орехи",   
    "орех": "Орехи",  
    "арахис": "Орехи",    

    # Мясо и Птица
    "индейка": "Курица/Индейка",
    "индейк": "Курица/Индейка", 
    "индеек": "Курица/Индейка",    
    "цыпленка": "Курица/Индейка",
    "цыплёнк": "Курица/Индейка",
    "цыплят": "Курица/Индейка",    
    "куриное": "Курица/Индейка",
    "филе грудки": "Курица/Индейка",
    
    "свинина": "Мясные изделия",
    "гуляш": "Мясные изделия",
    "азу": "Мясные изделия",
    "купаты": "Мясные изделия",
    "колбаса": "Мясные изделия",
    "сервелат": "Мясные изделия",
    "ветчина": "Мясные изделия",
    "сосиски": "Мясные изделия",
    "сардельки": "Мясные изделия",
    "салями": "Мясные изделия",
    "окорок": "Мясные изделия",
    "слайсы": "Мясные изделия",
    "пельмени": "Пельмени",
    
    # Рыба и Морепродукты
    "креветки": "Морепродукты",
    "крабовые": "Морепродукты",
    "килька": "Рыба консервированная",
    "горбуша": "Рыба",
    "скумбрия": "Рыба",
    "сельдь": "Рыба",
    "рыбное": "Рыба",
    "сардина": "Рыба консервированная",
    "сайра": "Рыба консервированная",
    "тунец": "Рыба консервированная",
    "рыбное ассорти": "Рыба",
    "краб": "Крабовое мясо",

    # Молочка и Яйца
    "йогурт": "Йогурты",
    "творог": "Творог",
    "творожок": "Творог",
    "сыр": "Сыр",
    "сметана": "Сметана",
    "сливки": "Сливки",
    "масло сливочное": "Сливочное масло",
    "спред": "Сливочное масло",    
    "яйцо": "Яйца",
    "яйца": "Яйца",    
    "молоко": "Молоко",

    # Бакалея и специи
    "макароны": "Макаронные изделия",
    "спагетти": "Макаронные изделия",
    "рожки": "Макаронные изделия",
    "рис": "Крупы",
    "гречка": "Крупы",
    "перловка": "Крупы",
    "сахар": "Сахар",
    "соль": "Соль",
    "масло подсолнечное": "Растительное масло",
    "подсолнечное": "Растительное масло",
    "уксус": "Уксус",
    "кетчуп": "Соусы",
    "соус": "Соусы",
    "приправа": "Специи",
    "лавровый": "Специи",
    "паприка": "Специи",
    "лимонная кислота": "Специи",

    # Сладости и Выпечка
    "конфеты": "Конфеты",
    "зефир": "Зефир",
    "вафли": "Вафли",
    "шоколад": "Шоколад",
    "печенье": "Печенье",
    "пастила": "Пастила",
    "торт": "Торты",
    "рулет": "Торты",
    "десерт": "Сладости",
    "хлеб": "Хлеб",
    "батон": "Хлеб",
    "лаваш": "Хлеб",
    "пломбир": "Мороженое",    
    "морожен": "Мороженое",    
    "джем": "Джем",
    "топпинг": "Джем", 

    # Напитки
    "кофе": "Кофе",
    "чай": "Чай",
    "вода": "Вода",
    "кола": "Газировка",
    "напиток": "Газировка",
    "коктейль": "Газировка",    
    "вино": "Алкоголь",
    "пиво": "Алкоголь",

    # Хозтовары и Гигиена
    "крем для лица": "Косметика",
    "крем для рук": "Косметика",
    "крем": "Косметика",     
    "зубная паста": "Гигиена",
    "шампунь": "Гигиена",
    "салфет": "Гигиена",
    "cалфет": "Гигиена",    
    "бумага": "Гигиена",
    "доместос": "Бытовая химия",
    "средство для мытья": "Бытовая химия",
    "средство для стирки": "Бытовая химия",
    "чистящее": "Бытовая химия",
    "стирки": "Бытовая химия",
    "порошок": "Бытовая химия",    
    "пакет": "Пакет",
    "клей": "Хозтовары",
    "губки": "Хозтовары",
    "посуд": "Хозтовары",    

    # Прочее
    "корм": "Разное",
    "игрушка": "Разное",
    "смешарик": "Разное",
    "журнал": "Журналы",    

}

# Уровень 2: Привязка единого названия к категории
CATEGORY_MAPPING = {
    # Овощи и фрукты
    "Бананы": "Овощи и фрукты", "Яблоки": "Овощи и фрукты", "Апельсины": "Овощи и фрукты",
    "Мандарины": "Овощи и фрукты", "Лимоны": "Овощи и фрукты", "Виноград": "Овощи и фрукты",
    "Дыня": "Овощи и фрукты", "Персики": "Овощи и фрукты", "Нектарины": "Овощи и фрукты",
    "Манго": "Овощи и фрукты", "Лук": "Овощи и фрукты", "Чеснок": "Овощи и фрукты",
    "Картофель": "Овощи и фрукты", "Капуста": "Овощи и фрукты", "Перец": "Овощи и фрукты",
    "Огурцы": "Овощи и фрукты", "Морковь": "Овощи и фрукты", "Грибы": "Овощи и фрукты",
    "Сухофрукты": "Овощи и фрукты", "Смесь овощная" : "Овощи и фрукты", "Горошек": "Овощи и фрукты",
    "Кукуруза": "Овощи и фрукты", "Огурцы": "Овощи и фрукты", "Абрикосы": "Овощи и фрукты",
    "Клубника": "Овощи и фрукты",
    
    #Орехи
    "Орехи": "Орехи", "Имбирь": "Имбирь",
    
    # Мясо и колбасы
    "Индейка": "Мясо и колбасы", "Курица": "Мясо и колбасы", "Свинина": "Мясо и колбасы",
    "Мясные изделия": "Мясо и колбасы", "Курица/Индейка": "Мясо и колбасы",
    "Пельмени": "Мясо и колбасы",

    # Рыба
    "Морепродукты": "Рыба и морепродукты", "Рыба": "Рыба и морепродукты",
    "Рыба консервированная": "Рыба и морепродукты", "Крабовое мясо": "Рыба и морепродукты",

    # Молочка
    "Йогурты": "Молочные продукты", "Творог": "Молочные продукты", "Сыр": "Молочные продукты",
    "Сметана": "Молочные продукты", "Сливки": "Молочные продукты", "Сливочное масло": "Молочные продукты",
    "Яйца": "Молочные продукты", "Мороженое": "Молочные продукты", "Молоко": "Молочные продукты",

    # Бакалея
    "Макаронные изделия": "Бакалея", "Крупы": "Бакалея", "Сахар": "Бакалея",
    "Соль": "Бакалея", "Растительное масло": "Бакалея", "Уксус": "Бакалея",
    "Соусы": "Бакалея", "Специи": "Бакалея",  

    # Сладости и Хлеб
    "Конфеты": "Сладости", "Зефир": "Сладости", "Вафли": "Сладости",
    "Шоколад": "Сладости", "Печенье": "Сладости", "Пастила": "Сладости",
    "Торты": "Сладости", "Сладости": "Сладости", "Хлеб": "Сладости", "Джем": "Сладости",


    # Напитки
    "Кофе": "Напитки", "Чай": "Напитки", "Вода": "Напитки",
    "Газировка": "Напитки", "Алкоголь": "Напитки",

    # Химия и гигиена
    "Косметика": "Бытовая химия и гигиена", "Гигиена": "Бытовая химия и гигиена",
    "Бытовая химия": "Бытовая химия и гигиена", "Хозтовары": "Бытовая химия и гигиена",
    "Пакет": "Пакет",

    # Прочее
    "Зоотовары": "Прочее", "Разное": "Прочее", "Cалфетки": "Прочее", "Журналы": "Прочее",
}

RUSSIAN_MONTHS_FULL = {
    1: "Января", 2: "Февраля", 3: "Марта", 4: "Апреля",
    5: "Мая", 6: "Июня", 7: "Июля", 8: "Августа",
    9: "Сентября", 10: "Октября", 11: "Ноября", 12: "Декабря"
}
RUSSIAN_MONTHS_SHORT = {
    "ЯНВ": 1, "ФЕВ": 2, "МАР": 3, "АПР": 4,
    "МАЙ": 5, "ИЮН": 6, "ИЮЛ": 7, "АВГ": 8,
    "СЕН": 9, "ОКТ": 10, "НОЯ": 11, "ДЕК": 12
}

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def parse_russian_date(date_str):
    if not date_str or date_str == "Не найдено":
        return "0000-00-00 00:00:00"
    try:
        date_str = date_str.strip().replace('\n', ' ')
        if ',' in date_str:
            date_part, time_part = date_str.split(',', 1)
            time_val = time_part.strip() + ':00'
        else:
            date_part = date_str
            time_val = "00:00:00"

        parts = date_part.split()
        day = int(parts[0])
        month_text = parts[1].upper()
        year = datetime.now().year
        for p in parts:
            if len(p) == 4 and p.isdigit():
                year = int(p)
                break
        month = 1
        for short, num in RUSSIAN_MONTHS_SHORT.items():
            if month_text.startswith(short):
                month = num
                break
        if month == 1 and "ЯНВ" not in month_text:
            for full, num in RUSSIAN_MONTHS_FULL.items():
                if full.lower() in month_text.lower():
                    month = num
                    break
        try:
            parsed_dt = datetime(year, month, day)
            if parsed_dt > datetime.now():
                year -= 1
        except ValueError:
            pass
        return f"{year}-{month:02d}-{day:02d} {time_val}"
    except Exception as e:
        print(f"Ошибка парсинга даты '{date_str}': {e}")
        return "0000-00-00 00:00:00"

def clean_price(price_str):
    if not price_str: return "0.00"
    digits = re.sub(r'\D', '', price_str)
    if not digits: return "0.00"
    if len(digits) < 3:
        digits = digits.zfill(3)
    return f"{digits[:-2]}.{digits[-2:]}"

def format_date_for_label(dt):
    return f"{dt.day} {RUSSIAN_MONTHS_FULL[dt.month]} {dt.year}"

# --- ЛОГИКА ОБРАБОТКИ ДАННЫХ (НОВАЯ ВЕТКА) ---
def process_data():
    print("Запуск обработки данных...")
    if not os.path.exists(CSV_FILE):
        print("Файл orders.csv не найден. Нечего обрабатывать.")
        return

    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=';')
        data = list(reader)

    if not data:
        print("Файл пуст.")
        return

    # 1. Удаление дубликатов
    # Уникальный ключ: дата + адрес + номер товара в чеке + название
    unique_data = {}
    for row in data:
        key = (row['date'], row['address'], row['item_no'], row['product_name'])
        unique_data[key] = row

    processed_list = list(unique_data.values())
    print(f"Дубликаты удалены. Осталось строк: {len(processed_list)}")

    # 2. Сортировка по дате и номеру товара
    # Сортируем сначала по дате (строка ISO), затем по item_no (число)
    processed_list.sort(key=lambda x: (x['date'], int(x['item_no']) if x['item_no'].isdigit() else 0))

    # 3. Категоризация
    final_data = []
    for row in processed_list:
        original_name = row['product_name'].lower()

        # Уровень 1: Нормализация (Поиск по ключевым словам)
        normalized_name = row['product_name'] # По умолчанию оригинал
        for keyword, unified_name in PRODUCT_MAPPING.items():
            if keyword.lower() in original_name:
                normalized_name = unified_name
                break

        # Уровень 2: Категория
        category = "Прочее"
        if normalized_name in CATEGORY_MAPPING:
            category = CATEGORY_MAPPING[normalized_name]

        # Добавляем новые поля в строку
        row['normalized_name'] = normalized_name
        row['category'] = category
        final_data.append(row)

    # 4. Сохранение в новый файл (перезапись)
    if final_data:
        keys = final_data[0].keys()
        with open(SORTED_CSV_FILE, 'w', encoding='utf-8-sig', newline='') as f:
            dict_writer = csv.DictWriter(f, fieldnames=keys, delimiter=';')
            dict_writer.writeheader()
            dict_writer.writerows(final_data)
        print(f"Обработанные данные сохранены в {SORTED_CSV_FILE}")

# --- ЛОГИКА КАЛЕНДАРЯ ---
async def select_date_range(page, start_date_str, end_date_str):
    print(f"Установка периода: {start_date_str} -> {end_date_str}")
    calendar_trigger = page.locator('div[aria-label*="Выбрать период"]')
    await calendar_trigger.click()
    await asyncio.sleep(0.5)

    start_dt = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date_str, '%Y-%m-%d')

    async def pick_date(target_dt):
        target_label = format_date_for_label(target_dt)
        for _ in range(24):
            day_button = page.locator(f'button[aria-label="{target_label}"]')
            if await day_button.count() > 0:
                await day_button.first.click()
                return
            any_date_btn = page.locator('button[aria-label*=" 20"]').first
            if await any_date_btn.count() == 0: break
            current_label = await any_date_btn.get_attribute("aria-label")
            current_year = int(current_label.split()[-1])
            current_month_name = current_label.split()[1]
            month_to_num = {v: k for k, v in RUSSIAN_MONTHS_FULL.items()}
            current_month_num = month_to_num.get(current_month_name, 1)
            if (target_dt.year > current_year) or \
               (target_dt.year == current_year and target_dt.month > current_month_num):
                await page.get_by_label("Следующий месяц").click()
            else:
                await page.get_by_label("Предыдущий месяц").click()
            await asyncio.sleep(0.3)

    await pick_date(start_dt)
    await asyncio.sleep(0.5)
    await pick_date(end_dt)
    await asyncio.sleep(0.5)

    try:
        confirm_btn = page.get_by_role("button", name="Выбрать", exact=True)
        if await confirm_btn.count() > 0:
            await confirm_btn.last.click()
        else:
            await page.locator('button:has-text("Выбрать")').last.click()
    except Exception as e:
        print(f"Не удалось нажать 'Выбрать': {e}")
    await asyncio.sleep(1)

# --- ОСНОВНОЙ ПАРСИНГ ---
async def check_auth(page):
    await asyncio.sleep(2)
    keywords = ["Главная", "История", "Партнёры"]
    for word in keywords:
        if await page.get_by_text(word).count() > 0:
            return True
    return False

async def parse_flow(page, start_date, end_date):
    try:
        await select_date_range(page, start_date, end_date)
    except Exception as e:
        print(f"Ошибка при выборе дат: {e}")

    if await page.get_by_text("История").count() > 0:
        if await page.locator('button[class*="_container"]').count() == 0:
            await page.get_by_text("История").first.click()
            await asyncio.sleep(3)

    while True:
        load_more_btn = page.get_by_text("Показать ещё", exact=True).first
        if await load_more_btn.is_visible():
            print("Загружаю больше заказов...")
            await load_more_btn.click()
            await asyncio.sleep(2)
        else:
            break

    order_buttons = page.locator('button[class*="_container"]')
    count = await order_buttons.count()
    print(f"Итого найдено заказов: {count}")

    all_data = []
    for i in range(count):
        try:
            btn = order_buttons.nth(i)
            await btn.scroll_into_view_if_needed()
            await btn.click()
            await page.wait_for_selector("h3.tp-h4-medium, h3.tp-h4-regular", timeout=5000)
            await asyncio.sleep(1)

            try:
                date_raw = await page.locator("h3.tp-h4-medium").first.inner_text()
                address_val = await page.locator("h3.tp-h4-regular").first.inner_text()
            except:
                date_raw, address_val = "Не найдено", "Не найдено"

            formatted_date = parse_russian_date(date_raw)

            rows = await page.locator(".flex.flex-row.gap-3.py-1").all()
            for idx, row in enumerate(rows, start=1):
                full_text = await row.inner_text()
                try:
                    text_no_img_price = re.sub(r'^\d+[\.,]\d+\s+', '', full_text)
                    split_match = re.search(r'(\d+(?:[\.,]\d+)?)\s*(шт\.|кг\.)\s*x\s*', text_no_img_price)

                    if split_match:
                        product_name = text_no_img_price[:split_match.start()].strip()
                        qty = split_match.group(1).strip()
                        prices_part = text_no_img_price[split_match.end():]
                        price_blocks = [p.strip() for p in prices_part.split('₽') if p.strip()]

                        p_unit_raw = price_blocks[0] if len(price_blocks) > 0 else "0"
                        p_total_raw = price_blocks[1] if len(price_blocks) > 1 else "0"
                        p_disc_raw = price_blocks[2] if len(price_blocks) > 2 else "0"
                    else:
                        product_name, qty, p_unit_raw, p_total_raw, p_disc_raw = "Не определено", "0", "0", "0", "0"

                    bonuses_match = re.search(r'\+\s*(\d+)', full_text)
                    bonuses = bonuses_match.group(1) if bonuses_match else "0"

                    all_data.append({
                        "item_no": idx,
                        "date": formatted_date,
                        "address": address_val.strip(),
                        "product_name": product_name,
                        "quantity": qty,
                        "price_unit": clean_price(p_unit_raw),
                        "total_price": clean_price(p_total_raw),
                        "discount_price": clean_price(p_disc_raw),
                        "bonuses": bonuses
                    })
                except Exception as row_e:
                    print(f"Ошибка строки: {row_e}")

            try:
                close_btn = page.get_by_label("Закрыть")
                if await close_btn.is_visible(): await close_btn.first.click()
                else: await page.keyboard.press("Escape")
            except:
                await page.keyboard.press("Escape")
            await asyncio.sleep(0.7)
        except Exception as e:
            print(f"Ошибка заказа {i}: {e}")

    if all_data:
        keys = all_data[0].keys()
        file_exists = os.path.exists(CSV_FILE)
        with open(CSV_FILE, 'a', encoding='utf-8-sig', newline='') as f:
            dict_writer = csv.DictWriter(f, fieldnames=keys, delimiter=';')
            if not file_exists:
                dict_writer.writeheader()
            dict_writer.writerows(all_data)
        print(f"Успешно сохранено {len(all_data)} позиций.")

async def main():
    if len(sys.argv) < 2:
        print("Использование: python scriptname.py login ИЛИ python scriptname.py parse [start_date] [end_date] ИЛИ python scriptname.py process")
        return

    mode = sys.argv[1]

    if mode == "process":
        process_data()
        return # Завершаем выполнение, так как браузер не нужен

    if mode == "parse":
        if len(sys.argv) >= 3: start_date = sys.argv[2]
        else: start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if len(sys.argv) >= 4: end_date = sys.argv[3]
        else: end_date = datetime.now().strftime('%Y-%m-%d')

    config = load_config()
    async with async_playwright() as p:
        # Если режим login — открываем браузер (чтобы ввести SMS),
        # если parse — запускаем в фоне (headless=True)
        is_headless = True if mode == "parse" else False
        browser = await p.chromium.launch(headless=is_headless)
        if mode == "login":
            context = await browser.new_context()
            page = await context.new_page()
            await page.goto(config['url'])
            try:
                await page.wait_for_selector("#mobile_phone", timeout=15000)
                await page.fill("#mobile_phone", config['phone'])
                await page.click("form:has(#mobile_phone) button[type='submit']")
                print("\nОЖИДАНИЕ: Введите SMS в браузере...")
                while not await check_auth(page): await asyncio.sleep(2)
                await context.storage_state(path=STATE_FILE)
                print("Сессия сохранена.")
            except Exception as e:
                print(f"Ошибка: {e}")
        elif mode == "parse":
            if not os.path.exists(STATE_FILE):
                print("Нет файла сессии!"); await browser.close(); return
            context = await browser.new_context(storage_state=STATE_FILE)
            page = await context.new_page()
            await page.goto(config['url'])
            if await check_auth(page):
                await parse_flow(page, start_date, end_date)
            else:
                print("Сессия истекла. Запустите login.")
        await asyncio.sleep(2)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
