import asyncio
import json
import os
import sys
import csv
import re
import hashlib
import httpx
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

# --- КОНФИГУРАЦИЯ ---
STATE_FILE = "state.json"
CSV_FILE = "orders.csv"
SORTED_CSV_FILE = "orders_sorted.csv"
MAPPING_FILE = "mapping.json"
IMG_FOLDER = "img"

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

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_mapping():
    if not os.path.exists(MAPPING_FILE):
        print(f"Предупреждение: Файл {MAPPING_FILE} не найден.")
        return {}
    with open(MAPPING_FILE, 'r', encoding='utf-8') as f:
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

async def download_product_image(url, product_name):
    if not url or url == "Не найдено":
        return "Не найдено"
    try:
        hash_name = hashlib.md5(product_name.encode('utf-8')).hexdigest()
        ext = ".jpg"
        if ".png" in url.lower(): ext = ".png"
        elif ".jpeg" in url.lower(): ext = ".jpeg"

        filename = f"{hash_name}{ext}"
        filepath = os.path.join(IMG_FOLDER, filename)

        if os.path.exists(filepath):
            return filename

        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10.0)
            if response.status_code == 200:
                with open(filepath, "wb") as f:
                    f.write(response.content)
                return filename
    except Exception as e:
        print(f"Ошибка при загрузке фото для {product_name}: {e}")
    return "Ошибка"

# --- ЛОГИКА ОБРАБОТКИ ДАННЫХ ---

def process_data():
    print("Запуск обработки данных...")
    new_data = []
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=';')
            new_data = list(reader)

    existing_data = []
    if os.path.exists(SORTED_CSV_FILE):
        with open(SORTED_CSV_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=';')
            existing_data = list(reader)

    if not new_data and not existing_data:
        print("Нет данных для обработки.")
        return

    combined_data = existing_data + new_data

    # Удаление дубликатов
    unique_map = {}
    for row in combined_data:
        key = (row.get('date'), row.get('address'), row.get('item_no'), row.get('product_name'))
        unique_map[key] = row

    processed_list = list(unique_map.values())
    processed_list.sort(key=lambda x: (x.get('date', ''), int(x.get('item_no', 0)) if x.get('item_no', '').isdigit() else 0))

    mapping = load_mapping()
    final_data = []
    for row in processed_list:
        # Гарантируем наличие поля image, если его нет в старых данных
        if 'image' not in row:
            row['image'] = 'Не найдено'

        original_name = row.get('product_name', '').lower()
        normalized_name = row.get('product_name', 'Не определено')
        category = "Прочее"
        for keyword, values in mapping.items():
            if keyword.lower() in original_name:
                normalized_name = values[0]
                category = values[1]
                break
        row['normalized_name'] = normalized_name
        row['category'] = category
        final_data.append(row)

    if final_data:
        # Берем ключи из первой строки, чтобы заголовок был правильным
        keys = final_data[0].keys()
        with open(SORTED_CSV_FILE, 'w', encoding='utf-8-sig', newline='') as f:
            dict_writer = csv.DictWriter(f, fieldnames=keys, delimiter=';')
            dict_writer.writeheader()
            dict_writer.writerows(final_data)
        print(f"Итоговые данные сохранены в {SORTED_CSV_FILE}")

# --- ЛОГИКА ПАРСИНГА ---

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
            if (target_dt.year > current_year) or (target_dt.year == current_year and target_dt.month > current_month_num):
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

async def check_auth(page):
    await asyncio.sleep(2)
    keywords = ["Главная", "История", "Партнёры"]
    for word in keywords:
        if await page.get_by_text(word).count() > 0:
            return True
    return False

async def parse_flow(page, start_date, end_date):
    os.makedirs(IMG_FOLDER, exist_ok=True)

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

                img_elem = row.locator('img')
                img_url = "Не найдено"
                if await img_elem.count() > 0:
                    img_url = await img_elem.first.get_attribute('src')

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

                    img_filename = await download_product_image(img_url, product_name)

                    bonuses_match = re.search(r'\+\s*(\d+)', full_text)
                    bonuses = bonuses_match.group(1) if bonuses_match else "0"

                    all_data.append({
                        "item_no": idx,
                        "date": formatted_date,
                        "address": address_val.strip(),
                        "product_name": product_name,
                        "image": img_filename,
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
        print(f"Успешно сохранено {len(all_data)} позиций в {CSV_FILE}.")

async def main():
    if len(sys.argv) < 2:
        print("Использование:\n python script.py login\n python script.py parse [start_date] [end_date]\n python script.py process")
        return

    mode = sys.argv[1]

    if mode == "process":
        process_data()
        return

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
                print("Нет файла сессии! Сначала запустите login."); await browser.close(); return
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