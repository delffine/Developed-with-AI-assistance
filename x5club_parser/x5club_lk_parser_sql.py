import asyncio
import json
import os
import sys
import re
import hashlib
import httpx
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

# --- КОНФИГУРАЦИЯ ---
STATE_FILE = "state.json"
MAPPING_FILE = "mapping.json"
IMG_FOLDER = "img"
MAX_PERIOD_DAYS = 30

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

# --- РАБОТА С БАЗОЙ ДАННЫХ ---

class Database:
    def __init__(self, config):
        self.conn_params = config['db']

    def get_connection(self):
        return psycopg2.connect(**self.conn_params)

    def init_db(self):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS orders (
                        idx_hash TEXT PRIMARY KEY,
                        updated_at TIMESTAMP DEFAULT now(),
                        order_date TIMESTAMP,
                        address TEXT,
                        item_no INTEGER,
                        product_name TEXT,
                        normalized_name TEXT,
                        category TEXT,
                        image_path TEXT,
                        quantity NUMERIC,
                        price_unit NUMERIC(10,2),
                        total_price NUMERIC(10,2),
                        discount_price NUMERIC(10,2),
                        bonuses INTEGER
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS parser_logs (
                        id SERIAL PRIMARY KEY,
                        created_at TIMESTAMP DEFAULT now(),
                        procedure TEXT,
                        message TEXT,
                        status INTEGER
                    );
                """)
            conn.commit()
        finally:
            conn.close()

    def log(self, procedure, message, status=2):
        prefix = {0: "[ERROR]", 1: "[INFO]", 2: "[LOG]"}.get(status, "[?]")
        logtime = datetime.now().strftime("%Y.%m.%d %H:%M:%S")
        print(f"{logtime} - {prefix} - {procedure}: {message}")
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO parser_logs (procedure, message, status) VALUES (%s, %s, %s)",
                    (procedure, message, status)
                )
            conn.commit()
        except Exception as e:
            print(f"Критическая ошибка записи в лог: {e}")
        finally:
            if conn: conn.close()

    def upsert_orders(self, orders_list):
        if not orders_list: return 0
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                query = """
                    INSERT INTO orders (
                        idx_hash, updated_at, order_date, address, item_no,
                        product_name, normalized_name, category, image_path,
                        quantity, price_unit, total_price, discount_price, bonuses
                    ) VALUES %s
                    ON CONFLICT (idx_hash) DO UPDATE SET
                        updated_at = now(),
                        normalized_name = EXCLUDED.normalized_name,
                        category = EXCLUDED.category,
                        image_path = EXCLUDED.image_path;
                """
                data = [
                    (
                        o['idx_hash'], datetime.now(), o['date'], o['address'], o['item_no'],
                        o['product_name'], o['normalized_name'], o['category'], o['image'],
                        o['quantity'], o['price_unit'], o['total_price'], o['discount_price'], o['bonuses']
                    ) for o in orders_list
                ]
                execute_values(cur, query, data)
                #count = cur.rowcount
                count = len(orders_list)
            conn.commit()
            return count
        except Exception as e:
            if conn: conn.rollback()
            raise e
        finally:
            if conn: conn.close()

    def get_last_order_date(self):
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT MAX(order_date) as last_date FROM orders")
                row = cur.fetchone()
                return row['last_date'] if row and row['last_date'] else None
        except Exception:
            return None
        finally:
            if conn: conn.close()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_mapping():
    if not os.path.exists(MAPPING_FILE): return {}
    with open(MAPPING_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def parse_russian_date(date_str):
    if not date_str or date_str == "Не найдено": return "0000-00-00 00:00:00"
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
            if parsed_dt > datetime.now(): year -= 1
        except ValueError: pass
        return f"{year}-{month:02d}-{day:02d} {time_val}"
    except Exception:
        return "0000-00-00 00:00:00"

def clean_price(price_str):
    if not price_str: return 0.00
    digits = re.sub(r'\D', '', price_str)
    if not digits: return 0.00
    if len(digits) < 3: digits = digits.zfill(3)
    return float(f"{digits[:-2]}.{digits[-2:]}")

def format_date_for_label(dt):
    return f"{dt.day} {RUSSIAN_MONTHS_FULL[dt.month]} {dt.year}"

async def download_image_task(client, url, product_name):
    if not url or url == "Не найдено": return "Не найдено"
    try:
        hash_name = hashlib.md5(product_name.encode('utf-8')).hexdigest()
        ext = ".jpg"
        if ".png" in url.lower(): ext = ".png"
        elif ".jpeg" in url.lower(): ext = ".jpeg"
        filename = f"{hash_name}{ext}"
        filepath = os.path.join(IMG_FOLDER, filename)
        if os.path.exists(filepath): return filename
        response = await client.get(url, timeout=10.0)
        if response.status_code == 200:
            with open(filepath, "wb") as f: f.write(response.content)
            return filename
    except: pass
    return "Ошибка"

async def process_images_batch(image_data):
    if not image_data: return {}
    results = {}
    async with httpx.AsyncClient() as client:
        tasks = [download_image_task(client, url, name) for name, url in image_data]
        downloaded_filenames = await asyncio.gather(*tasks)
        for i, (name, url) in enumerate(image_data):
            results[f"{name}_{url}"] = downloaded_filenames[i]
    return results

def calculate_dates(db, arg_start=None, arg_end=None):
    today = datetime.now()
    if arg_start and arg_end:
        start_date = datetime.strptime(arg_start, '%Y-%m-%d')
        end_date = datetime.strptime(arg_end, '%Y-%m-%d')
    else:
        last_date = db.get_last_order_date()
        if last_date:
            start_date = last_date + timedelta(days=1)
            end_date = start_date + timedelta(days=MAX_PERIOD_DAYS)
        else:
            start_date = today - timedelta(days=365)
            end_date = start_date + timedelta(days=MAX_PERIOD_DAYS)
    if end_date > today:
        end_date = today
    if start_date > today:
        return None, None
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

async def select_date_range(page, start_date_str, end_date_str):
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
    confirm_btn = page.get_by_role("button", name="Выбрать", exact=True)
    if await confirm_btn.count() > 0: await confirm_btn.last.click()
    else: await page.locator('button:has-text("Выбрать")').last.click()
    await asyncio.sleep(1)

async def check_auth(page):
    await asyncio.sleep(2)
    for word in ["Главная", "История", "Партнёры"]:
        if await page.get_by_text(word).count() > 0: return True
    return False

async def parse_flow(page, db, start_date, end_date):
    os.makedirs(IMG_FOLDER, exist_ok=True)
    db.log("parse", f"Начинаю парсинг", 2)
    
    try:
        await select_date_range(page, start_date, end_date)
        db.log("parse", f"Выбрал даты на календаре", 2)        
    except Exception as e:
        db.log("parse", f"Ошибка при выборе дат: {e}", 0)

    if await page.get_by_text("История").count() > 0:
        if await page.locator('button[class*="_container"]').count() == 0:
            await page.get_by_text("История").first.click()
            await asyncio.sleep(3)

    # Убрали лог "Загружаю больше заказов..."
    while True:
        load_more_btn = page.get_by_text("Показать ещё", exact=True).first
        if await load_more_btn.is_visible():
            await load_more_btn.click()
            await asyncio.sleep(2)
        else: break

    order_buttons = page.locator('button[class*="_container"]')
    count = await order_buttons.count()
    db.log("parse", f"Найдено покупок: {count}", 2)

    all_items = []
    image_queue = []

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
                        p_unit = price_blocks[0] if len(price_blocks) > 0 else "0"
                        p_total = price_blocks[1] if len(price_blocks) > 1 else "0"
                        p_disc = price_blocks[2] if len(price_blocks) > 2 else "0"
                    else:
                        product_name, qty, p_unit, p_total, p_disc = "Не определено", "0", "0", "0", "0"

                    image_queue.append((product_name, img_url))
                    bonuses_match = re.search(r'\+\s*(\d+)', full_text)
                    bonuses = int(bonuses_match.group(1)) if bonuses_match else 0

                    all_items.append({
                        "date": formatted_date, "address": address_val.strip(), "item_no": idx,
                        "product_name": product_name, "image_url": img_url, "quantity": qty,
                        "price_unit": clean_price(p_unit), "total_price": clean_price(p_total),
                        "discount_price": clean_price(p_disc), "bonuses": bonuses
                    })

                    # --- НОВАЯ ЛОГИКА ЛОГИРОВАНИЯ ПОЗИЦИЙ ---
                    if len(all_items) % 50 == 0:
                        db.log("parse", f"Получено позиций: {len(all_items)}", 2)

                except Exception as row_e:
                    db.log("parse", f"Ошибка строки: {row_e}", 0)

            try:
                close_btn = page.get_by_label("Закрыть")
                if await close_btn.is_visible(): await close_btn.first.click()
                else: await page.keyboard.press("Escape")
            except:
                await page.keyboard.press("Escape")
            await asyncio.sleep(0.7)
        except Exception as e:
            db.log("parse", f"Ошибка заказа {i}: {e}", 0)


    if image_queue:
        db.log("parse", f"Загрузка изображений ({len(image_queue)} шт)...", 2)
        image_results = await process_images_batch(image_queue)
        for item in all_items:
            key = f"{item['product_name']}_{item['image_url']}"
            item['image'] = image_results.get(key, "Ошибка")
            if 'image_url' in item: del item['image_url']

    mapping = load_mapping()
    final_data = []
    for row in all_items:
        orig_name = row['product_name'].lower()
        norm_name, cat = row['product_name'], "Прочее"
        for kw, vals in mapping.items():
            if kw.lower() in orig_name:
                norm_name, cat = vals[0], vals[1]
                break
        row['normalized_name'], row['category'] = norm_name, cat
        unique_str = f"{row['date']}|{row['address']}|{row['item_no']}|{row['product_name']}"
        row['idx_hash'] = hashlib.md5(unique_str.encode('utf-8')).hexdigest()
        final_data.append(row)

    inserted = db.upsert_orders(final_data)
    db.log("parse", f"Успешно добавлено/обновлено позиций: {inserted}", 2)
    return inserted

async def main():
    if len(sys.argv) < 2:
        print("Использование:\n python script.py login\n python script.py parse [start_date] [end_date]\n python script.py run_all [start_date] [end_date]")
        return

    mode = sys.argv[1]
    config = load_config()
    db = Database(config)
    db.init_db()

    try:
        if mode == "run_all":
            arg_start = sys.argv[2] if len(sys.argv) >= 3 else None
            arg_end = sys.argv[3] if len(sys.argv) >= 4 else None
            start_date, end_date = calculate_dates(db, arg_start, arg_end)
            if not start_date:
                db.log("run_all", "Все данные за период выкачаны.", 1)
                return
            db.log("run_all", f"Старт цикла: {start_date} -> {end_date}", 1)
            async with async_playwright() as p:
                if not os.path.exists(STATE_FILE):
                    db.log("run_all", "Ошибка: Нет файла сессии", 0)
                    return
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(storage_state=STATE_FILE)
                page = await context.new_page()
                await page.goto(config['url'], wait_until="networkidle")
                if await check_auth(page):
                    await parse_flow(page, db, start_date, end_date)
                    db.log("run_all", "Цикл завершен успешно", 1)
                else:
                    db.log("run_all", "Сессия истекла", 0)
                await browser.close()
            return

        async with async_playwright() as p:
            is_headless = True if mode != "login" else False
            browser = await p.chromium.launch(headless=is_headless)
            if mode == "login":
                context = await browser.new_context()
                page = await context.new_page()
                await page.goto(config['url'], wait_until="networkidle")
                try:
                    await page.wait_for_selector("#mobile_phone", timeout=15000)
                    await page.fill("#mobile_phone", config['phone'])
                    await page.click("form:has(#mobile_phone) button[type='submit']")
                    print("\nОЖИДАНИЕ: Введите SMS в браузере...")
                    while not await check_auth(page): await asyncio.sleep(2)
                    await context.storage_state(path=STATE_FILE)
                    db.log("login", "Сессия успешно сохранена", 1)
                except Exception as e:
                    db.log("login", f"Ошибка: {e}", 0)
            elif mode == "parse":
                arg_start = sys.argv[2] if len(sys.argv) >= 3 else None
                arg_end = sys.argv[3] if len(sys.argv) >= 4 else None
                start_date, end_date = calculate_dates(db, arg_start, arg_end)
                if not start_date:
                    print("Данных для парсинга нет."); await browser.close(); return
                if not os.path.exists(STATE_FILE):
                    print("Запустите login"); await browser.close(); return
                context = await browser.new_context(storage_state=STATE_FILE)
                page = await context.new_page()
                await page.goto(config['url'], wait_until="networkidle")
                if await check_auth(page):
                    await parse_flow(page, db, start_date, end_date)
                else:
                    db.log("parse", "Сессия истекла", 0)
            await browser.close()
    except Exception as e:
        db.log(mode, f"Критический сбой процедуры: {e}", 0)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())