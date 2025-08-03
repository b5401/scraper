import os
import re
import random
import json
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException, TimeoutException
from sqlalchemy import create_engine
import pandas as pd

# --- DATABASE CONFIG ---
DB_CONFIG = {
    'dbname': 'scraper_db',
    'user': 'admin',
    'password': 'admin',
    'host': 'localhost',
    'port': 5432,
}

# Глобальные переменные
driver = None
wait = None
max_retries = 3

def save_to_postgres(data, output_table):
    # Создаем SQLAlchemy engine
    engine = create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )

    # Преобразуем список словарей в DataFrame
    df = pd.DataFrame(data)

    # Разбираем координаты на отдельные колонки
    if 'coordinates' in df.columns:
        coords_df = pd.json_normalize(df['coordinates'], errors='ignore')
        coords_df.columns = ['lat', 'lon']
        df = pd.concat([df.drop('coordinates', axis=1), coords_df], axis=1)

    # Приводим названия колонок к нижнему регистру и заменяем точки на подчеркивания (для БД)
    df.columns = [col.lower().replace(" ", "_").replace(".", "_") for col in df.columns]
    print(df)

    # Записываем данные в PostgreSQL
    try:
        print('Попытка записать данные в файл')
        df.to_csv(f'{output_table}.csv', index=False, encoding='utf-8')
        df.to_sql(
            name=output_table,
            con=engine,
            if_exists='append',
            index=False
        )
        print(f"Успешно записано {len(df)} записей в таблицу {output_table}")
    except Exception as e:
        print("Ошибка записи в БД через pandas:", e)

def setup_dirs():
    """Создает необходимые директории"""
    os.makedirs("screenshots", exist_ok=True)

def init_driver():
    """Инициализация драйвера с настройками"""
    global driver, wait
    
    if driver:
        try:
            driver.quit()
        except:
            pass
            
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    chrome_options.add_argument("--headless=new")  # Headless режим
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(options=chrome_options)
    wait = WebDriverWait(driver, 25)

def restart_driver():
    """Перезапускает драйвер и открывает главную страницу карт Яндекса"""
    global driver
    
    print("Перезапускаем драйвер...")
    try:
        if driver:
            driver.quit()
    finally:
        init_driver()
        
    try:
        driver.get("https://yandex.ru/maps")  
        print("Драйвер перезапущен, страница открыта.")
        time.sleep(5)
    except Exception as e:
        print(f"Ошибка при открытии страницы после перезапуска: {e}")

def safe_find(by, selector, timeout=15, optional=False):
    """Безопасный поиск элемента с повторами"""
    global driver, wait, max_retries
    
    for attempt in range(max_retries):
        try:
            element = wait.until(
                EC.presence_of_element_located((by, selector))
            )
            return element
        except (TimeoutException, WebDriverException) as e:
            print(f"Ошибка поиска элемента '{selector}' (попытка {attempt + 1}): {str(e)}")
            if attempt < max_retries - 1:
                restart_driver()
            else:
                if not optional:
                    print(f"Не удалось найти обязательный элемент: {selector}")
                return None

def get_text_safe(parent, selector):
    """Безопасное получение текста"""
    try:
        return parent.find_element(By.CSS_SELECTOR, selector).text.strip()
    except:
        return None

def get_attr_safe(parent, selector, attr):
    """Безопасное получение атрибута"""
    try:
        return parent.find_element(By.CSS_SELECTOR, selector).get_attribute(attr)
    except:
        return None

def get_coords_from_element(org_element):
    """
    Извлекает координаты из атрибута data-coordinates у элемента организации.
    :param org_element: WebElement — элемент организации
    :return: dict or None — {'lat': float, 'lon': float}
    """
    global driver
    
    try:
        coords_str = driver.execute_script("""
            let el = arguments[0];
            while (el && !el.hasAttribute('data-coordinates')) {
                el = el.parentElement;
            }
            return el ? el.getAttribute('data-coordinates') : null;
        """, org_element)
        
        if coords_str and ',' in coords_str:
            lon, lat = map(float, coords_str.split(',', 1))
            return {
                "lat": lat,
                "lon": lon
            }
        else:
            print(f"Неверный формат координат: {coords_str}")
            return None
    except Exception as e:
        print(f"Ошибка при извлечении координат: {e}")
        return None

def parse_organization(org_element):
    """Парсит данные одной организации"""
    try:
        name = get_text_safe(org_element, '.search-business-snippet-view__title')
        address = get_text_safe(org_element, '.search-business-snippet-view__address')
        
        if not name or not address:
            return None
            
        link = get_attr_safe(org_element, 'a[href*="/org/"]', 'href')
        
        # Извлечение рейтинга
        rating_elem = org_element.find_element(
            By.CSS_SELECTOR, 
            '.business-rating-badge-view__rating-text'
        ) if True else None
        
        rating = rating_elem.text.strip() if rating_elem else None
        
        # Извлечение среднего чека
        avg_price = None
        try:
            subtitle_views = org_element.find_elements(
                By.CSS_SELECTOR,
                '.search-business-snippet-subtitle-view'
            )
            
            for subtitle in subtitle_views:
                title_elem = subtitle.find_element(
                    By.CSS_SELECTOR,
                    '.search-business-snippet-subtitle-view__title'
                ) if True else None
                
                if title_elem and ("Ср. чек" in title_elem.text or "Пиво" in title_elem.text):
                    desc_elem = subtitle.find_element(
                        By.CSS_SELECTOR,
                        '.search-business-snippet-subtitle-view__description'
                    ) if True else None
                    
                    if desc_elem:
                        price_text = desc_elem.text.strip()
                        
                        # Ищем диапазон (например, "1000–2000")
                        range_match = re.search(r'(\d+)[–\-]\s*(\d+)', price_text)
                        if range_match:
                            min_val = int(range_match.group(1))
                            max_val = int(range_match.group(2))
                            avg_price = str(round((min_val + max_val) / 2))
                        else:
                            # Ищем любое число
                            num_match = re.search(r'\d+', price_text)
                            avg_price = num_match.group(0) if num_match else None
                        break
        except Exception as e:
            print(f"Ошибка при парсинге среднего чека: {e}")
            
        # Извлечение количества оценок
        reviews_count = None
        try:
            reviews_elem = org_element.find_element(
                By.CSS_SELECTOR,
                '.business-rating-amount-view'
            ) if True else None
            
            if reviews_elem:
                text = reviews_elem.text.strip()
                match = re.search(r'(\d+)', text)
                reviews_count = match.group(1) if match else None
        except Exception as e:
            pass
            
        # Извлечение координат
        coords = get_coords_from_element(org_element)
        
        return {
            'name': name,
            'address': address,
            'rating': rating,
            'avg_price': avg_price,
            'reviews_count': reviews_count,
            'link': link,
            'coordinates': coords
        }
    except Exception as e:
        pass

def scroll_to_load_organizations():
    """Прокручивает список организаций, чтобы загрузить все элементы"""
    previous_count = 0
    no_change_count = 0
    max_no_change = 5  # Максимальное количество повторов без изменений
    # Находим основной контейнер для прокрутки
    scroll_container = driver.find_element(By.CSS_SELECTOR, '.scroll__container')

    while True:
        # Получаем текущее количество организаций
        org_elements = driver.find_elements(By.CSS_SELECTOR, '.search-business-snippet-view')
        current_count = len(org_elements)
        print(f'Текущее количество организаций на странице: {current_count}')

        if current_count == previous_count or current_count >= 300:
            no_change_count += 1
            print(f"Количество организаций не изменилось либо превысило 300 штук (повтор {no_change_count}/{max_no_change})")
            
            if no_change_count >= max_no_change:
                print("Достигнуто максимальное количество повторов без изменений. Поиск остановлен.")
                break
        else:
            no_change_count = 0  # Сбрасываем счетчик, если количество изменилось

        # Прокручиваем вниз
        for _ in range(10):
            try:
                driver.execute_script("arguments[0].scrollTop += 500;", scroll_container)
                time.sleep(0.1)
            except Exception as e:
                print(f"Ошибка прокрутки: {e}")
                break

        previous_count = current_count
        time.sleep(1)

    return previous_count

def search_organizations(query):
    """Выполняет поиск организаций в пределах Москвы и делает скриншот"""
    global max_retries

    # Убедитесь, что папка screenshots существует
    if not os.path.exists("screenshots"):
        os.makedirs("screenshots")

    moscow_center_url = "https://yandex.ru/maps/213/moscow/?ll=37.622504%2C55.752334&z=10"

    for attempt in range(max_retries):
        try:
            driver.get(moscow_center_url)
            print(f"Открыта карта Москвы: {moscow_center_url}")
            time.sleep(3)

            # Найти поле поиска 
            search_input = safe_find(By.CSS_SELECTOR, 'input[placeholder*="Поиск"]')
            if not search_input:
                continue

            search_input.clear()

            # Постепенный ввод текста
            for char in query:
                search_input.send_keys(char)
                time.sleep(random.uniform(0.1, 0.3))

            search_input.send_keys(Keys.RETURN)
            time.sleep(5)  # Ждём загрузку результатов

            # Нажимаем на кнопку "–", чтобы уменьшить масштаб
            zoom_out_button = safe_find(By.XPATH, '//button[@aria-label="Отдалить"]')
            if zoom_out_button:
                zoom_out_button.click()
                print("Нажата кнопка '–', масштаб уменьшен")
                time.sleep(2)  # Ждём обновления карты

            # Проверяем, загрузились ли результаты
            if safe_find(By.CSS_SELECTOR, '.search-business-snippet-view'):

                # Делаем скриншот после успешного поиска
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                screenshot_path = f"screenshots/result_{timestamp}_{query}.png"
                driver.save_screenshot(screenshot_path)
                print(f"Скриншот сохранён: {screenshot_path}")

                return True

        except Exception as e:
            print(f"Ошибка поиска (попытка {attempt + 1}): {str(e)}")
            if attempt < max_retries - 1:
                restart_driver()
            else:
                return False

    return False

def scrape(query, category, max_retries=3):
    """Основной метод сбора данных с возможностью повторного запуска при малом количестве организаций"""
    global driver

    attempt = 0
    
    while attempt <= max_retries:
        try:
            if not search_organizations(query):
                print(f"Не удалось выполнить поиск для: {query}")
                attempt += 1
                print(f"Попытка {attempt} из {max_retries}. Перезапуск...")
                driver.quit()
                driver = init_driver()  # Пересоздаем драйвер
                continue

            total_orgs = scroll_to_load_organizations()
            print(f"Всего загружено организаций: {total_orgs}")

            if total_orgs < 10:
                attempt += 1
                print(f"Организаций меньше 10. Попытка {attempt} из {max_retries}. Перезапуск...")
                driver.quit()
                driver = init_driver()  # Пересоздаем драйвер
                continue

            org_elements = driver.find_elements(By.CSS_SELECTOR, '.search-business-snippet-view')
            results = []

            for org in org_elements:
                org_data = parse_organization(org)
                if org_data:
                    results.append(org_data)

            # Удаление дубликатов
            seen = set()
            unique_results = []

            for item in results:
                key = (item['name'], item['address'])
                if key not in seen:
                    seen.add(key)
                    unique_results.append(item)

            print(f"Уникальных организаций: {len(unique_results)}")

            df = pd.DataFrame(unique_results)

            # Разбираем координаты на отдельные колонки
            if 'coordinates' in df.columns:
                coords_df = pd.json_normalize(df['coordinates'], errors='ignore')
                coords_df.columns = ['lat', 'lon']
                df = pd.concat([df.drop('coordinates', axis=1), coords_df], axis=1)

            # Приводим названия колонок к нижнему регистру и заменяем точки на подчеркивания (для БД)
            df.columns = [col.lower().replace(" ", "_").replace(".", "_") for col in df.columns]
            df['category'] = category
            df['insert_date'] = str(datetime.now().date())
            print(df)

            try:
                print('Попытка записать данные в файл')
                df.to_csv(f'output.csv', index=False, encoding='utf-8', mode='a')

            except Exception as e:
                print("Ошибка записи в файл через pandas:", e)

            # Сохранение в JSON (опционально)
            # with open(f'results/{output_table}.json', 'w', encoding='utf-8') as f:
            #     json.dump(unique_results, f, ensure_ascii=False, indent=2)

            return True

        except Exception as e:
            print(f"Критическая ошибка: {str(e)}")
            driver.save_screenshot(f"screenshots/error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            attempt += 1
            print(f"Произошла ошибка. Попытка {attempt} из {max_retries}. Перезапуск...")
            driver.quit()
            driver = init_driver()  # Пересоздаем драйвер

    print("Достигнуто максимальное количество попыток. Завершение работы.")
    return False

if __name__ == "__main__":
    setup_dirs()
    init_driver()
    
    queries = [
    ("pims Москва", "moscow_pims")
]
        # ("кафе Центральный административный округ", "moscow_cao"),
        # ("кафе Северо-Восточный административный округ", "moscow_svao"),
        # ("кафе Восточный административный округ", "moscow_vao"),
        # ("кафе Юго-Восточный административный округ", "moscow_yuvao"),

        # ("кафе Юго-Западный административный округ", "moscow_yuzao"),
        # ("кафе Северо-Западный административный округ", "moscow_szao"),
        # ("кафе Северный административный округ", "moscow_sao"),
        # ("кафе Западный административный округ", "moscow_zaо"),
        # ("кафе Зеленоградский административный округ", "moscow_zelenograd"),
        # ("кафе Троицкий административный округ", "moscow_troitsk"),
        # ("кафе Новомосковский административный округ", "moscow_novomoskovsk")
    
    for query, category in queries:
        print(f"\n=== Обрабатываем запрос: {query} ===")
        success = False
        
        for attempt in range(3):
            if scrape(query, category):
                success = True
                print(f"Данные по категории {category} сохранены в output_raw.csv")
                init_driver()  # Пересоздаем драйвер для следующего запроса
                break
            else:
                print(f"Попытка {attempt + 1} не удалась")
                time.sleep(10)
                
        if not success:
            print(f"Не удалось обработать запрос: {query} после 3 попыток")
            
        time.sleep(random.randint(15, 30))