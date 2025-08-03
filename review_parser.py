import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, ElementClickInterceptedException


# --- ПУТЬ К CSV ---
CSV_FILE = 'output_raw.csv'

# --- НАСТРОЙКИ БРАУЗЕРА ---
HEADLESS = True  # Режим без интерфейса. True — для фоновой работы
REVIEWS_PER_CATEGORY = 5  # Сколько отзывов собирать в каждой категории
SCROLL_PAUSE = 3  # Пауза после прокрутки страницы


def init_driver(headless=True):
    """Инициализация Selenium WebDriver"""
    print("[INFO] Инициализируем браузер...")
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--start-maximized")
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

    driver = webdriver.Chrome(options=chrome_options)
    return driver

def click_filter_button(driver, label, timeout=15, max_retries=5):
    """
    Открывает выпадающий список и выбирает нужный фильтр.
    С улучшенным ожиданием и проверкой наличия списка.
    """
    for attempt in range(max_retries):
        try:
            print(f"[FILTER] Попытка {attempt + 1}/{max_retries} для фильтра '{label}'")
            
            # 1. Находим и кликаем кнопку фильтрации
            dropdown_button = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".rating-ranking-view[role='button']")))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", dropdown_button)
            time.sleep(1)
            
            try:
                dropdown_button.click()
            except ElementClickInterceptedException:
                driver.execute_script("arguments[0].click();", dropdown_button)
            
            print(f"[FILTER] Открыли выпадающий список")
            time.sleep(2)  # Даём время списку появиться

            # 2. Ждём появления меню фильтров
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".rating-ranking-view__popup"))
            )

            # 3. Ищем нужный фильтр с защитой от устаревания элементов
            filter_xpath = f'//div[@class="rating-ranking-view__popup-line" and normalize-space()="{label}"]'
            
            # Обновляем поиск элементов при каждой попытке
            filter_options = WebDriverWait(driver, timeout).until(
                EC.presence_of_all_elements_located((By.XPATH, filter_xpath))
            )
            
            if not filter_options:
                print(f"[WARNING] Элемент '{label}' не найден в выпадающем списке")
                return False

            # Выбираем первый подходящий элемент
            filter_option = filter_options[0]
            
            # Прокручиваем к элементу и кликаем
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", filter_option)
            time.sleep(1)
            
            try:
                filter_option.click()
            except ElementClickInterceptedException:
                driver.execute_script("arguments[0].click();", filter_option)

            print(f"[FILTER] Успешно выбрали фильтр: {label}")
            time.sleep(3)  # Ждём прогрузки отзывов
            return True

        except StaleElementReferenceException:
            print(f"[RETRY] Элемент устарел, пробуем снова...")
            time.sleep(3)
        except TimeoutException:
            print(f"[RETRY] Таймаут при ожидании элементов, пробуем снова...")
            time.sleep(3)
        except Exception as e:
            print(f"[RETRY] Ошибка: {str(e)}, пробуем снова...")
            time.sleep(3)

    print(f"[ERROR] Не удалось выбрать фильтр '{label}' после {max_retries} попыток.")
    return False

def collect_reviews(driver, max_reviews=5, timeout=10):
    """Собирает отзывы со страницы"""
    reviews = []
    scroll_attempts = 0
    max_scroll_attempts = 5

    while len(reviews) < max_reviews and scroll_attempts < max_scroll_attempts:
        review_elements = WebDriverWait(driver, timeout).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "span.spoiler-view__text-container"))
        )

        for el in review_elements:
            text = el.text.strip()
            if text and text not in reviews:
                reviews.append(text)
                if len(reviews) >= max_reviews:
                    break

        if len(reviews) < max_reviews:
            # Прокручиваем вниз
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE)
            scroll_attempts += 1
            print(f"[SCROLL] Прокрутили вниз ({scroll_attempts}/{max_scroll_attempts}), найдено отзывов: {len(reviews)}")
        else:
            break

    return reviews[:max_reviews]


def parse_reviews_for_link(driver, link, timeout=10):
    """
    Переходит по ссылке /reviews и собирает по 5 отзывов: сначала отрицательные, потом положительные
    :param driver: экземпляр драйвера
    :param link: ссылка на карточку организации
    :return: {'negative': str, 'positive': str}
    """

    result = {
        'negative': '',
        'positive': ''
    }

    # Добавляем '/reviews/', если её нет в ссылке
    if not link.endswith('/reviews/'):
        full_link = link.rstrip('/') + '/reviews/'
    else:
        full_link = link

    try:
        print(f"[INFO] Открываем ссылку: {full_link}")
        driver.get(full_link)

        # Ждём загрузки страницы
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.TAG_NAME, 'body'))
        )

        # --- ОТРИЦАТЕЛЬНЫЕ ОТЗЫВЫ ---
        print("[NEGATIVE] Загружаем отрицательные отзывы...")
        if click_filter_button(driver, "Сначала отрицательные"):
            negative_reviews = collect_reviews(driver, REVIEWS_PER_CATEGORY)
            result['negative'] = " -_- ".join(negative_reviews)
            print(f"[NEGATIVE] Получено: {len(negative_reviews)}")

        # --- ПОЛОЖИТЕЛЬНЫЕ ОТЗЫВЫ ---
        print("[POSITIVE] Загружаем положительные отзывы...")
        if click_filter_button(driver, "Сначала положительные"):
            positive_reviews = collect_reviews(driver, REVIEWS_PER_CATEGORY)
            result['positive'] = " -_- ".join(positive_reviews)
            print(f"[POSITIVE] Получено: {len(positive_reviews)}")

    except Exception as e:
        print(f"[ERROR] Не удалось обработать ссылку {full_link}: {e}")

    return result


def update_csv_with_reviews():
    """Основная функция: загрузка данных и парсинг отзывов"""
    if not os.path.exists(CSV_FILE):
        print(f"[ERROR] Файл {CSV_FILE} не найден.")
        return

    print(f"[INFO] Загружаем данные из {CSV_FILE}...")
    df = pd.read_csv(CSV_FILE)

    if 'link' not in df.columns:
        print("[ERROR] В CSV отсутствует столбец 'link'.")
        return

    # Добавляем новые столбцы
    for col in ['negative', 'positive']:
        if col not in df.columns:
            df[col] = ''

    driver = init_driver(headless=HEADLESS)
    updated = False

    print(f"[INFO] Начинаем парсинг отзывов для {len(df)} организаций...")

    for idx, row in df.iterrows():
        link = row['link'].rstrip('/')
        print(f"\n[PROCESS] [{idx + 1}/{len(df)}] Парсим: {link}")

        # Если уже есть данные, пропускаем
        if pd.notna(row['negative']) and pd.notna(row['positive']):
            print(f"[SKIP] Уже есть данные для: {link}")
            continue

        try:
            review_data = parse_reviews_for_link(driver, link)
            df.at[idx, 'negative'] = review_data['negative']
            df.at[idx, 'positive'] = review_data['positive']
            updated = True
        except Exception as e:
            print(f"[ERROR] Критическая ошибка при обработке ссылки '{link}': {e}")
            df.at[idx, 'negative'] = '[ERROR]'
            df.at[idx, 'positive'] = '[ERROR]'
            updated = True

        # Сохраняем после каждой строки
        if updated:
            df.to_csv(CSV_FILE, index=False, encoding='utf-8')
            print(f"[SAVED] Данные для {link}")
            updated = False

        time.sleep(3)  # Анти-бан

    driver.quit()
    print("[SUCCESS] Парсинг отзывов завершён.")


if __name__ == "__main__":
    update_csv_with_reviews()