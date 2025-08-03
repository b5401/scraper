import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options

# --- ПУТЬ К CSV ---
CSV_FILE = 'output_raw.csv'

# --- НАСТРОЙКИ БРАУЗЕРА ---
HEADLESS = True  # Режим без интерфейса. True — для фоновой работы


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


def parse_contacts_for_link(driver, link, timeout=10):
    """
    Переходит по ссылке и пытается найти телефон, телеграм, вконтакте.
    :param driver: экземпляр драйвера
    :param link: ссылка на карточку организации
    :param timeout: время ожидания элемента
    :return: словарь с контактами: {'phone', 'telegram', 'vk'}
    """
    contacts = {
        'phone': None,
        'telegram': None,
        'vk': None,
    }

    try:
        print(f"[INFO] Открываем ссылку: {link}")
        # Открытие ссылки в новой вкладке
        driver.execute_script("window.open('');")
        driver.switch_to.window(driver.window_handles[1])
        driver.get(link)

        # Ждём загрузки страницы
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.TAG_NAME, 'body'))
        )

        # Поиск телефона
        try:
            phone_element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '.orgpage-phones-view__phone-number'))
            )
            contacts['phone'] = phone_element.text.strip()
            print(f"[PHONE] Найден: {contacts['phone']}")
        except TimeoutException:
            print(f"[PHONE] Не найден на странице: {link}")

        # Поиск соцсетей
        try:
            social_buttons = driver.find_elements(By.CSS_SELECTOR, ".business-contacts-view__social-button a.button._link")
            for btn in social_buttons:
                href = btn.get_attribute("href")
                aria_label = btn.get_attribute("aria-label").lower() if btn.get_attribute("aria-label") else ""

                if "telegram" in aria_label:
                    contacts['telegram'] = href
                    print(f"[TELEGRAM] Найден: {href}")
                elif "vkontakte" in aria_label or "vk" in aria_label:
                    contacts['vk'] = href
                    print(f"[VK] Найден: {href}")

        except Exception as e:
            print(f"[ERROR] Ошибка при парсинге соцсетей: {e}")

    except Exception as e:
        print(f"[ERROR] Не удалось обработать ссылку {link}: {e}")
    finally:
        # Закрываем вкладку
        driver.close()
        driver.switch_to.window(driver.window_handles[0])

    return contacts


def update_csv_with_contacts():
    """Основная функция: загрузка данных и парсинг контактов"""
    if not os.path.exists(CSV_FILE):
        print(f"[ERROR] Файл {CSV_FILE} не найден.")
        return

    print(f"[INFO] Загружаем данные из {CSV_FILE}...")
    # Загружаем данные
    df = pd.read_csv(CSV_FILE)

    # Проверяем наличие нужного столбца
    if 'link' not in df.columns:
        print("[ERROR] В CSV отсутствует столбец 'link'.")
        return

    # Добавляем новые столбцы, если их нет
    for col in ['phone', 'telegram', 'vk']:
        if col not in df.columns:
            df[col] = None

    # Инициализируем браузер
    driver = init_driver(headless=HEADLESS)
    updated = False

    print(f"[INFO] Начинаем парсинг контактов для {len(df)} организаций...")

    for idx, row in df.iterrows():
        link = row['link'].replace('reviews/', '')
        current_phone = row['phone']

        # # Пропускаем, если телефон уже есть
        # if pd.notna(current_phone):
        #     print(f"[SKIP] Уже есть данные для: {link}")
        #     continue

        print(f"[PROCESS] [{idx + 1}/{len(df)}] Парсим: {link}")
        contact_data = parse_contacts_for_link(driver, link)

        # Обновляем DataFrame
        for key in contact_data:
            df.at[idx, key] = contact_data[key]
        df.at[idx, 'link'] = link  # Сохраняем чистую ссылку
        updated = True

        # Сохранение после каждой строки (на случай ошибок)
        if updated:
            df.to_csv(CSV_FILE, index=False, encoding='utf-8')
            print(f"[SAVED] Данные для {link}: {contact_data}")
            updated = False

        time.sleep(2)  # Пауза между запросами

    driver.quit()
    print("[SUCCESS] Парсинг контактов завершён.")


if __name__ == "__main__":
    update_csv_with_contacts()