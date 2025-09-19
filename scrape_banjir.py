# scrape_banjir.py
import pandas as pd
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# =======================
# Config
# =======================
today = datetime.datetime.now().strftime("%Y%m%d")

paras_air_urls = {
    "Perlis": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=PLS&district=ALL&station=ALL&lang=en",
    "Kedah": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=KDH&district=ALL&station=ALL&lang=en",
    "Pulau Pinang": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=PNG&district=ALL&station=ALL&lang=en",
    "Perak": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=PRK&district=ALL&station=ALL&lang=en",
    "Selangor": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=SGR&district=ALL&station=ALL&lang=en",
    "Negeri Sembilan": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=NSN&district=ALL&station=ALL&lang=en",
    "Melaka": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=MLK&district=ALL&station=ALL&lang=en",
    "Johor": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=JHR&district=ALL&station=ALL&lang=en",
    "Pahang": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=PHG&district=ALL&station=ALL&lang=en",
    "Terengganu": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=TRG&district=ALL&station=ALL&lang=en",
    "Kelantan": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=KTN&district=ALL&station=ALL&lang=en",
    "Sarawak": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=SWK&district=ALL&station=ALL&lang=en",
    "Sabah": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=SBH&district=ALL&station=ALL&lang=en",
    "WP Kuala Lumpur": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=WKL&district=ALL&station=ALL&lang=en",
    "WP Putrajaya": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=WPT&district=ALL&station=ALL&lang=en",
    "WP Labuan": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=WPL&district=ALL&station=ALL&lang=en",
}

hujan_urls = {
    "Perlis": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state=PLS&district=ALL&station=ALL&lang=en",
    "Kedah": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state=KDH&district=ALL&station=ALL&lang=en",
    "Pulau Pinang": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state=PNG&district=ALL&station=ALL&lang=en",
    "Perak": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state=PRK&district=ALL&station=ALL&lang=en",
    "Selangor": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state=SGR&district=ALL&station=ALL&lang=en",
    "Negeri Sembilan": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state=NSN&district=ALL&station=ALL&lang=en",
    "Melaka": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state=MLK&district=ALL&station=ALL&lang=en",
    "Johor": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state=JHR&district=ALL&station=ALL&lang=en",
    "Pahang": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state=PHG&district=ALL&station=ALL&lang=en",
    "Terengganu": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state=TRG&district=ALL&station=ALL&lang=en",
    "Kelantan": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state=KTN&district=ALL&station=ALL&lang=en",
    "Sarawak": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state=SWK&district=ALL&station=ALL&lang=en",
    "Sabah": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state=SBH&district=ALL&station=ALL&lang=en",
    "WP Kuala Lumpur": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state=WKL&district=ALL&station=ALL&lang=en",
    "WP Putrajaya": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state=WPT&district=ALL&station=ALL&lang=en",
    "WP Labuan": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state=WPL&district=ALL&station=ALL&lang=en",
}

# =======================
# Scraper function
# =======================
def scrape_table(driver, state, url):
    driver.get(url)
    try:
        # cuba masuk iframe dulu
        iframe = driver.find_element(By.TAG_NAME, "iframe")
        driver.switch_to.frame(iframe)
    except:
        pass  # kalau tiada iframe, biar direct

    try:
        # tunggu table betul² load (max 30s)
        table = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        df = pd.read_html(table.get_attribute("outerHTML"))[0]
        df["state"] = state
        print(f"[OK] {state} ({len(df)} rows)")
        return df
    except Exception as e:
        print(f"[X] {state} | Error: {e}")
        return pd.DataFrame()
    finally:
        driver.switch_to.default_content()

# =======================
# Main process
# =======================
options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Scrape Paras Air
paras_air_data = []
for state, url in paras_air_urls.items():
    df = scrape_table(driver, state, url)
    if not df.empty:
        paras_air_data.append(df)

if paras_air_data:
    df_air = pd.concat(paras_air_data, ignore_index=True)
    df_air.to_csv("data/paras_air.csv", index=False)
    df_air.to_csv(f"data/paras_air_{today}.csv", index=False)
    print("✅ Paras Air saved")
else:
    print("⚠️ Tiada data Paras Air")

# Scrape Hujan
hujan_data = []
for state, url in hujan_urls.items():
    df = scrape_table(driver, state, url)
    if not df.empty:
        hujan_data.append(df)

if hujan_data:
    df_hujan = pd.concat(hujan_data, ignore_index=True)
    df_hujan.to_csv("data/hujan.csv", index=False)
    df_hujan.to_csv(f"data/hujan_{today}.csv", index=False)
    print("✅ Hujan saved")
else:
    print("⚠️ Tiada data Hujan")

driver.quit()
print("🎉 Semua data berjaya diproses & disimpan dalam folder /data/")
