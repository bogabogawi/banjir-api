import os
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# ======================
# Setup Selenium
# ======================
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(options=chrome_options)

today = datetime.now().strftime("%Y%m%d")
os.makedirs("data", exist_ok=True)

# ======================
# URL dictionary (Paras Air)
# ======================
urls_paras = {
    "Perlis": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/?state=PLS&district=ALL&station=ALL&lang=en",
    "Kedah": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/?state=KDH&district=ALL&station=ALL&lang=en",
    "Pulau Pinang": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/?state=PNG&district=ALL&station=ALL&lang=en",
    "Perak": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/?state=PRK&district=ALL&station=ALL&lang=en",
    "Selangor": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/?state=SGR&district=ALL&station=ALL&lang=en",
    "Negeri Sembilan": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/?state=NSN&district=ALL&station=ALL&lang=en",
    "Melaka": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/?state=MLK&district=ALL&station=ALL&lang=en",
    "Johor": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/?state=JHR&district=ALL&station=ALL&lang=en",
    "Pahang": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/?state=PHG&district=ALL&station=ALL&lang=en",
    "Terengganu": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/?state=TRG&district=ALL&station=ALL&lang=en",
    "Kelantan": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/?state=KTN&district=ALL&station=ALL&lang=en",
    "Sarawak": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/?state=SWK&district=ALL&station=ALL&lang=en",
    "Sabah": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/?state=SBH&district=ALL&station=ALL&lang=en",
    "WP Kuala Lumpur": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/?state=WPKL&district=ALL&station=ALL&lang=en",
    "WP Putrajaya": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/?state=WPPJ&district=ALL&station=ALL&lang=en",
    "WP Labuan": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/?state=WPLB&district=ALL&station=ALL&lang=en",
}

# ======================
# URL dictionary (Hujan)
# ======================
urls_hujan = {
    "Perlis": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/?state=PLS&district=ALL&station=ALL&lang=en",
    "Kedah": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/?state=KDH&district=ALL&station=ALL&lang=en",
    "Pulau Pinang": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/?state=PNG&district=ALL&station=ALL&lang=en",
    "Perak": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/?state=PRK&district=ALL&station=ALL&lang=en",
    "Selangor": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/?state=SGR&district=ALL&station=ALL&lang=en",
    "Negeri Sembilan": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/?state=NSN&district=ALL&station=ALL&lang=en",
    "Melaka": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/?state=MLK&district=ALL&station=ALL&lang=en",
    "Johor": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/?state=JHR&district=ALL&station=ALL&lang=en",
    "Pahang": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/?state=PHG&district=ALL&station=ALL&lang=en",
    "Terengganu": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/?state=TRG&district=ALL&station=ALL&lang=en",
    "Kelantan": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/?state=KTN&district=ALL&station=ALL&lang=en",
    "Sarawak": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/?state=SWK&district=ALL&station=ALL&lang=en",
    "Sabah": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/?state=SBH&district=ALL&station=ALL&lang=en",
    "WP Kuala Lumpur": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/?state=WPKL&district=ALL&station=ALL&lang=en",
    "WP Putrajaya": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/?state=WPPJ&district=ALL&station=ALL&lang=en",
    "WP Labuan": "https://publicinfobanjir.water.gov.my/hujan/data-hujan/?state=WPLB&district=ALL&station=ALL&lang=en",
}

# ======================
# Helper: Scrape table
# ======================
def scrape_table(state, url):
    try:
        driver.get(url)

        # cuba masuk iframe kalau ada
        try:
            iframe = driver.find_element(By.TAG_NAME, "iframe")
            driver.switch_to.frame(iframe)
        except:
            pass

        # cuba cari table id, kalau takde ambil <table> pertama
        try:
            table = driver.find_element(By.ID, "normaltable1")
        except:
            table = driver.find_element(By.TAG_NAME, "table")

        rows = table.find_elements(By.TAG_NAME, "tr")
        data = []
        for r in rows:
            cols = [c.text for c in r.find_elements(By.TAG_NAME, "td")]
            if cols:
                data.append(cols)

        driver.switch_to.default_content()

        if not data:
            print(f"[!] {state} tiada data")
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df["state"] = state
        print(f"[OK] {state} ({len(df)} rows)")
        return df

    except Exception as e:
        driver.switch_to.default_content()
        print(f"[X] {state} | Error: {e}")
        return pd.DataFrame()

# ======================
# Scrape Paras Air
# ======================
paras_data = []
for state, url in urls_paras.items():
    df = scrape_table(state, url)
    if not df.empty:
        paras_data.append(df)

if paras_data:
    df_paras = pd.concat(paras_data, ignore_index=True)
    df_paras.to_csv("data/paras_air.csv", index=False)
    df_paras.to_csv(f"data/paras_air_{today}.csv", index=False)
    print("✅ Paras Air data saved")
else:
    print("⚠️ Tiada data Paras Air disimpan")

# ======================
# Scrape Hujan
# ======================
hujan_data = []
for state, url in urls_hujan.items():
    df = scrape_table(state, url)
    if not df.empty:
        hujan_data.append(df)

if hujan_data:
    df_hujan = pd.concat(hujan_data, ignore_index=True)
    df_hujan.to_csv("data/hujan.csv", index=False)
    df_hujan.to_csv(f"data/hujan_{today}.csv", index=False)
    print("✅ Hujan data saved")
else:
    print("⚠️ Tiada data Hujan disimpan")

# ======================
# Tamat
# ======================
driver.quit()
print("🎉 Semua data berjaya diproses & disimpan dalam folder /data/")
