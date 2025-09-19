import os
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ----------------------------
# Setup Selenium
# ----------------------------
options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# ----------------------------
# URL negeri
# ----------------------------
paras_air_urls = {
    "Perlis": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/data-paras-air.cfm?state=PLS&district=ALL&station=ALL&lang=en",
    "Kedah": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/data-paras-air.cfm?state=KDH&district=ALL&station=ALL&lang=en",
    "Pulau Pinang": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/data-paras-air.cfm?state=PNG&district=ALL&station=ALL&lang=en",
    "Perak": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/data-paras-air.cfm?state=PRK&district=ALL&station=ALL&lang=en",
    "Selangor": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/data-paras-air.cfm?state=SGR&district=ALL&station=ALL&lang=en",
    "Negeri Sembilan": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/data-paras-air.cfm?state=NSN&district=ALL&station=ALL&lang=en",
    "Melaka": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/data-paras-air.cfm?state=MLK&district=ALL&station=ALL&lang=en",
    "Johor": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/data-paras-air.cfm?state=JHR&district=ALL&station=ALL&lang=en",
    "Pahang": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/data-paras-air.cfm?state=PHG&district=ALL&station=ALL&lang=en",
    "Terengganu": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/data-paras-air.cfm?state=TRG&district=ALL&station=ALL&lang=en",
    "Kelantan": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/data-paras-air.cfm?state=KTN&district=ALL&station=ALL&lang=en",
    "Sabah": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/data-paras-air.cfm?state=SBH&district=ALL&station=ALL&lang=en",
    "Sarawak": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/data-paras-air.cfm?state=SWK&district=ALL&station=ALL&lang=en",
    "Wilayah Persekutuan Kuala Lumpur": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/data-paras-air.cfm?state=WPKL&district=ALL&station=ALL&lang=en",
    "Wilayah Persekutuan Putrajaya": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/data-paras-air.cfm?state=WPPJ&district=ALL&station=ALL&lang=en",
    "Wilayah Persekutuan Labuan": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/data-paras-air.cfm?state=WPLB&district=ALL&station=ALL&lang=en",
}

hujan_urls = {
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

# ----------------------------
# Scraper function
# ----------------------------
def scrape_table(state, url):
    try:
        driver.get(url)

        # cuba masuk iframe
        try:
            iframe = driver.find_element(By.TAG_NAME, "iframe")
            driver.switch_to.frame(iframe)
        except:
            pass

        # tunggu table
        try:
            table = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "normaltable1"))
            )
        except:
            try:
                table = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "table"))
                )
            except:
                print(f"[!] {state} tiada table")
                return pd.DataFrame()

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

# ----------------------------
# Main run
# ----------------------------
today = datetime.today().strftime("%Y%m%d")

paras_data = []
for state, url in paras_air_urls.items():
    paras_data.append(scrape_table(state, url))

hujan_data = []
for state, url in hujan_urls.items():
    hujan_data.append(scrape_table(state, url))

# ----------------------------
# Save CSV
# ----------------------------
os.makedirs("data", exist_ok=True)

if paras_data:
    df_paras = pd.concat(paras_data, ignore_index=True)
    df_paras.to_csv("data/paras_air.csv", index=False)
    df_paras.to_csv(f"data/paras_air_{today}.csv", index=False)
    print("✅ Paras Air saved")
else:
    print("⚠️ Tiada data Paras Air")

if hujan_data:
    df_hujan = pd.concat(hujan_data, ignore_index=True)
    df_hujan.to_csv("data/hujan.csv", index=False)
    df_hujan.to_csv(f"data/hujan_{today}.csv", index=False)
    print("✅ Hujan saved")
else:
    print("⚠️ Tiada data Hujan")

driver.quit()

print("🎉 Semua data berjaya diproses & disimpan dalam folder /data/")
