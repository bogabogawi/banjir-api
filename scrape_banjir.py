import os
import time
import pandas as pd
import requests
import io
import urllib3
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By

# Disable SSL warning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

today = pd.Timestamp.today().strftime("%Y%m%d")
os.makedirs("data", exist_ok=True)

# ===============================
# 1. Paras Air (Scraping HTML API)
# ===============================
paras_air_states = {
    "Perlis": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=PLS&district=ALL&station=ALL&lang=en",
    "Kedah": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=KDH&district=ALL&station=ALL&lang=en",
    "Pulau Pinang": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=PNG&district=ALL&station=ALL&lang=en",
    "Perak": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=PRK&district=ALL&station=ALL&lang=en",
    "Selangor": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=SEL&district=ALL&station=ALL&lang=en",
    "Wilayah Persekutuan Kuala Lumpur": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=WLH&district=ALL&station=ALL&lang=en",
    "Negeri Sembilan": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=NSN&district=ALL&station=ALL&lang=en",
    "Melaka": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=MLK&district=ALL&station=ALL&lang=en",
    "Johor": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=JHR&district=ALL&station=ALL&lang=en",
    "Pahang": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=PHG&district=ALL&station=ALL&lang=en",
    "Terengganu": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=TRG&district=ALL&station=ALL&lang=en",
    "Kelantan": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=KEL&district=ALL&station=ALL&lang=en",
    "Sarawak": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=SRK&district=ALL&station=ALL&lang=en",
    "Sabah": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=SAB&district=ALL&station=ALL&lang=en",
    "Wilayah Persekutuan Labuan": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=WLP&district=ALL&station=ALL&lang=en"
}

paras_data = []
for state, url in paras_air_states.items():
    try:
        # Force skip SSL verify (selamat utk data public)
        r = requests.get(url, verify=False, timeout=30)
        tables = pd.read_html(io.StringIO(r.text))
        df = tables[0]
        df["state"] = state
        paras_data.append(df)
        print(f"[OK] paras_air - {state} ({len(df)} rows)")
    except Exception as e:
        print(f"[X] paras_air - {state} | Error: {e}")

if paras_data:
    df_paras = pd.concat(paras_data, ignore_index=True)
    df_paras.to_csv(f"data/paras_air_{today}.csv", index=False)
    print("✅ Paras Air data saved")

# ===============================
# 2. Hujan (Selenium scrape table)
# ===============================
hujan_states = {
    "Perlis": "PLS", "Kedah": "KDH", "Pulau Pinang": "PNG", "Perak": "PRK",
    "Selangor": "SEL", "Johor": "JHR", "Pahang": "PHG", "Terengganu": "TRG",
    "Kelantan": "KEL", "Sarawak": "SRK", "Sabah": "SAB",
    "Wilayah Persekutuan Kuala Lumpur": "WPK", "Wilayah Persekutuan Labuan": "WLP"
}

options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

hujan_data = []
for state, code in hujan_states.items():
    url = f"https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state={code}&district=ALL&station=ALL&lang=en"
    driver.get(url)
    time.sleep(2)  # tunggu table load

    try:
        table = driver.find_element(By.ID, "normaltable1")
        rows = table.find_elements(By.TAG_NAME, "tr")
        data = [[cell.text for cell in row.find_elements(By.TAG_NAME, "td")] for row in rows if row.text.strip()]
        df = pd.DataFrame(data)
        df["state"] = state
        hujan_data.append(df)
        print(f"[OK] hujan - {state} ({len(df)} rows)")
    except Exception as e:
        print(f"[X] hujan - {state} | Error: {e}")

driver.quit()

if hujan_data:
    df_hujan = pd.concat(hujan_data, ignore_index=True)
    df_hujan.to_csv(f"data/hujan_{today}.csv", index=False)
    print("✅ Hujan data saved")

print("🎉 Semua data berjaya diproses & disimpan dalam folder /data/")
