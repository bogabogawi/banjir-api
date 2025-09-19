import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

today = pd.Timestamp.today().strftime("%Y%m%d")
os.makedirs("data", exist_ok=True)

# ===============================
# Setup Selenium
# ===============================
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# ===============================
# 1. Paras Air (Selenium scrape table)
# ===============================
paras_air_states = {
    "Perlis": "PLS", "Kedah": "KDH", "Pulau Pinang": "PNG", "Perak": "PRK",
    "Selangor": "SEL", "Wilayah Persekutuan Kuala Lumpur": "WLH",
    "Negeri Sembilan": "NSN", "Melaka": "MLK", "Johor": "JHR",
    "Pahang": "PHG", "Terengganu": "TRG", "Kelantan": "KEL",
    "Sarawak": "SRK", "Sabah": "SAB", "Wilayah Persekutuan Labuan": "WLP"
}

paras_data = []
for state, code in paras_air_states.items():
    url = f"https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state={code}&district=ALL&station=ALL&lang=en"
    driver.get(url)
    time.sleep(3)  # bagi masa table load

    try:
        table = driver.find_element(By.ID, "normaltable1")
        rows = table.find_elements(By.TAG_NAME, "tr")
        data = [[cell.text for cell in row.find_elements(By.TAG_NAME, "td")] for row in rows if row.text.strip()]
        df = pd.DataFrame(data)
        df["state"] = state
        paras_data.append(df)
        print(f"[OK] paras_air - {state} ({len(df)} rows)")
    except Exception as e:
        print(f"[X] paras_air - {state} | Error: {e}")

if paras_data:
    df_paras = pd.concat(paras_data, ignore_index=True)
    df_paras.to_csv(f"data/paras_air_{today}.csv", index=False)
    df_paras.to_csv("data/paras_air.csv", index=False)  # versi latest
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

hujan_data = []
for state, code in hujan_states.items():
    url = f"https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state={code}&district=ALL&station=ALL&lang=en"
    driver.get(url)
    time.sleep(3)  # tunggu table load

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
    df_hujan.to_csv("data/hujan.csv", index=False)  # versi latest
    print("✅ Hujan data saved")

print("🎉 Semua data berjaya diproses & disimpan dalam folder /data/")
