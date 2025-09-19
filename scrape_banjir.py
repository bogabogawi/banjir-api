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
# 1. Paras Air (iframe + Selenium)
# ===============================
paras_air_states = {
    "Perlis": "PLS", "Kedah": "KDH", "Pulau Pinang": "PNG", "Perak": "PRK",
    "Selangor": "SEL", "Wilayah Persekutuan Kuala Lumpur": "WPK",
    "Negeri Sembilan": "NSN", "Melaka": "MLK", "Johor": "JHR",
    "Pahang": "PHG", "Terengganu": "TRG", "Kelantan": "KEL",
    "Sarawak": "SRK", "Sabah": "SAB", "Wilayah Persekutuan Labuan": "WLP"
}

options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

paras_data = []
for state, code in paras_air_states.items():
    url = f"https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state={code}&district=ALL&station=ALL&lang=en"
    driver.get(url)
    time.sleep(3)

    # ...
try:
    # Cuba cari iframe
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    if iframes:
        driver.switch_to.frame(iframes[0])  # masuk iframe pertama
        print(f"🔎 {state}: masuk iframe")
    else:
        print(f"ℹ️ {state}: tiada iframe, scrape direct")

    # scrape table (dalam iframe atau direct)
    table = driver.find_element(By.ID, "normaltable1")
    rows = table.find_elements(By.TAG_NAME, "tr")
    data = [[cell.text for cell in row.find_elements(By.TAG_NAME, "td")] for row in rows if row.text.strip()]
    df = pd.DataFrame(data)
    df["state"] = state
    paras_data.append(df)

    print(f"[OK] paras_air - {state} ({len(df)} rows)")

    driver.switch_to.default_content()

except Exception as e:
    print(f"[X] paras_air - {state} | Error: {e}")
    driver.switch_to.default_content()


# Standardize header
if paras_data:
    df_paras = pd.concat(paras_data, ignore_index=True)
    expected_cols = [
        "Station", "River", "District", "Level (m)",
        "Normal Level (m)", "Alert Level (m)",
        "Warning Level (m)", "Danger Level (m)", "state"
    ]
    real_cols = len(df_paras.columns) - 1  # exclude state
    rename_cols = expected_cols[:real_cols] + ["state"]
    df_paras.columns = rename_cols

    df_paras.to_csv("data/paras_air.csv", index=False)
    df_paras.to_csv(f"data/paras_air_{today}.csv", index=False)
    print("✅ Paras Air data saved")

# ===============================
# 2. Hujan (Selenium scrape table)
# ===============================
hujan_states = paras_air_states  # sama dict

hujan_data = []
for state, code in hujan_states.items():
    url = f"https://publicinfobanjir.water.gov.my/hujan/data-hujan/data-hujan-lanjutan/?state={code}&district=ALL&station=ALL&lang=en"
    driver.get(url)
    time.sleep(3)

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

# Standardize header hujan
if hujan_data:
    df_hujan = pd.concat(hujan_data, ignore_index=True)
    expected_hujan_cols = [
        "Station ID", "Station Name", "District", "Datetime",
        "1 hour (mm)", "3 hour (mm)", "6 hour (mm)", "12 hour (mm)",
        "24 hour (mm)", "state"
    ]
    real_cols = len(df_hujan.columns) - 1
    rename_cols = expected_hujan_cols[:real_cols] + ["state"]
    df_hujan.columns = rename_cols

    df_hujan.to_csv("data/hujan.csv", index=False)
    df_hujan.to_csv(f"data/hujan_{today}.csv", index=False)
    print("✅ Hujan data saved")

print("🎉 Semua data berjaya diproses & disimpan dalam folder /data/")

