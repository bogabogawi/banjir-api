# scrape_banjir.py
import pandas as pd
import requests
import datetime
import time

# =======================
# Config
# =======================
today = datetime.datetime.now().strftime("%Y%m%d")

paras_air_urls = {
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
    "Wilayah Persekutuan Labuan": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=WLP&district=ALL&station=ALL&lang=en",
}

# =======================
# Scraper function
# =======================
def scrape_table(state, url, max_retries=3):
    for attempt in range(1, max_retries+1):
        try:
            r = requests.get(url, timeout=30, verify=False)  # disable SSL verify
            r.raise_for_status()

            dfs = pd.read_html(r.text)
            if len(dfs) == 0:
                raise Exception("Tiada jadual ditemui")

            df = dfs[0]
            df["state"] = state
            print(f"[OK] {state} ({len(df)} rows)")
            return df
        except Exception as e:
            print(f"[Retry {attempt}] {state} gagal | {e}")
            time.sleep(3)
    print(f"[X] {state} | Gagal selepas {max_retries} cubaan")
    return pd.DataFrame()

# =======================
# Main process
# =======================
all_data = []
for state, url in paras_air_urls.items():
    df = scrape_table(state, url)
    if not df.empty:
        all_data.append(df)

if all_data:
    df_air = pd.concat(all_data, ignore_index=True)
    df_air.to_csv("data/paras_air.csv", index=False)
    df_air.to_csv(f"data/paras_air_{today}.csv", index=False)
    print("✅ Paras Air saved")

    # =======================
    # Alert check
    # =======================
    try:
        alert_rows = df_air[
            pd.to_numeric(df_air["Water Level (m) (Graph)"], errors="coerce")
            > pd.to_numeric(df_air["Threshold.3"], errors="coerce")
        ]
        if len(alert_rows) > 0:
            alert_rows.to_csv(f"data/alert_{today}.csv", index=False)
            print(f"🚨 {len(alert_rows)} stesen melebihi paras bahaya!")
        else:
            print("✅ Tiada stesen melebihi paras bahaya")
    except Exception as e:
        print(f"⚠️ Error check alert: {e}")

else:
    print("⚠️ Tiada data Paras Air")

print("🎉 Semua data berjaya diproses & disimpan dalam folder /data/")
