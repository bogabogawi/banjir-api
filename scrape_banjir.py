# scrape_banjir.py
import pandas as pd
import datetime
import requests

# =======================
# Config
# =======================
today = datetime.datetime.now().strftime("%Y%m%d")

# Semua negeri
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
    "Sarawak": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=SRK&district=ALL&station=ALL&lang=en",
    "Sabah": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=SBH&district=ALL&station=ALL&lang=en",
    "WP Kuala Lumpur": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=WKL&district=ALL&station=ALL&lang=en",
    "WP Putrajaya": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=WPT&district=ALL&station=ALL&lang=en",
    "WP Labuan": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=WPL&district=ALL&station=ALL&lang=en",
}

# =======================
# Scraper function
# =======================
def scrape_table(state, url):
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        dfs = pd.read_html(r.text)
        if len(dfs) == 0:
            print(f"[X] {state} | Tiada table")
            return pd.DataFrame()

        df = dfs[0]
        df["state"] = state
        print(f"[OK] {state} ({len(df)} rows)")
        return df
    except Exception as e:
        print(f"[X] {state} | Error: {e}")
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
    df_all = pd.concat(all_data, ignore_index=True)
    df_all.to_csv("data/paras_air.csv", index=False)
    df_all.to_csv(f"data/paras_air_{today}.csv", index=False)
    print("✅ Paras Air saved")
else:
    print("⚠️ Tiada data Paras Air")

print("🎉 Semua data berjaya diproses & disimpan dalam folder /data/")
