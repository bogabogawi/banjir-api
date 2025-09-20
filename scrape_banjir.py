# scrape_banjir.py
import pandas as pd
import datetime
import requests
import time
import ssl

# =======================
# Custom SSL context
# =======================
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE
# 👇 benarkan legacy renegotiation
try:
    ssl_context.options |= 0x4  # SSL_OP_LEGACY_SERVER_CONNECT
except Exception:
    pass

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
    "Sarawak": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=SRK&district=ALL&station=ALL&lang=en",
    "Sabah": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=SBH&district=ALL&station=ALL&lang=en",
    "WP Kuala Lumpur": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=WKL&district=ALL&station=ALL&lang=en",
    "WP Putrajaya": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=WPT&district=ALL&station=ALL&lang=en",
    "WP Labuan": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=WPL&district=ALL&station=ALL&lang=en",
}

# =======================
# Scraper function
# =======================
def scrape_table(state, url, max_retries=3):
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, timeout=30, verify=False)  # skip verify
            r.raise_for_status()
            dfs = pd.read_html(r.text)
            if not dfs:
                raise Exception("Tiada table")
            df = dfs[0]
            df["state"] = state
            print(f"[OK] {state} ({len(df)} rows)")
            return df
        except Exception as e:
            print(f"[Retry {attempt}] {state} gagal | {e}")
            time.sleep(5)
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
    df_all = pd.concat(all_data, ignore_index=True)

    # ============ ALERT CHECK ============
    alert_rows = []
    if "Water Level (m) (Graph)" in df_all.columns and "Danger" in df_all.columns:
        try:
            df_all["Water Level (m) (Graph)"] = pd.to_numeric(df_all["Water Level (m) (Graph)"], errors="coerce")
            df_all["Danger"] = pd.to_numeric(df_all["Danger"], errors="coerce")
            alert_rows = df_all[df_all["Water Level (m) (Graph)"] > df_all["Danger"]]
        except Exception as e:
            print("⚠️ Error semasa kira alert:", e)

    # Simpan data
    df_all.to_csv("data/paras_air.csv", index=False)
    df_all.to_csv(f"data/paras_air_{today}.csv", index=False)
    print("✅ Paras Air saved")

    # Papar alert
    if len(alert_rows) > 0:
        print("🚨 ALERT! Stesen melebihi paras bahaya:")
        print(alert_rows[["Station ID", "Station Name", "state", "Water Level (m) (Graph)", "Danger"]])
        # Simpan alert
        alert_rows.to_csv(f"data/alert_{today}.csv", index=False)
    else:
        print("✅ Tiada stesen melebihi paras bahaya")
else:
    print("⚠️ Tiada data Paras Air")

print("🎉 Semua data berjaya diproses & disimpan dalam folder /data/")
