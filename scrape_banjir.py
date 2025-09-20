#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper paras air Malaysia (JPS)
- Support JSON + HTML table
- Simpan semua dalam data/{state}.json
"""

import os
import json
import requests
from bs4 import BeautifulSoup

# Senarai negeri & URL
URLS = {
    "PLS": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=PLS&district=ALL&station=ALL&lang=en",
    "KDH": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=KDH&district=ALL&station=ALL&lang=en",
    "PNG": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=PNG&district=ALL&station=ALL&lang=en",
    "PRK": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=PRK&district=ALL&station=ALL&lang=en",
    "SGR": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=SGR&district=ALL&station=ALL&lang=en",

    "MLK": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air-data/?state=MLK&district=ALL&station=ALL&lang=en",
    "JHR": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air-data/?state=JHR&district=ALL&station=ALL&lang=en",
    "PHG": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air-data/?state=PHG&district=ALL&station=ALL&lang=en",
    "TRG": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air-data/?state=TRG&district=ALL&station=ALL&lang=en",
    "KTN": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air-data/?state=KTN&district=ALL&station=ALL&lang=en",
    "SWK": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air-data/?state=SWK&district=ALL&station=ALL&lang=en",
    "SBH": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air-data/?state=SBH&district=ALL&station=ALL&lang=en",
    "LBN": "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air-data/?state=LBN&district=ALL&station=ALL&lang=en",
}


def parse_html_table(html: str, state: str):
    """Parse HTML table → list of dict"""
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr")
    data = []

    for row in rows[1:]:  # skip header
        cols = [c.get_text(strip=True) for c in row.find_all("td")]
        if len(cols) >= 11:
            data.append({
                "station_id": cols[0],
                "name": cols[1],
                "district": cols[2],
                "river": cols[3],
                "sub_river": cols[4],
                "last_updated": cols[5],
                "level": cols[6],
                "thresholds": {
                    "normal": cols[7],
                    "alert": cols[8],
                    "warning": cols[9],
                    "danger": cols[10],
                },
                "source_state": state
            })
    return data


def fetch_state(state: str, url: str):
    """Fetch satu negeri, detect JSON / HTML"""
    print(f"📡 Fetch {state} ...", end=" ")
    try:
        r = requests.get(url, timeout=20, verify=False)
        text = r.text

        # Cuba JSON dulu
        try:
            j = r.json()
            data = j.get("data", [])
            print(f"✅ JSON ({len(data)} rekod)")
            return data
        except Exception:
            parsed = parse_html_table(text, state)
            print(f"✅ HTML ({len(parsed)} rekod)")
            return parsed

    except Exception as e:
        print(f"❌ Error: {e}")
        return []


def main():
    os.makedirs("data", exist_ok=True)
    all_data = {}

    for state, url in URLS.items():
        data = fetch_state(state, url)
        all_data[state] = data

        out_file = os.path.join("data", f"{state}.json")
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    # Simpan gabungan semua
    with open("data/all_states.json", "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    print("🎉 Semua negeri siap! Data disimpan dalam folder /data/")


if __name__ == "__main__":
    main()
