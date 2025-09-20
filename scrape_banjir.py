#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parser untuk fail HTML (download via curl) → JSON
"""

import os
import json
from bs4 import BeautifulSoup

RAW_DIR = "raw"
OUT_DIR = "data"

URLS = {
    "MLK": "Melaka",
    "JHR": "Johor",
    "PHG": "Pahang",
    "TRG": "Terengganu",
    "KTN": "Kelantan",
    "SWK": "Sarawak",
    "SBH": "Sabah",
    "LBN": "Labuan",
}

def parse_html_table(file_path, state):
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        html = f.read()
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr")
    data = []

    for row in rows[1:]:
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

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    all_data = {}

    for code, state in URLS.items():
        file_path = os.path.join(RAW_DIR, f"{code}.html")
        if not os.path.exists(file_path):
            print(f"❌ Skip {state}, file tiada")
            continue
        print(f"📄 Parse {state}...")
        data = parse_html_table(file_path, state)
        all_data[code] = data

        out_file = os.path.join(OUT_DIR, f"{code}.json")
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    # gabungan semua
    out_file = os.path.join(OUT_DIR, "all_states.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    print("🎉 Semua siap → data/*.json")

if __name__ == "__main__":
    main()
