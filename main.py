# main.py
from fastapi import FastAPI, Query
import pandas as pd
import os

app = FastAPI()

DATA_DIR = "data"

def load_csv(filename):
    path = os.path.join(DATA_DIR, filename)
    if os.path.exists(path):
        df = pd.read_csv(path)
        return df
    return pd.DataFrame()

@app.get("/alert")
def get_alert(state: str = None):
    try:
        df = load_csv("paras_air.csv")
        if df.empty:
            return {"error": "Tiada data paras_air.csv"}

        # cari kolum threshold yang wujud
        threshold_cols = [c for c in df.columns if "Danger" in c or "Bahaya" in c]
        if not threshold_cols:
            return {"error": f"Kolum threshold tak jumpa. Header: {list(df.columns)}"}

        danger_col = threshold_cols[0]

        # filter ikut threshold "Danger"
        alerts = df[df[danger_col].notna()]

        # ikut state kalau diberi
        if state:
            alerts = alerts[alerts["state"].str.lower() == state.lower()]

        return alerts.to_dict(orient="records")

    except Exception as e:
        return {"error": str(e)}

@app.get("/rain")
def get_rain(state: str = None):
    try:
        df = load_csv("hujan.csv")
        if df.empty:
            return {"error": "Tiada data hujan.csv"}

        # kalau ada state filter
        if state:
            df = df[df["state"].str.lower() == state.lower()]

        return df.to_dict(orient="records")

    except Exception as e:
        return {"error": str(e)}
