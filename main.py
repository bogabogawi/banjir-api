from fastapi import FastAPI, Query
import pandas as pd
import glob
import os

app = FastAPI()

# Helper: cari fail terbaru ikut prefix
def get_latest_csv(prefix):
    files = glob.glob(f"data/{prefix}_*.csv")
    if not files:
        return None
    return max(files, key=os.path.getmtime)

@app.get("/")
def home():
    return {"message": "Banjir API (FastAPI) running 🚀"}

@app.get("/alert")
def get_alert(state: str = Query(None)):
    paras_file = get_latest_csv("paras_air")
    if not paras_file:
        return {"error": "No paras_air data found"}

    try:
        df = pd.read_csv(paras_file)
        df = df.dropna()
        df.columns = [c.strip() for c in df.columns]  # normalize header

        # Cuba detect kolum paras & threshold
        possible_level_cols = [c for c in df.columns if "Level" in c and "(m)" in c]
        if len(possible_level_cols) < 2:
            return {"error": f"Kolum threshold tak jumpa. Header: {list(df.columns)}"}

        # Biasanya: Level (m), Alert Level (m), Warning Level (m), Danger Level (m)
        level_col = "Level (m)"
        danger_col = "Danger Level (m)" if "Danger Level (m)" in df.columns else possible_level_cols[-1]

        alerts = df[df[level_col] >= df[danger_col]]

        if state:
            alerts = alerts[alerts["state"].str.lower() == state.lower()]

        return alerts.to_dict(orient="records") if not alerts.empty else {"message": "tiada alert"}

    except Exception as e:
        return {"error": str(e)}
