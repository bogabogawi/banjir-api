from fastapi import FastAPI
import pandas as pd

app = FastAPI()

@app.get("/")
def home():
    return {"message": "🚨 Banjir API - gunakan /alert atau /rain"}

# =========================
# ALERT (paras air)
# =========================
@app.get("/alert")
def get_alert(state: str = None):
    try:
        df = pd.read_csv("data/paras_air.csv")

        # Buang row NaN
        df = df.dropna()

        # Pastikan kolum Danger wujud
        if "Danger" not in df.columns:
            return {"error": f"Kolum 'Danger' tak jumpa. Header: {list(df.columns)}"}

        # Tukar numeric
        df["Water Level (m) (Graph)"] = pd.to_numeric(df["Water Level (m) (Graph)"], errors="coerce")
        df["Danger"] = pd.to_numeric(df["Danger"], errors="coerce")

        # Filter ikut negeri
        if state:
            df = df[df["state"].str.lower() == state.lower()]

        # Kalau kosong
        if df.empty:
            return {"message": f"Tiada data untuk negeri {state if state else 'yang diminta'}"}

        # Cari bacaan lebih threshold
        alerts = df[df["Water Level (m) (Graph)"] >= df["Danger"]]

        # Kalau tak ada alert walaupun ada data
        if alerts.empty:
            return {"message": f"Tiada amaran banjir untuk negeri {state if state else 'yang diminta'}"}

        return alerts.replace({float("inf"): None, -float("inf"): None}).to_dict(orient="records")

    except Exception as e:
        return {"error": str(e)}

# =========================
# HUJAN
# =========================
@app.get("/rain")
def get_rain(state: str = None):
    try:
        df = pd.read_csv("data/hujan.csv")

        # Buang row NaN
        df = df.dropna()

        # Filter ikut negeri
        if state:
            df = df[df["state"].str.lower() == state.lower()]

        # Kalau kosong
        if df.empty:
            return {"message": f"Tiada data hujan untuk negeri {state if state else 'yang diminta'}"}

        return df.replace({float("inf"): None, -float("inf"): None}).to_dict(orient="records")

    except Exception as e:
        return {"error": str(e)}
