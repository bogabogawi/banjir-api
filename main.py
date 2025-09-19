from fastapi import FastAPI
import pandas as pd

app = FastAPI()

@app.get("/")
def home():
    return {"message": "🌊 Banjir API - guna /alert atau /rain"}

# ======================
# ALERT (Paras Air)
# ======================
@app.get("/alert")
def get_alert(state: str = None):
    try:
        df = pd.read_csv("data/paras_air.csv")

        # Buang row kosong
        df = df.dropna(how="all")

        # Pastikan kolum Danger ada
        if "Danger" not in df.columns:
            return {"error": f"Kolum 'Danger' tak jumpa. Header: {list(df.columns)}"}

        # Tukar string ke nombor
        df["Water Level (m) (Graph)"] = pd.to_numeric(df["Water Level (m) (Graph)"], errors="coerce")
        df["Danger"] = pd.to_numeric(df["Danger"], errors="coerce")

        # Cari station melebihi paras bahaya
        alerts = df[df["Water Level (m) (Graph)"] >= df["Danger"]]

        # Filter ikut negeri
        if state:
            alerts = alerts[alerts["state"].str.lower() == state.lower()]

        if alerts.empty:
            return {"message": f"Tiada data amaran untuk {state if state else 'mana-mana negeri'}"}

        return alerts.to_dict(orient="records")

    except Exception as e:
        return {"error": str(e)}


# ======================
# HUJAN
# ======================
@app.get("/rain")
def get_rain(state: str = None):
    try:
        df = pd.read_csv("data/hujan.csv")
        df = df.dropna(how="all")

        if state:
            df = df[df["state"].str.lower() == state.lower()]

        if df.empty:
            return {"message": f"Tiada data hujan untuk {state if state else 'mana-mana negeri'}"}

        return df.to_dict(orient="records")

    except Exception as e:
        return {"error": str(e)}
