from fastapi import FastAPI
import pandas as pd

app = FastAPI()

@app.get("/")
def home():
    return {"message": "🚨 Banjir API → guna /alert"}

# ==========================
# ALERT (Paras Air)
# ==========================
@app.get("/alert")
def get_alert(state: str = None):
    try:
        df = pd.read_csv("data/paras_air.csv")

        # Pastikan semua nama kolum konsisten
        df.columns = df.columns.str.strip()

        if state:
            df = df[df["state"].str.lower() == state.lower()]

        if df.empty:
            return {"message": f"Tiada data untuk {state}"}

        # Buat filter alert
        if "Water Level (m) (Graph)" in df.columns and "Threshold" in df.columns:
            # ambil column danger
            df["Alert"] = df.apply(
                lambda row: "⚠️ BAHAYA" if float(str(row["Water Level (m) (Graph)"]).replace("-", "0")) > float(str(row["Threshold.3"]).replace("-", "0")) else "OK",
                axis=1
            )
            alerts = df[df["Alert"] == "⚠️ BAHAYA"]
            return alerts.to_dict(orient="records")

        else:
            return {"error": f"Kolum tak jumpa. Header: {list(df.columns)}"}

    except Exception as e:
        return {"error": str(e)}
