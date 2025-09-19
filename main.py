from fastapi import FastAPI, Query
import pandas as pd

app = FastAPI()

@app.get("/alert")
def get_alert(state: str = None):
    try:
        # Baca CSV paras air
        df = pd.read_csv("data/paras_air.csv")

        # Buang row NaN supaya JSON valid
        df = df.dropna()

        # Pastikan column "Danger" wujud
        if "Danger" not in df.columns:
            return {"error": f"Kolum 'Danger' tak jumpa. Header: {list(df.columns)}"}

        # Cari bacaan yang cecah / lebih daripada threshold danger
        df["Water Level (m) (Graph)"] = pd.to_numeric(df["Water Level (m) (Graph)"], errors="coerce")
        df["Danger"] = pd.to_numeric(df["Danger"], errors="coerce")

        alerts = df[df["Water Level (m) (Graph)"] >= df["Danger"]]

        # Filter ikut negeri
        if state:
            alerts = alerts[alerts["state"].str.lower() == state.lower()]

        # Tukar ke JSON
        result = alerts.replace([float("inf"), -float("inf")], None).to_dict(orient="records")
        return result

    except Exception as e:
        return {"error": str(e)}

@app.get("/rain")
def get_rain(state: str = None):
    try:
        # Baca CSV hujan
        df = pd.read_csv("data/hujan.csv")
        df = df.dropna()

        # Contoh: ambil bacaan hujan hari ni (col terakhir biasanya latest)
        last_col = df.columns[-2]  # sebelum "state"
        df[last_col] = pd.to_numeric(df[last_col], errors="coerce")

        alerts = df[df[last_col] > 50]   # kalau lebih 50mm (heavy rain)

        if state:
            alerts = alerts[alerts["state"].str.lower() == state.lower()]

        result = alerts.replace([float("inf"), -float("inf")], None).to_dict(orient="records")
        return result

    except Exception as e:
        return {"error": str(e)}
