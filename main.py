from fastapi import FastAPI, Query
import pandas as pd
import math

app = FastAPI()

@app.get("/alert")
def get_alert(state: str = None):
    try:
        df = pd.read_csv("data/paras_air.csv")

        # Buang row yang ada NaN supaya JSON valid
        df = df.dropna()

        # Filter ikut Bahaya
        alerts = df[df["Threshold3"] == "Bahaya"]

        # Kalau filter ikut negeri
        if state:
            alerts = alerts[alerts["state"].str.lower() == state.lower()]

        # Tukar NaN/inf ke None
        result = alerts.replace([float('inf'), -float('inf')], None).to_dict(orient="records")

        return result

    except Exception as e:
        return {"error": str(e)}
