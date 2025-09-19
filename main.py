from fastapi import FastAPI
import pandas as pd

app = FastAPI()

# Endpoint asal paras_air
@app.get("/paras_air")
def get_paras_air(state: str = None):
    df = pd.read_csv("data/paras_air.csv")
    if state:
        df = df[df["state"].str.lower() == state.lower()]
    return df.to_dict(orient="records")

# Endpoint asal hujan
@app.get("/hujan")
def get_hujan(state: str = None):
    df = pd.read_csv("data/hujan.csv")
    if state:
        df = df[df["state"].str.lower() == state.lower()]
    return df.to_dict(orient="records")

# 🔴 Endpoint Alert (tunjuk bahaya)
@app.get("/alert")
def get_alert(state: str = None):
    df = pd.read_csv("data/paras_air.csv")
    # ambil station yang Threshold3 (bahaya) sudah dilaporkan
    df_alert = df[df["Threshold3"].notna()]  

    if state:
        df_alert = df_alert[df_alert["state"].str.lower() == state.lower()]

    return df_alert.to_dict(orient="records")
