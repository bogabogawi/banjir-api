from fastapi import FastAPI
import pandas as pd
import os

app = FastAPI()

DATA_DIR = "data"

@app.get("/")
def root():
    return {"message": "Banjir API running!"}

@app.get("/paras_air")
def get_paras_air():
    try:
        latest = sorted([f for f in os.listdir(DATA_DIR) if f.startswith("paras_air_")])[-1]
        df = pd.read_csv(os.path.join(DATA_DIR, latest))
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

@app.get("/hujan")
def get_hujan():
    try:
        latest = sorted([f for f in os.listdir(DATA_DIR) if f.startswith("hujan_")])[-1]
        df = pd.read_csv(os.path.join(DATA_DIR, latest))
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}
