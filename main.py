@app.get("/alert")
def get_alert(state: str = None):
    try:
        df = pd.read_csv("data/paras_air.csv")

        # Buang row NaN
        df = df.dropna()

        # Pastikan kolum Danger wujud
        if "Danger" not in df.columns:
            return {"error": f"Kolum 'Danger' tak jumpa. Header: {list(df.columns)}"}

        # Tukar ke numeric
        df["Water Level (m) (Graph)"] = pd.to_numeric(df["Water Level (m) (Graph)"], errors="coerce")
        df["Danger"] = pd.to_numeric(df["Danger"], errors="coerce")

        # Filter ikut state kalau ada
        if state:
            df = df[df["state"].str.lower() == state.lower()]

        # Kalau negeri tu memang kosong → bagi mesej
        if df.empty:
            return {"message": f"Tiada data untuk negeri {state if state else 'yang diminta'}"}

        # Cari bacaan yang melebihi threshold Danger
        alerts = df[df["Water Level (m) (Graph)"] >= df["Danger"]]

        # Tukar ke JSON
        result = alerts.replace({float("inf"): None, -float("inf"): None}).to_dict(orient="records")
        return result

    except Exception as e:
        return {"error": str(e)}
