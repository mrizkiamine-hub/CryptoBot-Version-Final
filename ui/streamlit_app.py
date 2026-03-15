import os
import requests
import streamlit as st

st.set_page_config(page_title="CryptoBot Demo (DST)", layout="centered")

API_URL = os.getenv("API_URL", "").rstrip("/")
if not API_URL:
    st.error("Missing API_URL env var. Example: https://...run.app")
    st.stop()

st.title("CryptoBot — Demo Streamlit")
st.caption("UI simple pour appeler l’endpoint /predict de l’API FastAPI (Cloud Run).")

st.subheader("Features (RSI+)")
ret_1h = st.number_input("ret_1h", value=0.001, format="%.6f")
ret_3h = st.number_input("ret_3h", value=0.002, format="%.6f")
ret_6h = st.number_input("ret_6h", value=-0.001, format="%.6f")
rsi_14 = st.number_input("rsi_14", value=52.0, format="%.4f")
trend_24h = st.number_input("trend_24h", value=0.0, format="%.6f")
vol_24h = st.number_input("vol_24h", value=0.01, format="%.6f")
rsi_slope_6h = st.number_input("rsi_slope_6h", value=1.2, format="%.6f")

payload = {
    "ret_1h": float(ret_1h),
    "ret_3h": float(ret_3h),
    "ret_6h": float(ret_6h),
    "rsi_14": float(rsi_14),
    "trend_24h": float(trend_24h),
    "vol_24h": float(vol_24h),
    "rsi_slope_6h": float(rsi_slope_6h),
}

if st.button("Predict"):
    try:
        r = requests.post(f"{API_URL}/predict", json=payload, timeout=10)
        if r.status_code != 200:
            st.error(f"API error {r.status_code}: {r.text}")
        else:
            st.success("Prediction OK")
            st.json(r.json())
    except Exception as e:
        st.error(f"Request failed: {e}")
