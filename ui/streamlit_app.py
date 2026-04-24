import os
import requests
import streamlit as st

st.set_page_config(page_title="CryptoBot UI", layout="wide")

# En docker-compose: API_URL = http://api:8000
# En local (si tu lances streamlit hors docker): mettre API_URL=http://127.0.0.1:8000
API_URL = os.getenv("API_URL", "http://127.0.0.1:8000").rstrip("/")

st.title("CryptoBot — UI (local)")
st.caption(f"API_URL = {API_URL}")

def safe_json(resp: requests.Response):
    try:
        return resp.json()
    except Exception:
        return {"raw": resp.text}

# --- Health quick check ---
with st.expander("API status", expanded=False):
    try:
        r = requests.get(f"{API_URL}/health", timeout=8)
        st.write("GET /health", r.status_code)
        st.json(safe_json(r))
    except Exception as e:
        st.error(f"API unreachable: {e}")

tab1, tab2, tab3 = st.tabs(["Signal latest (DB)", "Predict (manual)", "Drift (DB)"])

# -------------------------
# TAB 1: /signal/latest
# -------------------------
with tab1:
    st.subheader("Signal latest (DB → features → predict)")
    st.write("Appelle `POST /signal/latest` : l’API lit Postgres, calcule RSI+ et renvoie le signal sur la dernière bougie.")

    if st.button("Get latest signal", type="primary"):
        try:
            r = requests.post(f"{API_URL}/signal/latest", timeout=20)
            if r.status_code != 200:
                st.error(f"/signal/latest failed {r.status_code}: {r.text}")
            else:
                out = r.json()
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("open_time", out["open_time"])
                c2.metric("proba_buy", f"{out['proba_buy']:.4f}")
                c3.metric("signal_buy", int(out["signal_buy"]))
                c4.metric("threshold", f"{out['threshold']:.2f}")

                st.write(f"symbol={out.get('symbol')} | interval={out.get('interval')}")
                st.subheader("features_used")
                # affiche sous forme table
                st.dataframe(
                    [{"feature": k, "value": v} for k, v in out["features_used"].items()],
                    use_container_width=True
                )
                with st.expander("Raw JSON"):
                    st.json(out)
        except Exception as e:
            st.error(f"Request failed: {e}")

# -------------------------
# TAB 2: /predict manual
# -------------------------
with tab2:
    st.subheader("Predict (manual /predict)")
    st.caption("Tu renseignes les 7 features RSI+, l’API retourne proba_buy + signal_buy (seuil=0.56).")

    col1, col2 = st.columns(2)
    with col1:
        ret_1h = st.number_input("ret_1h", value=0.001, format="%.6f")
        ret_3h = st.number_input("ret_3h", value=0.002, format="%.6f")
        ret_6h = st.number_input("ret_6h", value=-0.001, format="%.6f")
        rsi_14 = st.number_input("rsi_14", value=52.0, format="%.4f")
    with col2:
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

    if st.button("Predict", type="primary"):
        try:
            r = requests.post(f"{API_URL}/predict", json=payload, timeout=12)
            if r.status_code != 200:
                st.error(f"/predict failed {r.status_code}: {r.text}")
            else:
                out = r.json()
                c1, c2, c3 = st.columns(3)
                c1.metric("proba_buy", f"{out['proba_buy']:.4f}")
                c2.metric("signal_buy", int(out["signal_buy"]))
                c3.metric("threshold", f"{out['threshold']:.2f}")
                with st.expander("payload"):
                    st.json(payload)
        except Exception as e:
            st.error(f"Request failed: {e}")

# -------------------------
# TAB 3: /drift/latest
# -------------------------
with tab3:
    st.subheader("Drift (DB)")
    window = st.slider("window_hours", min_value=72, max_value=2000, value=720, step=24)

    if st.button("Compute drift", type="primary"):
        try:
            r = requests.get(f"{API_URL}/drift/latest?window={int(window)}", timeout=30)
            if r.status_code != 200:
                st.error(f"/drift/latest failed {r.status_code}: {r.text}")
            else:
                out = r.json()
                c1, c2, c3 = st.columns(3)
                c1.metric("latest_open_time", out["latest_open_time"])
                c2.metric("drift_score", f"{out['drift_score']:.4f}")
                c3.metric("window_hours", int(out["window_hours"]))

                st.subheader("summary")
                st.json(out["summary"])

                # table per_feature
                rows = []
                for feat, d in out["per_feature"].items():
                    rows.append({
                        "feature": feat,
                        "z_shift_mean": d["z_shift_mean"],
                        "rel_diff_std": d["rel_diff_std"],
                        "ref_mean": d["ref"]["mean"],
                        "ref_std": d["ref"]["std"],
                        "cur_mean": d["current"]["mean"],
                        "cur_std": d["current"]["std"],
                    })
                st.subheader("per_feature")
                st.dataframe(rows, use_container_width=True)

                with st.expander("Raw JSON"):
                    st.json(out)
        except Exception as e:
            st.error(f"Request failed: {e}")
