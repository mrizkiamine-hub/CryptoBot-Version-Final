import os

import requests
import streamlit as st


st.set_page_config(
    page_title="CryptoBot UI",
    layout="wide",
)

# En Docker Compose : API_URL = http://api:8000
# En lancement local hors Docker : API_URL = http://127.0.0.1:8000
API_URL = os.getenv("API_URL", "http://127.0.0.1:8000").rstrip("/")


def safe_json(response: requests.Response) -> dict:
    """Retourne la réponse JSON ou le texte brut si le parsing échoue."""
    try:
        return response.json()
    except Exception:
        return {"raw": response.text}


def display_signal_result(result: dict) -> None:
    """Affiche le dernier signal calculé depuis PostgreSQL."""
    signal_buy = int(result["signal_buy"])
    decision_label = "BUY" if signal_buy == 1 else "NO BUY"

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric("open_time", result["open_time"])
    col2.metric("proba_buy", f"{result['proba_buy']:.4f}")
    col3.metric("signal_buy", signal_buy)
    col4.metric("Décision", decision_label)
    col5.metric("threshold", f"{result['threshold']:.2f}")

    st.caption(
        f"symbol={result.get('symbol')} | "
        f"interval={result.get('interval')}"
    )

    st.subheader("Features utilisées")

    features = result.get("features_used", {})

    st.dataframe(
        [
            {"feature": feature, "value": value}
            for feature, value in features.items()
        ],
        use_container_width=True,
        hide_index=True,
    )

    with st.expander("Réponse JSON brute"):
        st.json(result)


def display_manual_prediction(result: dict, payload: dict) -> None:
    """Affiche le résultat d'une prédiction manuelle."""
    signal_buy = int(result["signal_buy"])
    decision_label = "BUY" if signal_buy == 1 else "NO BUY"

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("proba_buy", f"{result['proba_buy']:.4f}")
    col2.metric("signal_buy", signal_buy)
    col3.metric("Décision", decision_label)
    col4.metric("threshold", f"{result['threshold']:.2f}")

    with st.expander("Payload envoyé"):
        st.json(payload)

    with st.expander("Réponse JSON brute"):
        st.json(result)


# -------------------------------------------------------------------
# Initialisation des états persistants de la session Streamlit
# -------------------------------------------------------------------
if "latest_signal" not in st.session_state:
    st.session_state["latest_signal"] = None

if "manual_prediction" not in st.session_state:
    st.session_state["manual_prediction"] = None

if "manual_payload" not in st.session_state:
    st.session_state["manual_payload"] = None


# -------------------------------------------------------------------
# En-tête
# -------------------------------------------------------------------
st.title("CryptoBot — Interface locale")
st.caption(f"API_URL = {API_URL}")


# -------------------------------------------------------------------
# Vérification rapide de l'API
# -------------------------------------------------------------------
with st.expander("Statut de l'API", expanded=False):
    try:
        response = requests.get(f"{API_URL}/health", timeout=8)

        if response.status_code == 200:
            st.success("API disponible")
        else:
            st.warning(f"API accessible avec le code HTTP {response.status_code}")

        st.write("GET /health", response.status_code)
        st.json(safe_json(response))

    except requests.RequestException as exc:
        st.error(f"API indisponible : {exc}")


# -------------------------------------------------------------------
# Onglets principaux
# -------------------------------------------------------------------
tab_signal, tab_predict = st.tabs(
    [
        "Dernier signal (DB)",
        "Prédiction manuelle",
    ]
)


# -------------------------------------------------------------------
# ONGLET 1 : /signal/latest
# -------------------------------------------------------------------
with tab_signal:
    st.subheader("Dernier signal : DB → features → prédiction")

    st.write(
        "L'endpoint `POST /signal/latest` lit PostgreSQL, calcule les "
        "features RSI+ puis renvoie le signal associé à la dernière bougie."
    )

    if st.button(
        "Calculer le dernier signal",
        type="primary",
        key="get_latest_signal",
    ):
        try:
            response = requests.post(
                f"{API_URL}/signal/latest",
                timeout=20,
            )

            if response.status_code != 200:
                st.error(
                    f"/signal/latest a échoué "
                    f"({response.status_code}) : {response.text}"
                )
            else:
                st.session_state["latest_signal"] = response.json()

        except requests.RequestException as exc:
            st.error(f"Erreur pendant l'appel API : {exc}")

    if st.session_state["latest_signal"] is not None:
        display_signal_result(st.session_state["latest_signal"])


# -------------------------------------------------------------------
# ONGLET 2 : /predict
# -------------------------------------------------------------------
with tab_predict:
    st.subheader("Prédiction manuelle : /predict")

    st.caption(
        "Renseignez les 7 features RSI+. "
        "L'API renvoie proba_buy et signal_buy avec un seuil de 0,56."
    )

    col1, col2 = st.columns(2)

    with col1:
        ret_1h = st.number_input(
            "ret_1h",
            value=0.001,
            format="%.6f",
        )

        ret_3h = st.number_input(
            "ret_3h",
            value=0.002,
            format="%.6f",
        )

        ret_6h = st.number_input(
            "ret_6h",
            value=-0.001,
            format="%.6f",
        )

        rsi_14 = st.number_input(
            "rsi_14",
            value=52.0,
            format="%.4f",
        )

    with col2:
        trend_24h = st.number_input(
            "trend_24h",
            value=0.0,
            format="%.6f",
        )

        vol_24h = st.number_input(
            "vol_24h",
            value=0.01,
            format="%.6f",
        )

        rsi_slope_6h = st.number_input(
            "rsi_slope_6h",
            value=1.2,
            format="%.6f",
        )

    payload = {
        "ret_1h": float(ret_1h),
        "ret_3h": float(ret_3h),
        "ret_6h": float(ret_6h),
        "rsi_14": float(rsi_14),
        "trend_24h": float(trend_24h),
        "vol_24h": float(vol_24h),
        "rsi_slope_6h": float(rsi_slope_6h),
    }

    if st.button(
        "Lancer la prédiction",
        type="primary",
        key="run_manual_prediction",
    ):
        try:
            response = requests.post(
                f"{API_URL}/predict",
                json=payload,
                timeout=12,
            )

            if response.status_code != 200:
                st.error(
                    f"/predict a échoué "
                    f"({response.status_code}) : {response.text}"
                )
            else:
                st.session_state["manual_payload"] = payload
                st.session_state["manual_prediction"] = response.json()

        except requests.RequestException as exc:
            st.error(f"Erreur pendant l'appel API : {exc}")

    if st.session_state["manual_prediction"] is not None:
        display_manual_prediction(
            st.session_state["manual_prediction"],
            st.session_state["manual_payload"],
        )
