import logging
import re

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException

from app.core.config import settings
from app.core.errors import DbUnavailableError

from app.db import fetch_latest_closes
from app.ml.drift import load_reference_stats, compare_stats
from app.ml.features import compute_features_rsi_plus, _wilder_rsi
from app.ml.model_loader import load_bundle, predict_proba
from app.ml.schemas import PredictRequest, PredictResponse


# --- logging simple ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("cryptobot-api")

app = FastAPI(title=settings.APP_NAME, version=settings.APP_VERSION)

bundle = None
DRIFT_REF_PATH = "models/drift_reference.json"


@app.on_event("startup")
def startup():
    global bundle
    bundle = load_bundle(settings.MODEL_PATH, settings.MODEL_META_PATH)

    logger.info("API started: %s v%s", settings.APP_NAME, settings.APP_VERSION)
    logger.info("Model loaded from: %s", settings.MODEL_PATH)
    logger.info("Metadata path: %s", settings.MODEL_META_PATH)
    logger.info("Threshold: %.4f", bundle.threshold)
    logger.info("Features (%d): %s", len(bundle.features), ", ".join(bundle.features))
    logger.info("DB features enabled: %s", settings.ENABLE_DB_FEATURES)

    if settings.ENABLE_DB_FEATURES:
        pg_uri_safe = re.sub(
            r"(postgresql\+psycopg2://[^:]+):[^@]+@",
            r"\1:***@",
            settings.PG_URI,
        )
        logger.info("PG_URI: %s", pg_uri_safe)
        logger.info(
            "Market: %s | Interval: %s | Lookback rows: %d",
            settings.MARKET_SYMBOL,
            settings.INTERVAL_CODE,
            settings.LOOKBACK_ROWS,
        )


@app.get("/")
def root():
    return {
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "docs": "/docs",
        "health": "/health",
        "model_info": "/model/info",
        "predict": "/predict",
        "signal_latest": "/signal/latest",
        "drift_latest": "/drift/latest?window=720",
    }


@app.get("/health")
def health():
    return {"status": "ok", "service": settings.APP_NAME, "version": settings.APP_VERSION}


@app.get("/model/info")
def model_info():
    if bundle is None:
        raise HTTPException(status_code=500, detail="Model not loaded")

    return {
        "threshold": bundle.threshold,
        "features": bundle.features,
        "metadata": bundle.metadata,
    }


@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    if bundle is None:
        raise HTTPException(status_code=500, detail="Model not loaded")

    features = req.model_dump()
    proba = predict_proba(bundle, features)
    signal = 1 if proba >= bundle.threshold else 0

    return PredictResponse(
        proba_buy=proba,
        signal_buy=signal,
        threshold=bundle.threshold,
    )


@app.post("/signal/latest")
def signal_latest():
    if not settings.ENABLE_DB_FEATURES:
        raise HTTPException(status_code=400, detail="DB features disabled")
    if bundle is None:
        raise HTTPException(status_code=500, detail="Model not loaded")

    try:
        df = fetch_latest_closes(
            pg_uri=settings.PG_URI,
            symbol=settings.MARKET_SYMBOL,
            interval=settings.INTERVAL_CODE,
            limit=settings.LOOKBACK_ROWS,
        )

        feat = compute_features_rsi_plus(df)
        proba = predict_proba(bundle, feat)
        signal = 1 if proba >= bundle.threshold else 0

        return {
            "open_time": df["open_time"].iloc[-1].isoformat(),
            "proba_buy": proba,
            "signal_buy": signal,
            "threshold": bundle.threshold,
            "features_used": feat,
            "symbol": settings.MARKET_SYMBOL,
            "interval": settings.INTERVAL_CODE,
        }

    except DbUnavailableError as e:
        logger.warning("DB unavailable: %s", str(e))
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.exception("signal/latest failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/drift/latest")
def drift_latest(window: int = 720):
    """
    Simple drift report:
    - reference stats computed from training dataset (models/drift_reference.json)
    - current stats computed from latest `window` hours from Postgres-derived features
    """
    if not settings.ENABLE_DB_FEATURES:
        raise HTTPException(status_code=400, detail="DB features disabled")
    if bundle is None:
        raise HTTPException(status_code=500, detail="Model not loaded")

    try:
        # Need extra history to compute RSI/trend/vol properly (rolling + shifts).
        extra = 250
        limit = int(window) + settings.LOOKBACK_ROWS + extra

        df = fetch_latest_closes(
            pg_uri=settings.PG_URI,
            symbol=settings.MARKET_SYMBOL,
            interval=settings.INTERVAL_CODE,
            limit=limit,
        )

        close = df["close"].astype(float)

        feat_df = pd.DataFrame(
            {
                "open_time": df["open_time"],
                "close": close,
            }
        )

        # Same feature definitions as RSI+ dataset
        feat_df["ret_1h"] = np.log(close / close.shift(1))
        feat_df["ret_3h"] = np.log(close / close.shift(3))
        feat_df["ret_6h"] = np.log(close / close.shift(6))

        feat_df["rsi_14"] = _wilder_rsi(close, period=14)
        feat_df["trend_24h"] = np.log(close / close.shift(24))
        feat_df["vol_24h"] = feat_df["ret_1h"].rolling(24, min_periods=24).std()
        feat_df["rsi_slope_6h"] = feat_df["rsi_14"] - feat_df["rsi_14"].shift(6)

        # Keep complete rows + last window
        feat_df = feat_df.dropna().tail(int(window)).reset_index(drop=True)

        ref = load_reference_stats(DRIFT_REF_PATH)
        comp = compare_stats(ref, feat_df, bundle.features)

        return {
            "window_hours": int(window),
            "symbol": settings.MARKET_SYMBOL,
            "interval": settings.INTERVAL_CODE,
            "latest_open_time": df["open_time"].iloc[-1].isoformat(),
            "drift_score": comp["drift_score"],
            "summary": comp["summary"],
            "per_feature": comp["features"],
        }

    except DbUnavailableError as e:
        logger.warning("DB unavailable: %s", str(e))
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.exception("drift/latest failed")
        raise HTTPException(status_code=500, detail=str(e))


        