import logging
import re

from fastapi import FastAPI, HTTPException

from app.core.config import settings
from app.core.errors import DbUnavailableError

from app.db import fetch_latest_closes
from app.ml.features import compute_features_rsi_plus
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


