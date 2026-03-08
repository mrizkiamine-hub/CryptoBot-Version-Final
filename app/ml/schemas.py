from pydantic import BaseModel

class PredictRequest(BaseModel):
    ret_1h: float
    ret_3h: float
    ret_6h: float
    rsi_14: float
    trend_24h: float
    vol_24h: float
    rsi_slope_6h: float

class PredictResponse(BaseModel):
    proba_buy: float
    signal_buy: int
    threshold: float
