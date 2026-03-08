from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    APP_NAME: str = "cryptobot-api"
    APP_VERSION: str = "0.1.0"

    MODEL_PATH: str = "models/logreg_rsi_plus.joblib"
    MODEL_META_PATH: str = "models/metadata.json"

    # DB (pour /signal/latest)
    ENABLE_DB_FEATURES: bool = True
    PG_URI: str = "postgresql+psycopg2://daniel:datascientest@localhost:5432/dst_db"
    MARKET_SYMBOL: str = "BTCUSDT"
    INTERVAL_CODE: str = "1h"
    LOOKBACK_ROWS: int = 300  # marge pour RSI(14), trend/vol 24h, etc.

settings = Settings()
