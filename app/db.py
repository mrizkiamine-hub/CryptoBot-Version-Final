# app/db.py
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from app.core.errors import DbUnavailableError

SQL_LATEST = """
SELECT f.open_time, f.close, f.volume_base, f.trade_count
FROM cryptobot.fact_market_price f
JOIN cryptobot.dim_market m ON m.id = f.market_id
WHERE m.symbol = :symbol AND f.interval_code = :interval
ORDER BY f.open_time DESC
LIMIT :limit;
"""

def fetch_latest_closes(pg_uri: str, symbol: str, interval: str, limit: int) -> pd.DataFrame:
    """
    Fetch the latest `limit` rows from Postgres for (symbol, interval).
    Returns a DataFrame sorted by open_time ascending (chronological).
    """
    try:
        engine = create_engine(pg_uri, pool_pre_ping=True)

        with engine.connect() as conn:
            df = pd.read_sql(
                text(SQL_LATEST),
                conn,
                params={"symbol": symbol, "interval": interval, "limit": int(limit)},
            )

        if df.empty:
            raise DbUnavailableError(
                "DB reachable but returned 0 rows for requested symbol/interval."
            )

        df["open_time"] = pd.to_datetime(df["open_time"], utc=True)

        # Query is DESC for speed; we re-sort ASC for correct time series order
        df = df.sort_values("open_time").reset_index(drop=True)
        return df

    except SQLAlchemyError as e:
        raise DbUnavailableError(f"DB connection/query failed: {type(e).__name__}") from e