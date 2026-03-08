# CryptoBot (DST) — Pipeline Data + ML + API (Step 4)

## 📌 Présentation
CryptoBot est un projet de data engineering autour des marchés crypto.
Il couvre un pipeline reproductible (**API → RAW → STG → CLEAN → FACT**) + un modèle ML orienté trading,
puis son industrialisation via **FastAPI + Docker Compose**.

Projet réalisé dans le cadre de la formation DataScientest.

---

## 🗂️ Structure (fichiers utiles)
cryptobot/
├── app/                          # API FastAPI (Step 4)
│   ├── main.py                   # Endpoints /health /predict /signal/latest /drift/latest
│   ├── core/                     # settings (.env) + erreurs
│   ├── db.py                     # accès Postgres + fetch_latest_closes()
│   └── ml/                       # loader modèle + schemas + features + drift
│
├── models/                       # Artefacts ML figés (Step 4)
│   ├── logreg_rsi_plus.joblib
│   ├── metadata.json
│   └── drift_reference.json
│
├── scripts/
│   ├── etl/
│   │   ├── fetch/                # Step 1: collecte snapshots (Binance/CoinGecko)
│   │   └── load/                 # Step 2: ingestion RAW (JSON->Postgres) + macro
│   ├── ml_final/                 # Step 3 final retenu (dataset + backtest)
│   │   ├── 08b_build_rsi_dataset_plus.py
│   │   └── 19_backtest_tpsl_sequential.py
│   └── step4/                    # Step 4: export artefacts + drift + smoke test
│       ├── train_export_model.py
│       ├── build_drift_reference.py
│       └── smoke_test_step4.sh
│
├── docker/
│   └── postgres-init/
│       └── 10_cryptobot_dump.sql         # init DB (schema cryptobot + données)
│
├── data/
│   ├── raw/                      # snapshots Binance + CoinGecko (Step 1)
│   └── processed/                # dataset ML final + macro (Step 3)
│       ├── ml_rsi_dataset_plus.csv
│       ├── macro_daily.json
│       ├── final_step3_bestcase.txt
│       └── final_step3_worstcase.txt
│
├── sql/                          # Step 2: DDL + transformations RAW→STG→CLEAN→FACT
├── Dockerfile
├── docker-compose.yml
├── requirements_step4.txt
└── .env.step4

---

## 🧠 Modèle ML (Step 3 → utilisé en Step 4)
- Modèle : LogisticRegression(class_weight="balanced")
- Features (RSI+) :
  - ret_1h, ret_3h, ret_6h, rsi_14, trend_24h, vol_24h, rsi_slope_6h
- Threshold : 0.56
- Dataset final : data/processed/ml_rsi_dataset_plus.csv
- Artefact exporté : models/logreg_rsi_plus.joblib + models/metadata.json

---

## 🚀 Step 4 — API FastAPI
### Endpoints
- GET `/` : infos + liens utiles
- GET `/health` : status OK
- GET `/model/info` : features + threshold + metadata
- POST `/predict` : proba_buy + signal_buy (features en entrée)
- POST `/signal/latest` : calcule les features depuis Postgres et renvoie un signal
- GET `/drift/latest?window=720` : drift report simple (stats ref vs fenêtre récente DB)

Swagger : http://127.0.0.1:8000/docs

---

## 🐳 Lancement (Docker Compose)
### Pré-requis
- Docker + Docker Compose

### Démarrer
```bash
docker compose up -d --build
docker compose ps

Services :
API : http://127.0.0.1:8000
Postgres (interne au compose)
pgAdmin : http://127.0.0.1:5050
Mongo : localhost:27017

✅ Tests rapides (smoke test)
Smoke test automatisé
bash scripts/step4/smoke_test_step4.sh

Ce test vérifie :
/health OK
/model/info OK
/predict OK
/signal/latest et /drift/latest OK (DB up)
DB down → endpoints DB renvoient 503
🛑 Stop
docker compose down

👤 Auteur

Projet réalisé par Med Amine Mrizki
dans le cadre de la formation DataScientest.


---


### Bloc — tests “manuel rapide” (visuel)
```bash
docker compose up -d --build
docker compose ps

curl -sS http://127.0.0.1:8000/ ; echo
curl -sS http://127.0.0.1:8000/health ; echo
curl -sS http://127.0.0.1:8000/model/info | head -c 300 ; echo

curl -sS -X POST http://127.0.0.1:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"ret_1h":0.001,"ret_3h":0.002,"ret_6h":0.003,"rsi_14":55,"trend_24h":0.01,"vol_24h":0.005,"rsi_slope_6h":1.2}' ; echo

docker compose exec -T postgres psql -U daniel -d dst_db -c "SELECT COUNT(*) FROM cryptobot.fact_market_price;"
curl -sS -X POST http://127.0.0.1:8000/signal/latest | head -c 220 ; echo
curl -sS "http://127.0.0.1:8000/drift/latest?window=720" | head -c 220 ; echo
Bloc 2 — smoke test (automatisé, fail-fast)
bash scripts/step4/smoke_test_step4.sh