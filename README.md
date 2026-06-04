# CryptoBot (DST) — Pipeline Data + ML + API (local Docker)

## 📌 Présentation
CryptoBot est un projet de data engineering autour des marchés crypto.
Il couvre un pipeline reproductible (**API → RAW → STG → CLEAN → FACT**) + un modèle ML orienté trading,
puis son industrialisation **en local** via **FastAPI + Docker Compose**, avec :
- une **UI Streamlit** (démo utilisateur),
- une **automatisation simple via Cron** (CI/CD simplifié local).

Projet réalisé dans le cadre de la formation DataScientest.

---

## 🗂️ Structure (fichiers utiles)
cryptobot/
├── app/                          # API FastAPI (Step 4)
│   ├── main.py                   # Endpoints /health /predict /signal/latest
│   ├── core/                     # settings (.env) + erreurs
│   ├── db.py                     # accès Postgres + fetch_latest_closes()
│   └── ml/                       # loader modèle + schemas + features
│
├── ui/                           # UI Streamlit (local) - consomme l'API
│   ├── streamlit_app.py          # onglets: Signal latest / Predict
│   ├── requirements.txt
│   └── Dockerfile
│
├── models/                       # Artefacts ML figés (Step 4)
│   ├── logreg_rsi_plus.joblib
│   ├── metadata.json
│
├── scripts/
│   ├── etl/
│   │   ├── fetch/                # Step 1: collecte snapshots (Binance/CoinGecko)
│   │   └── load/                 # Step 2: ingestion RAW (JSON->Postgres) + macro
│   ├── ml_final/                 # Step 3 final retenu (dataset + backtest)
│   │   ├── 08b_build_rsi_dataset_plus.py
│   │   └── 19_backtest_tpsl_sequential.py
│   ├── step4/                    # Step 4: export artefacts + smoke test
│   │   ├── train_export_model.py
│   │   └── smoke_test_step4.sh
│   └── ops/                      # Automatisation local (cron)
│       └── run_latest_signal.sh  # appelle /signal/latest + logs JSONL + rotation
│
├── docker/
│   └── postgres-init/
│       └── 10_cryptobot_dump.sql # init DB (schema cryptobot + données)
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
├── .env.step4.example
└── .dockerignore

---

## 🧠 Modèle ML (Step 3 → utilisé en Step 4)
- Modèle : `LogisticRegression(class_weight="balanced")`
- Features (RSI+) :
  - `ret_1h, ret_3h, ret_6h, rsi_14, trend_24h, vol_24h, rsi_slope_6h`
- Threshold : `0.56`
- Dataset final : `data/processed/ml_rsi_dataset_plus.csv`
- Artefacts exportés : `models/logreg_rsi_plus.joblib` + `models/metadata.json`

---

## 🚀 API FastAPI (Step 4)
### Endpoints
- GET `/` : infos + liens utiles
- GET `/health` : status OK
- GET `/model/info` : features + threshold + metadata
- POST `/predict` : `proba_buy` + `signal_buy` (features en entrée)
- POST `/signal/latest` : lit Postgres, calcule RSI+, renvoie un signal (dernière bougie)

Swagger : http://127.0.0.1:8000/docs

### Robustesse DB
- DB down → `/signal/latest` renvoie **503**
- `/health` et `/predict` restent OK

---

## 🐳 Lancement (Docker Compose)
### Pré-requis
- Docker + Docker Compose

### Démarrer
```bash
docker compose up -d --build
docker compose ps
````

Services (local) :

* API : [http://127.0.0.1:8000](http://127.0.0.1:8000)
* UI Streamlit : [http://127.0.0.1:8501](http://127.0.0.1:8501)
* pgAdmin : [http://127.0.0.1:5050](http://127.0.0.1:5050)
* Mongo : localhost:27017 (optionnel)

### Stop

```bash
docker compose down -v
```

---

## 🖥️ UI Streamlit (local)

L’UI Streamlit est intégrée dans Docker Compose (service `ui`) et consomme l’API via le réseau Docker.

URL : [http://127.0.0.1:8501](http://127.0.0.1:8501)

Onglets UI :

* **Signal latest (DB)** : appelle `POST /signal/latest` (DB → features → predict)
* **Predict (manual)** : appelle `POST /predict` (saisie des 7 features)

---

## 🗄️ pgAdmin (requêter PostgreSQL)

URL : [http://127.0.0.1:5050](http://127.0.0.1:5050)
Login : `admin@admin.com`
Password : `admin`

Register Server (dans pgAdmin) :

* Host : `postgres`
* Port : `5432`
* DB : `dst_db`
* User : `daniel`
* Password : valeur définie localement dans `.env`

Exemples :

```sql
SELECT COUNT(*) FROM cryptobot.fact_market_price;

SELECT open_time, close
FROM cryptobot.fact_market_price
ORDER BY open_time DESC
LIMIT 10;
```

---

## ✅ Tests (smoke test)

Smoke test automatisé :

```bash
bash scripts/step4/smoke_test_step4.sh
```

Ce test vérifie :

* `/health`, `/model/info`, `/predict` OK
* `/signal/latest` OK (DB up)
* DB down → endpoints DB renvoient 503 (et API non-DB reste OK)

---

## ⏱️ Automatisation (CI/CD simplifié local) via Cron

Objectif : automatiser un mini workflow “prod-like” en local.

* Un cron job exécute périodiquement un script qui appelle `POST /signal/latest`.
* Les résultats sont historisés en JSONL (1 ligne par exécution).

### Script

* Script : `scripts/ops/run_latest_signal.sh`
* Sorties (runtime local, non versionnées) :

  * `logs/signal_latest.jsonl` : historique JSONL (run_ts_utc + status + http_code + response)
  * `logs/cron_signal.log` : log d’exécution (stdout/stderr)
* Le script inclut :

  * gestion d’erreurs (`status=error`)
  * rotation simple (seuil ~5MB) du fichier JSONL

### Installer un cron (exemple toutes les 15 minutes)

Afficher la crontab :

```bash
crontab -l
```

Ajouter la tâche :

```bash
(crontab -l 2>/dev/null; echo "*/15 * * * * cd $HOME/cryptobot && API_URL=http://127.0.0.1:8000 ./scripts/ops/run_latest_signal.sh >> ./logs/cron_signal.log 2>&1") | crontab -
```

Désactiver / réactiver :

* `crontab -e` puis commenter/décommenter la ligne cron (avec `#`)

Vérifier :

```bash
tail -n 5 logs/cron_signal.log
tail -n 2 logs/signal_latest.jsonl | head -c 250 ; echo
```

---

## 🧩 Note MongoDB

MongoDB est présent dans le docker-compose à titre optionnel.
La version finale du projet exploite principalement PostgreSQL (RAW/STG/CLEAN/STAR).
Mongo pourra être retiré après soutenance si non utilisé.

---

## 👤 Auteur

Projet réalisé par Med Amine Mrizki dans le cadre de la formation DataScientest.

