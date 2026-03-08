import json
import joblib
import pandas as pd
from dataclasses import dataclass
from typing import Any, Dict, List

@dataclass
class ModelBundle:
    model: Any
    features: List[str]
    threshold: float
    metadata: Dict[str, Any]

def load_bundle(model_path: str, meta_path: str) -> ModelBundle:
    model = joblib.load(model_path)

    with open(meta_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    features = metadata["features"]
    threshold = float(metadata["threshold"])

    return ModelBundle(model=model, features=features, threshold=threshold, metadata=metadata)

def predict_proba(bundle: ModelBundle, feature_dict: Dict[str, float]) -> float:
    # Use DataFrame with column names to avoid sklearn warning
    X = pd.DataFrame([feature_dict], columns=bundle.features)
    proba = bundle.model.predict_proba(X)[:, 1][0]
    return float(proba)
