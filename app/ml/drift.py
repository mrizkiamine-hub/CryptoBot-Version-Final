import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, List

import numpy as np
import pandas as pd


@dataclass
class Stats:
    mean: float
    std: float
    min: float
    max: float
    n: int


def _safe_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")


def compute_stats(df: pd.DataFrame, features: List[str]) -> Dict[str, Stats]:
    out: Dict[str, Stats] = {}
    for f in features:
        s = pd.to_numeric(df[f], errors="coerce").dropna()
        out[f] = Stats(
            mean=_safe_float(s.mean()),
            std=_safe_float(s.std(ddof=0)),
            min=_safe_float(s.min()),
            max=_safe_float(s.max()),
            n=int(s.shape[0]),
        )
    return out


def rel_diff(a: float, b: float, eps: float = 1e-12) -> float:
    return float(abs(a - b) / (abs(a) + eps))


def z_shift(mean_ref: float, std_ref: float, mean_cur: float, eps: float = 1e-12) -> float:
    # difference in units of reference std (robust when mean_ref ~ 0)
    return float(abs(mean_cur - mean_ref) / (abs(std_ref) + eps))


def build_reference_stats(dataset_path: str, features: List[str]) -> Dict[str, Any]:
    df = pd.read_csv(dataset_path)
    stats = compute_stats(df, features)
    return {
        "dataset": dataset_path,
        "features": features,
        "stats": {k: vars(v) for k, v in stats.items()},
    }


def save_reference_stats(path: str, payload: Dict[str, Any]) -> None:
    Path(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_reference_stats(path: str) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def compare_stats(ref_stats: Dict[str, Any], cur_df: pd.DataFrame, features: List[str]) -> Dict[str, Any]:
    cur = compute_stats(cur_df, features)

    per_feature = {}
    z_shifts = []
    std_rel_diffs = []

    for f in features:
        r = ref_stats["stats"][f]
        c = cur[f]

        # robust mean drift
        z = z_shift(r["mean"], r["std"], c.mean)
        z_shifts.append(z)

        # std drift (relative)
        std_d = rel_diff(r["std"], c.std)
        std_rel_diffs.append(std_d)

        per_feature[f] = {
            "ref": r,
            "current": vars(c),
            "z_shift_mean": z,
            "rel_diff_std": std_d,
        }

    drift_score = float(np.mean(z_shifts + std_rel_diffs)) if (z_shifts and std_rel_diffs) else 0.0

    return {
        "drift_score": drift_score,
        "features": per_feature,
        "summary": {
            "avg_z_shift_mean": float(np.mean(z_shifts)) if z_shifts else 0.0,
            "avg_rel_diff_std": float(np.mean(std_rel_diffs)) if std_rel_diffs else 0.0,
        },
    }
