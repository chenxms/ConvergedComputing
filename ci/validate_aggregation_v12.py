#!/usr/bin/env python3
"""
Validate Aggregation v1.2 JSON outputs against schema and key invariants.

Usage:
  python ci/validate_aggregation_v12.py \
    --schema docs/qa/aggregation_v12_schema.json \
    --files docs/qa/snapshots/*.json

If --files omitted, will search docs/qa/snapshots/*.json
Exits non-zero on any failure.
"""

import argparse
import glob
import json
import math
import sys
from pathlib import Path

import jsonschema


def approx_equal(a: float, b: float, tol: float = 0.01) -> bool:
    try:
        return abs(float(a) - float(b)) <= tol
    except Exception:
        return False


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def validate_schema(data, schema):
    jsonschema.validate(instance=data, schema=schema)


def check_subject_invariants(subject: dict, errors: list):
    name = subject.get("subject_name", "<unknown>")
    typ = subject.get("type")
    if typ not in {"exam", "questionnaire"}:
        errors.append(f"subject[{name}]: invalid type={typ} (must be exam|questionnaire)")

    metrics = subject.get("metrics", {}) or {}
    # Common rank check
    if "rank" in metrics and not isinstance(metrics["rank"], int):
        errors.append(f"subject[{name}]: metrics.rank must be integer")
    if isinstance(metrics.get("rank"), int) and metrics["rank"] < 1:
        errors.append(f"subject[{name}]: metrics.rank must be >= 1")

    if typ == "exam":
        # difficulty/discrimination
        if "difficulty" in metrics:
            d = metrics["difficulty"]
            if not (isinstance(d, (int, float)) and 0 <= d <= 1):
                errors.append(f"subject[{name}]: difficulty out of [0,1]")
        if metrics.get("discrimination") is not None:
            disc = metrics.get("discrimination")
            if not (isinstance(disc, (int, float)) and 0 <= disc <= 1):
                errors.append(f"subject[{name}]: discrimination out of [0,1]")

        # percentiles monotonic
        p10 = metrics.get("p10")
        p50 = metrics.get("p50")
        p90 = metrics.get("p90")
        if all(v is not None for v in (p10, p50, p90)):
            try:
                if not (p10 <= p50 <= p90):
                    errors.append(f"subject[{name}]: percentiles not monotonic (p10<=p50<=p90)")
            except Exception:
                errors.append(f"subject[{name}]: percentiles not comparable")

        # grade distribution sum to ~100
        rates = [metrics.get(k) for k in ("rate_excellent", "rate_good", "rate_pass", "rate_fail")]
        present = [r for r in rates if isinstance(r, (int, float))]
        if present:
            s = sum(present)
            if not approx_equal(s, 100.0, tol=0.05):
                errors.append(f"subject[{name}]: grade rates sum={s} != 100±0.05")
            for r in present:
                if not (0 <= r <= 100):
                    errors.append(f"subject[{name}]: grade rate {r} out of [0,100]")

    elif typ == "questionnaire":
        # score_rate in [0,100]
        if "score_rate" in metrics:
            sr = metrics["score_rate"]
            if not (isinstance(sr, (int, float)) and 0 <= sr <= 100):
                errors.append(f"subject[{name}]: score_rate out of [0,100]")

    # school rankings item shape
    for i, it in enumerate(subject.get("school_rankings") or []):
        rk = it.get("rank")
        if not isinstance(rk, int) or rk < 1:
            errors.append(f"subject[{name}]: school_rankings[{i}].rank must be int>=1")

    # dimensions basic checks
    for i, dim in enumerate(subject.get("dimensions") or []):
        if "score_rate" in dim:
            sr = dim["score_rate"]
            if not (isinstance(sr, (int, float)) and 0 <= sr <= 100):
                errors.append(f"subject[{name}]: dimensions[{i}].score_rate out of [0,100]")
        if "rank" in dim and (not isinstance(dim["rank"], int) or dim["rank"] < 1):
            errors.append(f"subject[{name}]: dimensions[{i}].rank must be int>=1")


def validate_file(path: Path, schema_path: Path) -> list:
    errors: list[str] = []
    try:
        data = load_json(path)
    except Exception as e:
        return [f"{path}: cannot load JSON: {e}"]

    try:
        schema = load_json(schema_path)
    except Exception as e:
        return [f"{path}: cannot load schema {schema_path}: {e}"]

    # schema validation
    try:
        validate_schema(data, schema)
    except jsonschema.ValidationError as ve:
        errors.append(f"{path}: JSON Schema validation failed: {ve.message}")

    # additional invariants
    if data.get("schema_version") != "v1.2":
        errors.append(f"{path}: schema_version must be 'v1.2'")

    agglvl = data.get("aggregation_level")
    subs = data.get("subjects") or []
    if not isinstance(subs, list):
        errors.append(f"{path}: subjects must be array")
    else:
        for subj in subs:
            check_subject_invariants(subj, errors)

    return errors


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default="docs/qa/aggregation_v12_schema.json")
    ap.add_argument("--files", nargs="*", help="JSON files to validate (glob allowed)")
    args = ap.parse_args()

    files: list[str] = []
    if args.files:
        for pat in args.files:
            files.extend(glob.glob(pat))
    else:
        files.extend(glob.glob("docs/qa/snapshots/*.json"))

    files = sorted(set(files))
    if not files:
        print("[qa-v12] No snapshot JSON found. Skipping (non-blocking).", file=sys.stderr)
        return 0

    schema_path = Path(args.schema)
    all_errors: list[str] = []
    for f in files:
        errs = validate_file(Path(f), schema_path)
        if errs:
            all_errors.extend(errs)

    if all_errors:
        print("[qa-v12] Validation failures:", file=sys.stderr)
        for e in all_errors:
            print(" - " + e, file=sys.stderr)
        return 1
    else:
        print(f"[qa-v12] All {len(files)} file(s) passed validation.")
        return 0


if __name__ == "__main__":
    sys.exit(main())

