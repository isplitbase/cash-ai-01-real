
import os
import json
import argparse
from pathlib import Path
import tempfile

def atomic_write_json(path, data):
    tmp_path = str(path) + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)

def calculate_ratios(rows):
    total_prev2 = sum(r.get("前々期", 0) for r in rows)
    total_prev1 = sum(r.get("前期", 0) for r in rows)
    total_now   = sum(r.get("今期", 0) for r in rows)

    for r in rows:
        r["前々期構成比"] = round((r.get("前々期", 0) / total_prev2 * 100) if total_prev2 else 0, 2)
        r["前期構成比"]   = round((r.get("前期", 0) / total_prev1 * 100) if total_prev1 else 0, 2)
        r["今期構成比"]   = round((r.get("今期", 0) / total_now * 100) if total_now else 0, 2)

    return rows

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workdir", required=True)
    args = parser.parse_args()

    workdir = Path(args.workdir)
    input_path = workdir / "data.json"
    output_path = workdir / "output.json"

    if not input_path.exists():
        raise FileNotFoundError(f"{input_path} not found")

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 期待構造: list[dict]
    if not isinstance(data, list):
        raise ValueError("data.json must contain a list of rows")

    processed = calculate_ratios(data)

    atomic_write_json(output_path, processed)

if __name__ == "__main__":
    main()
