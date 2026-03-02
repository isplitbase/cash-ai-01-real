from __future__ import annotations
import json
import os
import subprocess
import tempfile
import shutil
from pathlib import Path
from typing import Any, Dict


def _format_kouseihi_two_decimals(obj: Any) -> Any:
    """Force 前々期構成比/前期構成比/今期構成比 to string with 2 decimals.
    Applies recursively to list/dict structures. Non-numeric values are left as-is.
    """
    keys = {"前々期構成比", "前期構成比", "今期構成比"}

    if isinstance(obj, list):
        for i, v in enumerate(obj):
            obj[i] = _format_kouseihi_two_decimals(v)
        return obj

    if isinstance(obj, dict):
        for k, v in list(obj.items()):
            if k in keys and v is not None:
                try:
                    # accept int/float/numeric strings
                    num = float(v)
                    obj[k] = f"{num:.2f}"
                except Exception:
                    pass
            else:
                obj[k] = _format_kouseihi_two_decimals(v)
        return obj

    return obj

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # ~/cash-ai-01
ORIGINALS_DIR = PROJECT_ROOT / "app" / "pipeline" / "originals"

def _run(cmd: list[str], cwd: Path, env: Dict[str, str]) -> None:
    p = subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if p.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n--- output ---\n{p.stdout}")

def run_001_002_003(payload: Dict[str, Any]) -> Dict[str, Any]:
    data_json = {
        "BS": payload.get("BS", []),
        "PL": payload.get("PL", []),
        "販売費": payload.get("SGA", []),
        "製造原価": payload.get("MFG", []),
    }

    run_dir = Path(tempfile.mkdtemp(prefix="cashai_", dir="/tmp"))

    try:
        (run_dir / "data.json").write_text(
            json.dumps(data_json, ensure_ascii=False),
            encoding="utf-8"
        )

        api_key = os.environ.get("OPENAI_API_KEY", "")
        env = dict(os.environ)
        if api_key:
            env["OPENAI_API_KEY2"] = api_key

        # cash-ai-01 直下を PYTHONPATH に入れる（google/colabスタブを拾える）
        env["PYTHONPATH"] = str(PROJECT_ROOT)

        _run(["python3", str(ORIGINALS_DIR / "cloab001.py"), "--workdir", str(run_dir)], cwd=ORIGINALS_DIR, env=env)

        # 新cloab001.py（=旧cloab001+002相当）は output.json を生成する想定
        data_path = run_dir / "output.json"
        if not data_path.exists():
            # 念のため旧仕様（003相当）もフォールバック
            data_path = run_dir / "output_updated.json"
        if not data_path.exists():
            raise RuntimeError("output.json / output_updated.json が生成されませんでした。")


        data_obj = json.loads(data_path.read_text(encoding="utf-8"))

        output_path = run_dir / "output.json"
        output_obj = None
        if output_path.exists():
            try:
                output_obj = json.loads(output_path.read_text(encoding="utf-8"))
            except Exception:
                output_obj = None

        _format_kouseihi_two_decimals(data_obj)
        if output_obj is not None:
            _format_kouseihi_two_decimals(output_obj)
        return {"data": data_obj, "output": output_obj}

    finally:
        # /tmp 枯渇防止：必ず掃除
        shutil.rmtree(run_dir, ignore_errors=True)