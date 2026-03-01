# -*- coding: utf-8 -*-
"""cloud-run互換版（cloab001.20260228.py 最小改修）
- runner から --workdir が渡される前提で、入出力を workdir 配下に統一
- Colabマジック(!pip)は Cloud Run では動かないため subprocess での pip に置換（既存コード準拠）
- 書き込みは一時ファイル→os.replace の原子的置換で並行運行時の中途半端な書き込みを回避
"""

import argparse
import os
import sys
import tempfile
import pathlib
from typing import Optional

def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--workdir", default=".", help="入出力ファイルを配置する作業ディレクトリ（runnerが指定）")
    return p.parse_known_args(argv)[0]

_ARGS = _parse_args(sys.argv[1:])
WORKDIR = pathlib.Path(_ARGS.workdir).resolve()

def _p(name: str) -> pathlib.Path:
    return WORKDIR / name

def _atomic_write_bytes(path: pathlib.Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

def _atomic_write_text(path: pathlib.Path, text: str, encoding: str = "utf-8") -> None:
    _atomic_write_bytes(path, text.encode(encoding))

def _atomic_write_with_open(path: pathlib.Path, mode: str, encoding: "Optional[str]" = None, newline: "Optional[str]" = None):
    """with文で使う用：一時ファイルに書いてから os.replace する"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
    f = os.fdopen(fd, mode, encoding=encoding, newline=newline)
    class _Ctx:
        def __enter__(self_inner):
            return f
        def __exit__(self_inner, exc_type, exc, tb):
            try:
                f.flush()
                os.fsync(f.fileno())
            except Exception:
                pass
            f.close()
            if exc_type is None:
                os.replace(tmp, path)
            else:
                try:
                    os.remove(tmp)
                except Exception:
                    pass
            return False
    return _Ctx()


# -*- coding: utf-8 -*-
import json
import re
import csv
from pathlib import Path
from google.colab import userdata
from openai import OpenAI

# -----------------------------
# 1. 共通設定（そのまま使ってください）
# -----------------------------
MODEL = "gpt-4.1-mini"
api_key = userdata.get('OPENAI_API_KEY2')
if not api_key:
    raise RuntimeError("環境変数 OPENAI_API_KEY2（runnerがOPENAI_API_KEYからセット）を設定してください。")
client = OpenAI(api_key=api_key)

DATA_PATH = _p("data.json")
if not DATA_PATH.exists():
    raise FileNotFoundError("data.jsonが見つかりません。")
with DATA_PATH.open(encoding="utf-8") as f:
    source_data = json.load(f)

# 全データを統合するリスト
final_output_list = []

# ============================================================
# 【A】 1〜78行目の処理をここに入れる
# ============================================================

import csv
# -------------------------------------
# openai が未インストール時のみ pip install
# -------------------------------------
try:
    import openai  # noqa: F401
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "openai"])
    import openai  # noqa: F401

import json
import re
from pathlib import Path
from google.colab import userdata
from openai import OpenAI

# -----------------------------
# 設定
# -----------------------------
MODEL = "gpt-4.1-mini"

api_key = userdata.get('OPENAI_API_KEY2')
if not api_key:
    raise RuntimeError("環境変数 OPENAI_API_KEY2（runnerがOPENAI_API_KEYからセット）を設定してください。")

client = OpenAI(api_key=api_key)

# -----------------------------
# data.json を読み込み
# -----------------------------
DATA_PATH = _p("data.json")
if not DATA_PATH.exists():
    raise FileNotFoundError("data.json が見つかりません。")

with DATA_PATH.open(encoding="utf-8") as f:
    source_data = json.load(f)

# ============================================================
# 1. BS＋製造原価（1〜111行）の集計
# ============================================================

# -----------------------------
# システムプロンプト（完全保持）
# -----------------------------
system_prompt = """
あなたは日本の企業の決算書の専門家です。
【最重要ルール：金額の抽出と時系列】
データ抽出の際、時系列の取り違えを防止するため、以下のルールを全行に厳格に適用してください：
1. 「今期」列の金額：必ず data.json の各科目の "今期" -> "金額" から取得すること。
2. 「前期」列の金額：必ず data.json の各科目の "前期" -> "金額" から取得すること。
3. 「前々期」列の金額：必ず data.json の各科目の "前々期" -> "金額" から取得すること。
4. 出力順序の固定：各行は必ず「行番号｜勘定科目｜前々期｜前期｜今期｜区分｜集計方法」の順で出力し、列の順番を絶対に入れ替えないでください。
5. 金額が空文字、null、または存在しない場合は 0 としてください。
【出力フォーマットに関して、次のルールを絶対に守ってください】
1. 出力は 78行のテキストのみとし、それ以外の行や説明文、空行、コメントは一切出力してはいけません。
2. 各行は次の形式とします（カンマではなく全角の縦棒「｜」で区切る）：
   行番号｜勘定科目｜前々期｜前期｜今期｜区分｜集計方法
3. 行番号は 1 〜 78 の整数とし、1 行目は 1、2 行目は 2、…、78行目は 78 です。
4. 「前々期」「前期」「今期」は整数のみとし、カンマ区切りや単位（円、千円など）は付けません。金額が無い場合は 0 とします。
5. 「区分」は、変動費なら "V"、固定費なら "F"、該当しない行は空文字 "" とします。
6. 区切り文字として使用する全角縦棒「｜」は、フィールドの中（特に「集計方法」）では絶対に使わないでください。
7. ヘッダ行は出力してはいけません。
"""

# -----------------------------
# 行定義などの仕様（完全保持）
# -----------------------------
spec_text = """
■目的
与えられた data.json の BS 部分（貸借対照表）および製造原価報告書に相当する情報を、
1〜78 行のフォーマットに集計します。

■前提
- data.json は概ね次のような構造を持つとします（一例）:
  {
    "BS": [
      {
        "勘定科目": "...",
        "分類": "...",  // 例: 流動資産, 固定資産, 流動負債, 固定負債, 純資産, 製造原価, 販管費 など
        "前々期": { "金額": 数値または空文字, "page_no": ... },
        "前期": { "金額": 数値または空文字, "page_no": ... },
        "今期": { "金額": 数値または空文字, "page_no": ... }
      },
      ...
    ],
    "PL": [...],
    "販売費": [...]
  }

- 集計対象は原則として "BS" 配列内のデータです。製造原価・労務費・製造経費なども "BS" 又は別セクションから読み取ってください。
- 金額が空文字の場合は 0 とみなします。
- 「前々期」「前期」「今期」の金額をそれぞれ集計してください。
- 「表記ゆれ」（例：什器備品 / 什器・備品、建物附属設備 / 建物付属設備、賃金 / 工員賃金 / 直接工賃金 など）や漢字違いなどは、あなたの判断で同一科目として扱ってください。
- JSON 内に「合計」行（例：流動資産合計、固定資産合計、資産合計、負債合計、株主資本合計、純資産合計、製造原価合計など）がある場合、それを利用する。自動計算は禁止。

■出力形式（重要）
- あなたの出力は 78 行のテキストのみです。
- 各行の形式：
  行番号｜勘定科目｜今期｜前期｜前々期｜区分｜集計方法
- 区切りはすべて全角の縦棒「｜」とし、フィールド内では使用しないでください。
- 「集計方法」は日本語の文章で構いませんが、「｜」は使用禁止です。
- 「区分」は変動費なら "V"、固定費なら "F"、それ以外の行は ""（空文字）とします。
- 行番号は 1〜78で、行番号順に並べてください。

------------------------------------------------------------
■1〜78 行：BS（貸借対照表）
------------------------------------------------------------

1,現金・預金,現金と預金の合計
 -（例）●●銀行預金、●●信金、など銀行名が入った科目
2,（うち定期預金）,定期預金の合計
3,受取手形,受取手形の合計
4,売掛金,売掛金の合計
5,当座資産(その他),有価証券と1～4に含まれない当座資産
 -（例）預け金
6,当座資産の合計,該当する勘定科目がない場合は1+3+4+5
7,製品・商品,製品・商品の合計
8,原材料,原材料の合計
9,仕掛品,仕掛品の合計、未成工事支出金を含める
10,棚卸資産(その他),製品・商品、原材料、仕掛品以外の棚卸資産の合計（該当する勘定科目が１つの場合はその勘定科目を表示）
11,棚卸資産小計,""

12～19,その他流動資産に属する貸倒引当金以外を1科目ずつ配置
20,貸倒引当金（▲）,貸倒引当金（▲）の合計
21,その他流動資産(その他),12～16行および20行以外の「その他流動資産」に属する科目の合計。
22,その他流動資産小計,12行目から19行目および21行目の合計から、20行目（貸倒引当金）を差し引いた純額を算出してください。
23,流動資産合計,「流動資産合計」の値を data.json から直接転記。再計算不可。
24,建物付属設備,- 以下の関連科目をすべて合算し、一つの「建物付属設備」として集計してください。
  ・「建物」「建物付属設備」「建物附属設備」「建物備品」
  ・「建築物」「建物（減価償却累計額を除く）」
- 建物本体と付属設備が別々に計上されている場合は、その合計値を記載してください。
- 「建物減価償却累計額」などのマイナス科目は、ここには含めず、純粋な取得価額（または帳簿価額）の合算を行ってください。
- 絶対に合算してはいけない科目（構築物、機械装置、車両運搬具、什器・備品、土地）
- 集計方法には「建物、建物附属設備を合算」のように、どの用語を統合したか簡潔に記述してください。
25,構築物,構築物に関係する科目の金額を集計してください。
26,機械装置,機械装置に関係する科目の金額を集計してください。
27,車両運搬具,- 以下の関連科目をすべて合算し、一つの「車両運搬具」として集計してください。
  ・「車両運搬具」「車輛運搬具（旧字体）」
  ・「車両」「車輛」「運搬具」
  ・「車両及び運搬具」「車輛及び運搬具」
- 社用車、トラック、フォークリフトなどの具体的な車両名称が別掲されている場合も、すべてこの行に統合してください。
- 「車両運搬具減価償却累計額」などのマイナス科目は含めず、取得価額（または帳簿価額）の合算を行ってください。
- 集計方法には「車両、車輛運搬具を合算」のように、統合した用語を簡潔に記述してください。
28,什器・備品,- 以下の関連科目をすべて合算し、一つの「什器・備品」として集計してください。
  ・「工具器具備品」「工具・器具・備品」「器具備品」
  ・「什器」「備品」「什器備品」
  ・「事務機器」「事務備品」
- 工具、金型、測定器などが別掲されている場合も、ここに含めてください。
- ただし、「ソフトウェア」や「商標権」などの無形固定資産は含めないでください。
- 「工具器具備品減価償却累計額」などのマイナス科目は含めず、取得価額（または帳簿価額）の合算を行ってください。
- 集計方法には「工具器具備品、什器備品を合算」のように、統合した用語を簡潔に記述してください。
29,土地
30,建設仮勘定
31,その他有形固定資産
32,有形固定資産小計,""
33,無形固定資産小計
------------------------------------------------------------
■34〜42 行：投資その他の資産
------------------------------------------------------------
34,投資有価証券,「投資有価証券」の金額のみ。
35,出資金,「出資金」の金額のみ。
36,保証金,保証金または権利金に該当する科目。
37,その他の投資等,34〜36および38〜41行の【いずれにも該当しない】個別の投資資産のみを合算。
※注意：39行の「保険積立金」など、他の行に抽出した科目をここに含めてはいけません（二重計上禁止）。「投資その他の資産合計」等の合計行の数値も使用禁止です。
38,関係会社株式,関係会社株式の金額。
39,保険積立金,保険積立金、保険掛金、解約返戻金相当額等の保険に関わる金額。
40,貸倒引当金（▲）,投資その他の資産に対応する貸倒引当金をプラスの数値（絶対値）で抽出。
41,長期前払費用,長期前払費用、長期延滞税などの金額。
42,投資等小計,自動計算（34+35+36+37+38+39+41 - 40）。
43,繰延資産
44,固定資産合計,「固定資産合計」の値を data.json から直接転記。再計算不可。
45,資産合計,「資産合計」または「資産の部合計」の値を data.json から直接転記。絶対に内訳から再計算しないでください。
46,支払手形,勘定科目名は必ず「支払手形」として固定し、data.json の「支払手形」に該当する金額をそのまま転記する。該当がない場合は0。
47,買掛金,勘定科目名は必ず「買掛金」として固定し、data.json の「買掛金」に該当する金額をそのまま転記する。該当がない場合は0。
48〜52,流動負債スロット1〜5,
- 「分類＝流動負債」に属する科目のうち、46行（支払手形）、47行（買掛金）、53行（未払法人税）、54行（未払消費税）に該当しない科目を、
  出現順に1科目ずつ配置する。
- 各行には1科目のみを設定し、合算はしない。
- 合計行（例：流動負債合計、流動負債の部合計等）は絶対に含めない。
- 該当科目が不足する場合は、勘定科目は空文字とする（ダブルクォートは出力しない）、金額を0とする。
53,未払法人税,勘定科目名は必ず「未払法人税」として固定し、「未払法人税」「未払法人税等」など法人税に該当する科目の金額を合算して転記する。該当がない場合は0。
54,未払消費税,勘定科目名は必ず「未払消費税」として固定し、「未払消費税」「仮受消費税」等、消費税に該当する科目の金額を合算して転記する。該当がない場合は0。
55,流動負債（その他）,
- 「分類＝流動負債」に属する科目のうち、
  46〜54行のいずれにも配置されなかった残りの科目をすべて合算して計上する。
- 48〜52行に入りきらなかった科目がある場合も、ここに累計する。
- 合計行（例：流動負債合計、流動負債の部合計等）は絶対に含めない。
56,流動負債合計,""
57,固定負債スロット1,BSの「分類=固定負債」から、設備支払手形(59行)以外の固定負債内訳科目を1科目抽出して金額をそのまま入れる。固定負債の合計行（例:「固定負債」「固定負債合計」「固定負債の部合計」等）は二重計上防止のため絶対にスロットに入れない。該当が無い場合は勘定科目を""、金額を0。
58,固定負債スロット2,同上（2科目目）。同じ科目を重複して入れない。該当が無い場合は勘定科目を""、金額を0。
59,設備支払手形は、勘定科目名を必ず「設備支払手形」と出力する。金額が0の場合でも勘定科目名を省略してはいけない（例：59｜設備支払手形｜0｜0｜0｜｜設備支払手形なし）。
60,固定負債スロット3,57,58と同様に固定負債内訳科目を抽出（3科目目）。該当が無い場合は勘定科目を""、金額を0。
61,固定負債スロット4,同上（4科目目）。該当が無い場合は勘定科目を""、金額を0。
62,固定負債スロット5,同上（5科目目）。該当が無い場合は勘定科目を""、金額を0。
63,固定負債スロット6,同上（6科目目）。該当が無い場合は勘定科目を""、金額を0。
64,固定負債合計,BSに存在する固定負債の合計行（例:「固定負債」「固定負債合計」「固定負債の部合計」等）の金額をそのまま取得して出力する。57〜63の合計から再計算してはならない（Python側で検算するため）。該当の合計行が無い場合のみ、57+58+59+60+61+62+63の合計で補完してよい。

65,負債合計,""

66,資本金
67,資本剰余金
68,利益剰余金
69,うち準備金、積立金
70,うち繰越利益剰余金
71,資本等小計,資本等小計が無い場合は"資本金"+"資本剰余金"+"利益剰余金"の合計
72,自己株式（▲）
73,評価換算差額等
74,純資産合計,""
75,純資産・負債合計,""
76,借方／貸方照合（資産合計－純資産・負債合計）
77,受取手形割引高
78,受取手形裏書譲渡高

- 全ての行において、金額が計上される場合は必ず適切な勘定科目名を付与してください。
- 行15〜19などの未使用スロットで、金額が0の場合のみ、勘定科目を "" としても構いません。
"""


# -----------------------------
# 元データ付きのユーザープロンプト（完全保持）
# -----------------------------
user_prompt = (
    "以下が元データ(JSON)です。この BS および製造原価関連データを、直前の仕様にしたがって 1〜78 行に集計してください。\n"
    "出力は必ず 78 行のテキストのみとし、各行を「行番号｜勘定科目｜今期｜前期｜前々期｜区分｜集計方法」の形式で出力してください。\n"
    "ヘッダ行や説明文は絶対に出力しないでください。\n"
    "=== 元データ(JSON) ===\n"
    "<JSON_START>\n"
    + json.dumps(source_data, ensure_ascii=False)
    + "\n<JSON_END>"
)

# -----------------------------
# Responses API 呼び出し
# -----------------------------
response = client.responses.create(
    model=MODEL,
    input=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": spec_text},
        {"role": "user", "content": user_prompt},
    ],
    temperature=0.0,
    max_output_tokens=8192,
)

# -----------------------------
# 出力取得
# -----------------------------
raw_text = ""
for block in response.output:
    for item in block.content:
        if isinstance(item, dict) and item.get("type") == "output_text":
            raw_text += item.get("text", "")
        elif hasattr(item, "type") and item.type == "output_text":
            raw_text += item.text

if not raw_text.strip():
    raise RuntimeError("LLM 出力が取得できませんでした。")

# -----------------------------
# 行抽出
# -----------------------------
lines = []
for line in raw_text.splitlines():
    l = line.strip()
    if re.match(r"^\d{1,3}｜", l):
        lines.append(l)

if len(lines) != 78:
    print(raw_text)
    raise ValueError(f"行数が78行ではありません（{len(lines)}行）。")

# ============================================================
# 1〜78行のみ表示して終了
# ============================================================

print("=== 1〜78行のみ表示 ===")
for l in lines[:78]:
    print(l)

print("=== 79行目以降は出力処理を削除済み ===")

# ============================================================
# 1〜78行のデータ整理と JSON/CSV 出力（行6の再計算ロジック付き）
# ============================================================

rows_1_78_for_csv = []
rows_1_78_for_json = []

# --- Step A: 一旦すべての行を辞書に格納（再計算しやすくするため） ---
row_data_map = {}

for l in lines[:78]:
    parts = l.split("｜", 6)
    if len(parts) != 7:
        continue

    try:
        row_num = int(parts[0])
        val_konki = int(parts[2])
        val_zenki = int(parts[3])
        val_zenzenki = int(parts[4])
    except ValueError:
        row_num = parts[0]
        val_konki, val_zenki, val_zenzenki = 0, 0, 0

    row_data_map[row_num] = {
        "勘定科目": parts[1],
        "今期": val_konki,
        "前期": val_zenki,
        "前々期": val_zenzenki,
        "区分": parts[5],
        "集計方法": parts[6]
    }
# --- Step A2: 1〜78行を必ず存在させる（空行でも出力） ---
for i in range(1, 79):
    if i not in row_data_map:
        row_data_map[i] = {
            "勘定科目": "",
            "今期": 0,
            "前期": 0,
            "前々期": 0,
            "区分": "",
            "集計方法": ""
        }

# --- Step B: 行6（当座資産合計）の補完ロジック修正版 ---
if 6 in row_data_map:
    target = row_data_map[6]

    # data.json 側に存在する「当座資産合計系」名称を優先採用する
    quick_asset_total_names = [
        "当座資産",
        "当座資産計",
        "当座資産小計",
        "当座資産合計",
        "当座資産の合計"
    ]

    found_direct = False

    # source_data["BS"] から直接該当名称を検索
    for item in source_data.get("BS", []):
        name = str(item.get("勘定科目", "")).strip()
        if name in quick_asset_total_names:
            for term in ["今期", "前期", "前々期"]:
                val = to_int_safe_bs(
                    item.get(term, {}).get("金額", 0)
                    if isinstance(item.get(term), dict)
                    else item.get(term, 0)
                )
                row_data_map[6][term] = val
            row_data_map[6]["集計方法"] = "data.json記載の当座資産合計を採用"
            found_direct = True
            break

    # 該当科目が存在しなかった場合のみ Python 再計算
    if not found_direct:
        calc_targets = [1, 3, 4, 5]
        for term in ["今期", "前期", "前々期"]:
            components_sum = sum(
                row_data_map.get(i, {}).get(term, 0)
                for i in calc_targets
            )
            row_data_map[6][term] = components_sum

        row_data_map[6]["集計方法"] = "Python再計算(行1+3+4+5)"

# --- Step B2: 行37（その他の投資等）の二重計上を防止する ---
if 37 in row_data_map:
    # 比較対象の内訳行（34, 35, 36, 38, 39, 41）
    check_rows = [34, 35, 36, 38, 39, 41]
    for term in ["今期", "前期", "前々期"]:
        val_37 = row_data_map[37].get(term, 0)
        for r_num in check_rows:
            other_val = row_data_map.get(r_num, {}).get(term, 0)
            if val_37 != 0 and val_37 == other_val:
                row_data_map[37][term] = 0
                row_data_map[37]["集計方法"] = f"Python側で重複排除(行{r_num}と一致)"
                break

# --- Step B3: 行42（投資等小計）の再計算（二重計上排除後の数値で計算） ---
if 42 in row_data_map:
    for term in ["今期", "前期", "前々期"]:
        new_total = (
            row_data_map.get(34, {}).get(term, 0) +
            row_data_map.get(35, {}).get(term, 0) +
            row_data_map.get(36, {}).get(term, 0) +
            row_data_map.get(37, {}).get(term, 0) +
            row_data_map.get(38, {}).get(term, 0) +
            row_data_map.get(39, {}).get(term, 0) +
            row_data_map.get(41, {}).get(term, 0) -
            row_data_map.get(40, {}).get(term, 0)
        )
        row_data_map[42][term] = new_total
        row_data_map[42]["集計方法"] = "Python側で再計算(34+35+36+37+38+39+41-40)"

# --- Step C: 出力用リストへの変換 ---
# すべてのキーを強制的に int に変換してソートする（文字列が混ざっていても比較可能にする）
for row_num in sorted(row_data_map.keys(), key=lambda x: int(x)):
    d = row_data_map[row_num]

    # JSON用
    rows_1_78_for_json.append({
        "行番号": row_num,
        "勘定科目": d["勘定科目"],
        "前々期": d["前々期"],
        "前期": d["前期"],
        "今期": d["今期"],
        "区分": d["区分"],
        "集計方法": d["集計方法"]
    })

    # CSV用
    rows_1_78_for_csv.append([
        row_num, d["勘定科目"], d["前々期"], d["前期"], d["今期"], d["区分"], d["集計方法"]
    ])

# ============================================================
# 最終的なファイル保存処理
# ============================================================

# 1. 1_78.json の出力
output_json_path = _p("1_78.json")
with _atomic_write_with_open(output_json_path, "w", encoding="utf-8") as f:
    json.dump(rows_1_78_for_json, f, ensure_ascii=False, indent=2)

print(f"✅ {output_json_path} を保存しました。")

# 2. check_1_78.csv の出力（WindowsのExcelで見やすいようにcp932で保存）
output_csv_path = _p("check_1_78.csv")
with _atomic_write_with_open(output_csv_path, "w", encoding="cp932", newline="") as f:
    writer = csv.writer(f)
    # ヘッダー行
    writer.writerow(["行番号", "勘定科目", "前々期", "前期", "今期", "区分", "集計方法"])
    # データ行
    writer.writerows(rows_1_78_for_csv)

print(f"✅ {output_csv_path} を保存しました。")

#

# 1〜78行目の結果を統合用リストに追加します
# ファイルの最後の方にある「rows_1_78_for_json」という名前のリストを代入します
try:
    final_output_list.extend(rows_1_78_for_json)
except NameError:
    print("警告: rows_1_78_for_json が見つかりません。変数名を確認してください。")

# ============================================================
# 【B】 79〜154行目の処理をここに入れる
# ============================================================

import json
import csv
import re
from pathlib import Path
from google.colab import userdata
from openai import OpenAI

# -----------------------------
# 設定
# -----------------------------
MODEL = "gpt-4.1-mini"

# Colab のユーザーシークレットから API キー取得
api_key = userdata.get('OPENAI_API_KEY2')
if not api_key:
    raise RuntimeError("環境変数 OPENAI_API_KEY2（runnerがOPENAI_API_KEYからセット）を設定してください。")

client = OpenAI(api_key=api_key)

# -----------------------------
# data.json を読み込み（パス固定）
# -----------------------------
DATA_PATH = _p("data.json")
if not DATA_PATH.exists():
    raise FileNotFoundError("data.json が見つかりません。Colab に data.json をアップロードしてください。")

with DATA_PATH.open(encoding="utf-8") as f:
    source_data = json.load(f)

# ============================================================
# 共通ユーティリティ関数
# ============================================================

def to_int_safe_bs(s):
    if s is None:
        return 0
    if isinstance(s, (int, float)):
        return int(s)
    s = str(s).replace(",", "").replace(" ", "").replace("　", "")
    if s == "" or s == "-" or s == "ー":
        return 0
    if "△" in s or "▲" in s:
        s = s.replace("△", "").replace("▲", "")
        try:
            return -int(s)
        except ValueError:
            return 0
    try:
        return int(s)
    except ValueError:
        return 0

def _normalize_account_name(name: str) -> str:
    """勘定科目名を正規化する。"""
    name = str(name).strip()
    name = name.replace(" ", "").replace("　", "")  # 全角半角スペース除去
    name = name.replace("・", "").replace("・", "")  # 中点除去
    name = name.replace("勘定科目", "").replace("科目", "")  # 「勘定科目」などの語句を除去
    return name

def _get_amount_triplet(item: dict) -> list:
    """辞書から今期、前期、前々期の金額を整数で取得する。"""
    now_val = to_int_safe_bs(item.get("今期", {}).get("金額", 0) if isinstance(item.get("今期"), dict) else item.get("今期", 0))
    prev_val = to_int_safe_bs(item.get("前期", {}).get("金額", 0) if isinstance(item.get("前期"), dict) else item.get("前期", 0))
    prev2_val = to_int_safe_bs(item.get("前々期", {}).get("金額", 0) if isinstance(item.get("前々期"), dict) else item.get("前々期", 0))
    return [now_val, prev_val, prev2_val]


# ============================================================
# 1. BS＋製造原価（79〜154行）の集計
# ============================================================

# -----------------------------
# システムプロンプト（79〜154行用）
# -----------------------------
system_prompt = """
あなたは日本の企業の決算書の専門家です。
【最重要ルール：金額の抽出と時系列】
データ抽出の際、時系列の取り違えを防止するため、以下のルールを全行に厳格に適用してください：
1. 「今期」列の金額：必ず data.json の各科目の "今期" -> "金額" から取得すること。
2. 「前期」列の金額：必ず data.json の各科目の "前期" -> "金額" から取得すること。
3. 「前々期」列の金額：必ず data.json の各科目の "前々期" -> "金額" から取得すること。
4. 出力順序の固定：各行は必ず「行番号｜勘定科目｜前々期｜前期｜今期｜区分｜集計方法」の順で出力し、列の順番を絶対に入れ替えないでください。
5. 金額が空文字、null、または存在しない場合は 0 としてください。
【出力フォーマットに関して、次のルールを絶対に守ってください】
1. 出力は テキストのみとし、それ以外の行や説明文、空行、コメントは一切出力してはいけません。
2. 各行は次の形式とします（カンマではなく全角の縦棒「｜」で区切る）：
   行番号｜勘定科目｜前々期｜前期｜今期｜区分｜集計方法
3. 行番号は 79〜154 の整数とし、79行目は 79、80行目は 80、…、154行目は 154 です。
4. 「前々期」「前期」「今期」は整数のみとし、カンマ区切りや単位（円、千円など）は付けません。金額が無い場合は 0 とします。
5. 「区分」は、変動費なら "V"、固定費なら "F"、該当しない行は空文字 "" とします。
6. 区切り文字として使用する全角縦棒「｜」は、フィールドの中（特に「集計方法」）では絶対に使わないでください。
7. ヘッダ行は出力してはいけません。
"""

# -----------------------------
# 行定義などの仕様（79〜154行用）
# -----------------------------
spec_text = """
■目的
与えられた data.json の BS 部分（貸借対照表）および製造原価報告書に相当する情報を、
79〜154 行のフォーマットに集計します。

■前提
- data.json は概ね次のような構造を持つとします（一例）:
  {
    "BS": [
      {
        "勘定科目": "...",
        "分類": "...",  // 例: 流動資産, 固定資産, 流動負債, 固定負債, 純資産, 製造原価, 販管費 など
        "前々期": { "金額": 数値または空文字, "page_no": ... },
        "前期": { "金額": 数値または空文字, "page_no": ... },
        "今期": { "金額": 数値または空文字, "page_no": ... }
      },
      ...
    ],
    "PL": [...],
    "販売費": [...]
  }

- 集計対象は原則として "BS" 配列内のデータです。製造原価・労務費・製造経費なども "BS" 又は別セクションから読み取ってください。
- 金額が空文字の場合は 0 とみなします。
- 「今期」「前期」「前々期」の金額をそれぞれ集計してください。
- 「表記ゆれ」（例：什器備品 / 什器・備品、建物附属設備 / 建物付属設備、賃金 / 工員賃金 / 直接工賃金 など）や漢字違いなどは、あなたの判断で同一科目として扱ってください。
- JSON 内に「合計」行（例：流動資産合計、固定資産合計、資産合計、負債合計、株主資本合計、純資産合計、製造原価合計など）がある場合、それを利用する。自動計算は禁止。

■出力形式（重要）
- あなたの出力はテキストのみです。
- 各行の形式：
  行番号｜勘定科目｜前々期｜前期｜今期｜区分｜集計方法
- 区切りはすべて全角の縦棒「｜」とし、フィールド内では使用しないでください。
- 「集計方法」は日本語の文章で構いませんが、「｜」は使用禁止です。
- 「区分」は変動費なら "V"、固定費なら "F"、それ以外の行は ""（空文字）とします。
- 行番号は 79〜154 で、行番号順に並べてください。

------------------------------------------------------------
■79〜80 行：未使用行
------------------------------------------------------------

79, "",0,0,0,"","未使用行のため0とした"
80, "",0,0,0,"","未使用行のため0とした"

# ============================================================
# ★修正：製造原価報告書（81〜111行）は BS/PL を一切参照しない
#      → data.json の「製造原価」配列に記載の科目のみで確定させる
# （禁止事項対応：このブロック以外は変更しない）
# ============================================================

seizo_list = source_data.get("製造原価", [])

# 製造原価の「経費」候補を抽出する際の禁止語（仕様に準拠）
_SEIZO_DENY_WORDS = [
    "合計", "小計", "総計",
    "当期経費", "経費合計", "当期総製造費用", "当期製品製造原価",
    "製造原価",
    "期首", "期末", "仕掛品", "棚卸", "増減"
]

def _name_has_any(name: str, words) -> bool:
    for w in words:
        if w in name:
            return True
    return False

def _seizo_candidates(filter_kind: str = None):
    # 製造原価配列の候補を返す。
    # グローバル変数として seizo_list が存在することを前提としています
    # もしエラーが出る場合は、関数の外で seizo_list = [] などと定義してください
    out = []

    # リストが空、または未定義の場合の対策
    target_list = globals().get('seizo_list', []) if 'seizo_list' not in locals() else seizo_list

    for it in (target_list or []):
        nm_raw = str(it.get("勘定科目", "")).strip()
        if nm_raw == "":
            continue

        bunrui = str(it.get("分類", "")).strip()

        if filter_kind == "経費":
            # 経費スロットや105計算の前提として、分類が明確に「経費/製造経費」のものだけを経費候補とする
            if ("経費" in bunrui) or ("製造経費" in bunrui):
                out.append(it)

        elif filter_kind == "労務費":
            if ("労務" in bunrui) or (bunrui == ""):
                out.append(it)

        elif filter_kind == "材料":
            if ("材料" in bunrui) or (bunrui == ""):
                out.append(it)

        else:
            # filter_kind が None または上記以外の場合は全件追加
            out.append(it)

    return out

def _sum_seizo_by_patterns(items, include_patterns, exclude_patterns=None):
    if exclude_patterns is None:
        exclude_patterns = []

    total = [0, 0, 0]
    matched = []

    for item in (items or []):
        name = _normalize_account_name(item.get("勘定科目", ""))
        if name == "":
            continue

        if any(re.search(ep, name) for ep in exclude_patterns):
            continue

        if not any(re.search(ip, name) for ip in include_patterns):
            continue

        vals = _get_amount_triplet(item)
        for j in range(3):
            total[j] += vals[j]

        matched.append(str(item.get("勘定科目", "")).strip())

    return total, matched


def _set_row_seizo(line_no: int, account_name: str, vals, method: str):
    # 製造原価報告書（81〜111行）は「製造原価配列のみ」で確定させる。
    # 行が未作成の場合でも必ず作成し、既存値（LLM/他表由来）を残さない。
    if line_no not in row_dict:
        row_dict[line_no] = {"行番号": line_no}

    row_dict[line_no]["勘定科目"] = account_name
    row_dict[line_no]["今期"], row_dict[line_no]["前期"], row_dict[line_no]["前々期"] = vals
    row_dict[line_no]["集計方法"] = method


# -----------------------------
# 81〜84 材料費（製造原価配列のみ）
# -----------------------------
# 81 材料棚卸高（期首）
v81, m81 = _sum_seizo_by_patterns(
    _seizo_candidates("材料"),
    include_patterns=[r"期首材料棚卸高", r"材料棚卸高", r"期首材料"],
    exclude_patterns=[r"期末"]
)
_set_row_seizo(81, "材料棚卸高", v81, ("製造原価より: " + "、".join(m81)) if m81 else "製造原価より: 該当なし")

# 82 当期材料仕入高
v82, m82 = _sum_seizo_by_patterns(
    _seizo_candidates("材料"),
    include_patterns=[r"当期材料仕入高", r"材料仕入高", r"原材料仕入", r"材料購入", r"原材料購入", r"購入高", r"仕入"],
    exclude_patterns=[r"期首", r"期末", r"棚卸", r"在庫", r"増減", r"合計", r"小計", r"総計"]
)
_set_row_seizo(82, "当期材料仕入高", v82, ("製造原価より: " + "、".join(m82)) if m82 else "製造原価より: 該当なし")

# 83 期末材料棚卸高
v83, m83 = _sum_seizo_by_patterns(
    _seizo_candidates("材料"),
    include_patterns=[r"期末材料棚卸高", r"期末材料", r"材料棚卸高"],
    exclude_patterns=[r"期首"]
)
_set_row_seizo(83, "期末材料棚卸高", v83, ("製造原価より: " + "、".join(m83)) if m83 else "製造原価より: 該当なし")

# 84 当期材料費（製造原価に「当期材料費/材料費」行があれば優先。無ければ 81+82-83）
v84_direct, m84_direct = _sum_seizo_by_patterns(
    _seizo_candidates("材料"),
    include_patterns=[r"当期材料費", r"材料費"],
    exclude_patterns=[r"合計", r"小計", r"総計"]
)
if (v84_direct[0] != 0 or v84_direct[1] != 0 or v84_direct[2] != 0) and m84_direct:
    _set_row_seizo(84, "当期材料費（Ｖ）", v84_direct, "製造原価より: " + "、".join(m84_direct))
else:
    v84_calc = [v81[j] + v82[j] - v83[j] for j in range(3)]
    _set_row_seizo(84, "当期材料費（Ｖ）", v84_calc, "製造原価のみで計算（81+82-83）")

# -----------------------------
# 85〜89 労務費（製造原価配列のみ）
# -----------------------------
# 85 賃金（賞与以外の給与性を集約）
include_85 = [r"賃金", r"雑給", r"給料", r"給与", r"作業員給与", r"工員賃金", r"直接工賃金", r"臨時", r"パート", r"アルバイト", r"手当", r"役員報酬"]
exclude_85 = [r"賞与", r"退職", r"法定福利", r"福利", r"厚生", r"当期労務費", r"労務費合計", r"合計", r"小計", r"総計"]
v85, m85 = _sum_seizo_by_patterns(_seizo_candidates("労務費"), include_85, exclude_85)
_set_row_seizo(85, "賃金", v85, ("製造原価より: " + "、".join(m85)) if m85 else "製造原価より: 該当なし")

# 86 賞与
v86, m86 = _sum_seizo_by_patterns(
    _seizo_candidates("労務費"),
    include_patterns=[r"賞与", r"賞与手当", r"賞与引当金", r"賞与給付"],
    exclude_patterns=[r"雑給", r"給料", r"給与", r"役員報酬", r"合計", r"小計", r"総計"]
)
_set_row_seizo(86, "賞与", v86, ("製造原価より: " + "、".join(m86)) if m86 else "製造原価より: 該当なし")

# 87 退職金
v87, m87 = _sum_seizo_by_patterns(
    _seizo_candidates("労務費"),
    include_patterns=[r"退職", r"退職金", r"退職給付"],
    exclude_patterns=[r"合計", r"小計", r"総計"]
)
_set_row_seizo(87, "退職金", v87, ("製造原価より: " + "、".join(m87)) if m87 else "製造原価より: 該当なし")

# 88 厚生費（法定福利・福利厚生）
v88, m88 = _sum_seizo_by_patterns(
    _seizo_candidates("労務費"),
    include_patterns=[r"法定福利", r"社会保険", r"健康保険", r"厚生年金", r"労働保険", r"雇用保険", r"福利厚生", r"厚生費"],
    exclude_patterns=[r"賃金", r"給与", r"雑給", r"賞与", r"退職", r"合計", r"小計", r"総計"]
)
_set_row_seizo(88, "厚生費", v88, ("製造原価より: " + "、".join(m88)) if m88 else "製造原価より: 該当なし")

# 89 当期労務費（85〜88の合計：製造原価のみで計算）
v89_calc = [v85[j] + v86[j] + v87[j] + v88[j] for j in range(3)]
_set_row_seizo(89, "当期労務費", v89_calc, "製造原価のみで計算（85+86+87+88）")

# -----------------------------
# 90〜105 製造経費（製造原価配列のみ）
# -----------------------------
# 90 減価償却費
v90, m90 = _sum_seizo_by_patterns(
    _seizo_candidates("経費"),
    include_patterns=[r"減価償却", r"償却費"],
    exclude_patterns=[r"合計", r"小計", r"総計"] + _SEIZO_DENY_WORDS
)
_set_row_seizo(90, "減価償却費", v90, ("製造原価より: " + "、".join(m90)) if m90 else "製造原価より: 該当なし")

# 91 外注加工費
v91, m91 = _sum_seizo_by_patterns(
    _seizo_candidates("経費"),
    include_patterns=[r"外注加工", r"加工外注", r"外注費.*加工", r"外注費\(?加工\)?"],
    exclude_patterns=[r"合計", r"小計", r"総計"] + _SEIZO_DENY_WORDS
)
_set_row_seizo(91, "外注加工費", v91, ("製造原価より: " + "、".join(m91)) if m91 else "製造原価より: 該当なし")

# 92 消耗品費
v92, m92 = _sum_seizo_by_patterns(
    _seizo_candidates("経費"),
    include_patterns=[r"消耗品", r"副資材"],
    exclude_patterns=[r"合計", r"小計", r"総計"] + _SEIZO_DENY_WORDS
)
_set_row_seizo(92, "消耗品費", v92, ("製造原価より: " + "、".join(m92)) if m92 else "製造原価より: 該当なし")

# 93〜104：経費スロット（90〜92以外、禁止語を含む科目は除外）
expense_items = []
for it in _seizo_candidates("経費"):
    nm = str(it.get("勘定科目", "")).strip()
    if nm == "":
        continue
    # 禁止語（合計・期首期末など）を含むものはスロットに入れない
    if _name_has_any(nm, _SEIZO_DENY_WORDS):
        continue
    # ★合計行・総括行は経費スロットに入れない（105で扱う）
    if re.fullmatch(r"(経費|製造原価)", nm):
        continue
    # 90〜92に該当したものは除外（二重計上防止）
    nm_norm = _normalize_account_name(nm)
    already = False
    for pat in (m90 + m91 + m92):
        if _normalize_account_name(pat) == nm_norm and nm_norm != "":
            already = True
            break
    if already:
        continue
    expense_items.append(it)

# スロット 93〜103 に 1 科目ずつ配置
slot_line_nos = list(range(93, 104))  # 93..103
used_names_norm = set()

def _assign_empty_slot(ln: int):
    _set_row_seizo(ln, "", [0, 0, 0], "製造原価より: 該当なし")

assigned = 0
for ln in slot_line_nos:
    # 次の未使用科目を探す
    found = None
    while expense_items:
        cand = expense_items.pop(0)
        cand_name = str(cand.get("勘定科目", "")).strip()
        cand_norm = _normalize_account_name(cand_name)
        if cand_norm == "" or cand_norm in used_names_norm:
            continue
        used_names_norm.add(cand_norm)
        found = cand
        break

    if found is None:
        _assign_empty_slot(ln)
    else:
        vals = _get_amount_triplet(found)
        _set_row_seizo(ln, str(found.get("勘定科目", "")).strip(), vals, "製造原価より: 単独計上")
        assigned += 1

# 104：溢れ対応（残り経費を合算、無ければ0）
remain_total = [0, 0, 0]
remain_names = []
for it in expense_items:
    nm = str(it.get("勘定科目", "")).strip()
    if nm == "":
        continue
    if _name_has_any(nm, _SEIZO_DENY_WORDS):
        continue
    # ★合計行・総括行は経費スロットに入れない（105で扱う）
    if re.fullmatch(r"(経費|製造原価)", nm):
        continue
    nm_norm = _normalize_account_name(nm)
    if nm_norm == "" or nm_norm in used_names_norm:
        continue
    vals = _get_amount_triplet(it)
    for j in range(3):
        remain_total[j] += vals[j]
    remain_names.append(nm)

if remain_names:
    _set_row_seizo(104, "製造経費スロット溢れ対応", remain_total, "製造原価より溢れ分合算: " + "、".join(remain_names))
else:
    _set_row_seizo(104, "", [0, 0, 0], "製造原価より: 該当なし")

# 105：当期経費（製造原価に「当期経費」があればそれを採用。無ければ 90〜104 を合算）
v105_direct, m105_direct = _sum_seizo_by_patterns(
    _seizo_candidates("経費"),
    include_patterns=[r"当期経費"],
    exclude_patterns=[]
)
if (v105_direct[0] != 0 or v105_direct[1] != 0 or v105_direct[2] != 0) and m105_direct:
    _set_row_seizo(105, "当期製造経費", v105_direct, "製造原価より: " + "、".join(m105_direct))
else:
    # 90〜104 の行値（上で製造原価のみで確定済み）を合算
    v105_calc = [0, 0, 0]
    for ln in range(90, 105):
        vv = get_vals(ln)
        for j in range(3):
            v105_calc[j] += vv[j]
    _set_row_seizo(105, "当期製造経費", v105_calc, "製造原価のみで計算（90〜104合計）")

# -----------------------------
# 106〜111 仕掛品・製造原価（製造原価配列のみ）
# -----------------------------
# 106 期首仕掛品
v106, m106 = _sum_seizo_by_patterns(
    _seizo_candidates(),
    include_patterns=[r"期首仕掛品", r"期首.*仕掛", r"期首WIP"],
    exclude_patterns=[]
)
_set_row_seizo(106, "期首仕掛品", v106, ("製造原価より: " + "、".join(m106)) if m106 else "製造原価より: 該当なし")

# 108 期末仕掛品
v108, m108 = _sum_seizo_by_patterns(
    _seizo_candidates(),
    include_patterns=[r"期末仕掛品", r"期末.*仕掛", r"期末WIP"],
    exclude_patterns=[]
)
_set_row_seizo(108, "期末仕掛品", v108, ("製造原価より: " + "、".join(m108)) if m108 else "製造原価より: 該当なし")

# 109 他勘定振替高
v109, m109 = _sum_seizo_by_patterns(
    _seizo_candidates(),
    include_patterns=[r"他勘定振替", r"振替高"],
    exclude_patterns=[]
)
_set_row_seizo(109, "他勘定振替高", v109, ("製造原価より: " + "、".join(m109)) if m109 else "製造原価より: 該当なし")

# 107 小計（製造原価のみで計算：84 + 89 + 105 + 106）
v107_calc = [get_vals(84)[j] + get_vals(89)[j] + get_vals(105)[j] + get_vals(106)[j] for j in range(3)]
_set_row_seizo(107, "小計", v107_calc, "製造原価のみで計算（84+89+105+106）")

# 110 期首-期末仕掛品差額(V) = 106 - 108
v110_calc = [get_vals(106)[j] - get_vals(108)[j] for j in range(3)]
_set_row_seizo(110, "期首-期末仕掛品差額(V)", v110_calc, "製造原価のみで計算（106-108）")

# 111 当期製造原価 = 107 - 108 - 109
v111_calc = [get_vals(107)[j] - get_vals(108)[j] - get_vals(109)[j] for j in range(3)]
_set_row_seizo(111, "当期製造原価", v111_calc, "製造原価のみで計算（107-108-109）")

------------------------------------------------------------
■勘定科目に関する注意
------------------------------------------------------------

- 全ての行において、金額が計上される場合は必ず適切な勘定科目名を付与してください。
- 未使用スロットで、金額が0の場合のみ、勘定科目を "" としても構いません。
"""

# -----------------------------
# 元データ付きのユーザープロンプト（79〜154行用）
# -----------------------------
user_prompt = (
    "以下が元データ(JSON)です。この BS および製造原価関連データを、直前の仕様にしたがって 1〜111 行に集計してください。\n"
    "出力は必ずテキストのみとし、各行を「行番号｜勘定科目｜前々期｜前期｜今期｜区分｜集計方法」の形式で出力してください。\n"
    "ヘッダ行や説明文は絶対に出力しないでください。\n"
    "=== 元データ(JSON) ===\n"
    "<JSON_START>\n"
    + json.dumps(source_data, ensure_ascii=False)
    + "\n<JSON_END>"
)

# ============================================================
# 2-A. PL（112〜120行：売上〜売上総利益）の集計【新規追加】
# ============================================================

system_prompt_pl_112_120 = """
あなたは日本の中小企業の決算書（単体）の専門家です。

出力フォーマットに関して、次のルールを絶対に守ってください：
1. 出力は 9 行のテキストのみとし（112〜120行）、それ以外の行や説明文、空行、コメントは一切出力してはいけません。
2. 各行は次の形式とします（カンマではなく全角の縦棒「｜」で区切る）：
   行番号｜勘定科目｜今期｜前期｜前々期｜区分｜集計方法
3. 行番号は 112 〜 120 の整数とし、112 行目は 112、…、120 行目は 120 です。
4. 「今期」「前期」「前々期」は整数のみとし、カンマ区切りや単位（円、千円など）は付けません。金額が無い場合は 0 とします。
5. 「区分」は、変動費なら "V"、固定費なら "F"、該当しない行は空文字 "" とします。
6. 区切り文字として使用する全角縦棒「｜」は、フィールドの中（特に「集計方法」）では絶対に使わないでください。
7. ヘッダ行（「行番号｜勘定科目｜…」など）は出力してはいけません。1 行目からいきなり「112｜売上高｜…」の形式で始めてください。
8. 9 行ちょうど出力してください。10 行以上や 8 行以下になってはいけません。

以上のフォーマットに違反すると、後段の処理が失敗します。
"""

spec_text_pl_112_120 = """
■目的
与えられた data.json の PL 部分および製造原価情報を用いて、
112〜120 行（売上高〜売上総利益）を集計します。

■前提
- data.json の "PL" 配列には、「売上高」「売上値引」「売上割戻」「売上戻り」「完成工事高」「完成工事原価」「売上原価」「期首商品棚卸高」「期末商品棚卸高」「工事原価」などの科目が含まれていると想定します。
- 金額が空文字の場合は 0 とみなします。
- 「表記ゆれ」はあなたの判断で同一科目として扱ってください（例：売上高／完成工事高、売上原価／完成工事原価 など）。

■行定義（112〜120行）

112,売上高,売上高の合計（売上高の合計は、単純に一番大きな値ではなく、売上値引きなどマイナスされる売上項目がある事を考慮して下さい。）
113,期首製品・商品棚高,期首製品・商品棚高の合計（期首工事棚卸高も含む）
114,商品仕入高,商品仕入高の合計（製品売上原価を含む）※期首棚卸を含めないこと
115,当期製造原価,当期製造原価の合計（完成工事原価を含む）
116,他勘定振替高,製品売上原価、原価算入諸費用、労務費及び他勘定振替高の合計。
117,期末製品・商品棚卸高,期末製品・商品棚卸高の合計（未成工事支出金を含む）
118,期首-期末製品差額(V),期首-期末製品差額(V)の合計。113 − 117　自動計算と明示すること。
119,売上原価,PL記載の「売上原価」をそのまま採用すること
120,売上総利益,PL記載の売上総利益をそのまま採用すること

■各行の具体的な集計ルール

●112 行：売上高
- 最優先：PL 配列に「売上高」という勘定科目が存在する場合は、その金額をそのまま 112 行に採用してください（他科目を足し引きして組み替えない）。
- 「売上高」が存在しない場合のみ、以下のルールで集計して正味売上高を算出してください。

【加算（プラス）する科目】
- 「売上高」「事業収入」「売電収入」「完成工事高」など売上・収入系の科目
- 「ネット売上高」は売上控除ではないため、必ずプラス（加算）として扱う（マイナスにしない）

【控除（マイナス）してよい科目（売上値引き系のみ）】
- 科目名に次の語を含むものだけを売上控除として扱い、マイナス（控除）する：
  「売上値引」「値引」「売上戻り」「戻り」「売上割戻」「割戻」「返品」「リベート」「相殺」「キャンセル」
- ただし「ネット売上高」は上記に該当しても控除扱いにしない（常に加算）。

- 集計方法には、採用した勘定科目（加算/控除）を簡潔に列挙してください。
- 区分は "" とします。


●113 行：期首製品・商品棚高
- 「期首商品棚卸高」「期首製品棚卸高」「期首製品及び商品棚卸高」「期首工事棚卸高」など、
  売上原価計算における期首在庫（商品・製品・工事等）を合算してください。
- 区分は "" とします。

●114 行：商品仕入高
- 114 行は、「期首棚卸高を含まない、当期中に発生した純粋な仕入金額（購入額）」を表す行とする。
- 原則として、PL 配列のうち「分類＝売上原価」に属し、かつ次の条件をすべて満たす科目を対象に集計する。

【114 に含める条件】
- 当期中の「仕入」「購入」「材料購入」「商品購入」等を表す科目であること。
- 当期製造原価・売上原価合計・仕入合計などの合計・調整概念ではないこと。
- 外部からの商品調達・仕入に関する項目をすべて合算（リベート等の調整を含む）

【典型的に 114 に含まれる科目例（名称は例示）】
- 商品材料仕入高、商品仕入高、材料仕入高、原材料仕入高
- ネット仕入高、ネット仕入
- リベート、リベート等収入
- 購入高
※上記は例示であり、名称一致ではなく「当期の仕入（購入）金額」という意味内容で判断する。

【114 に含めてはいけない科目】
- 期首商品棚卸高、期首製品棚卸高
- 期末商品棚卸高、期末製品棚卸高
- 棚卸資産増減、在庫増減
- 当期製造原価、売上原価、売上原価合計
- 仕入合計、差引仕入高等の合計・調整科目

- 集計方法には、勘定科目と金額を記載する。
- 区分は "" とする。


●115 行：当期製造原価
- 製造業の場合は「当期製造原価」「製品製造原価」「当期工事原価」などに該当する科目を合算してください。
- 可能であれば、BS・製造原価パートで計算した行111「当期製造原価」と整合するように金額を合わせてください（JSON に該当科目がある範囲で構いません）。
- 区分は "" とします。

●116 行：他勘定振替高
- 「他勘定振替高」「製造原価振替高」「完成工事原価振替高」などを合算してください。
- 区分は "" とします。

●117 行：期末製品・商品棚卸高
- 「期末商品棚卸高」「期末製品棚卸高」「期末商品及び製品棚卸高」「未成工事支出金」など、
  売上原価計算における期末在庫（商品・製品・工事等）を合算してください。
- 区分は "" とします。

●118 行：期首-期末製品差額(V)
- 原則として、113 行の期首製品・商品棚高から 117 行の期末製品・商品棚卸高を差し引いた金額（113 - 117）を計上してください。
- もし PL 上に「製品棚卸差額」「工事棚卸差額」など同趣旨の科目が存在する場合は、それらを優先的に用いても構いません。
- 区分は "V"（変動費）とします。

●119 行：売上原価合計
- 売上原価全体として「売上原価」「完成工事原価」など PL 上の売上原価科目を合算してください。
- 売上原価の内訳として 113〜118 行で示した構造（期首在庫＋仕入高＋当期製造原価＋他勘定振替高−期末在庫）の考え方と大きく矛盾しないようにしてください。
- PL 上に明確な「売上原価」が存在する場合は、そちらの合計金額を優先し、113〜118 行との関係は「集計方法」で説明してください。
- 区分は "" とします。

●120 行：売上総利益
- 売上総利益は、原則として 112 行「売上高」から 119 行「売上原価合計」を差し引いた金額（売上高−売上原価）としてください。
- PL 上に「売上総利益」「粗利益」「完成工事総利益」などがある場合は、そちらの金額に合わせることを優先して構いません。
- 区分は "" とします。

■注意
- 112〜120 行の「区分」は 118 行のみ "V" で、それ以外の行は "" としてください。
- 9 行すべてを必ず出力し、行番号は 112〜120 の連番にしてください。
"""

user_prompt_pl_112_120 = (
    "以下が元データ(JSON)です。この PL データおよび製造原価データを、直前の仕様にしたがって 112〜120 行に集計してください。\n"
    "出力は必ず 9 行のテキストのみとし、各行を「行番号｜勘定科目｜今期｜前期｜前々期｜区分｜集計方法」の形式で出力してください。\n"
    "ヘッダ行や説明文は絶対に出力しないでください。\n"
    "=== 元データ(JSON) ===\n"
    "<JSON_START>\n"
    + json.dumps(source_data, ensure_ascii=False)
    + "\n<JSON_END>"
)

response_pl_112_120 = client.responses.create(
    model=MODEL,
    input=[
        {"role": "system", "content": system_prompt_pl_112_120},
        {"role": "user", "content": spec_text_pl_112_120},
        {"role": "user", "content": user_prompt_pl_112_120},
    ],
    temperature=0.0,
    max_output_tokens=4096,
)

raw_text_pl_112_120 = ""
for block in response_pl_112_120.output:
    for item in block.content:
        if isinstance(item, dict) and item.get("type") == "output_text":
            raw_text_pl_112_120 += item.get("text", "")
        elif hasattr(item, "type") and item.type == "output_text":
            raw_text_pl_112_120 += item.text

if not raw_text_pl_112_120.strip():
    print("---- デバッグ：response_pl_112_120.output ----")
    print(response_pl_112_120.output)
    raise RuntimeError("PL 112〜120 用 LLM 出力が取得できませんでした。")

lines_pl_112_120 = []
for line in raw_text_pl_112_120.splitlines():
    l = line.strip()
    if re.match(r"^\d{3}｜", l):
        lines_pl_112_120.append(l)

if len(lines_pl_112_120) != 9:
    print("---- デバッグ：raw_text_pl_112_120 ----")
    print(raw_text_pl_112_120)
    raise ValueError(f"PL 112〜120 部分の行数が 9 行ではありません（{len(lines_pl_112_120)} 行でした）。")

rows_pl_112_120 = []
for l in lines_pl_112_120:
    parts = l.split("｜", 6)
    if len(parts) != 7:
        raise ValueError(f"PL 112〜120: 7 フィールドに分割できませんでした: {l}")

    line_no_str, account_name, now_str, prev_str, prev2_str, kubun_str, method = parts

    try:
        line_no = int(line_no_str)
    except ValueError:
        raise ValueError(f"PL 112〜120: 行番号が整数ではありません: {line_no_str}")

    def to_int_safe_pl112(s: str) -> int:
        s = s.strip()
        if s == "":
            return 0
        s = s.replace(",", "")
        return int(s)

    now_val = to_int_safe_pl112(now_str)
    prev_val = to_int_safe_pl112(prev_str)
    prev2_val = to_int_safe_pl112(prev2_str)

    row_obj = {
        "行番号": line_no,
        "勘定科目": account_name.strip(),
        "今期": now_val,
        "前期": prev_val,
        "前々期": prev2_val,
        "区分": kubun_str.strip(),
        "集計方法": method.strip(),
    }
    rows_pl_112_120.append(row_obj)

expected_numbers_pl_112_120 = list(range(112, 121))
actual_numbers_pl_112_120 = sorted(r["行番号"] for r in rows_pl_112_120)
if actual_numbers_pl_112_120 != expected_numbers_pl_112_120:
    raise ValueError(f"PL 112〜120 部分の行番号が 112〜120 の連番になっていません: {actual_numbers_pl_112_120}")

# ============================================================
# 2-B. PL（121〜154行：販管費〜当期利益）の集計（前回どおり）
# ============================================================

system_prompt_pl = """
あなたは日本の中小企業の決算書（単体）の専門家です。

出力フォーマットに関して、次のルールを絶対に守ってください：
1. 出力は 34 行のテキストのみとし（121〜154行）、それ以外の行や説明文、空行、コメントは一切出力してはいけません。
2. 各行は次の形式とします（カンマではなく全角の縦棒「｜」で区切る）：
   行番号｜勘定科目｜今期｜前期｜前々期｜区分｜集計方法
3. 行番号は 121 〜 154 の整数とし、121 行目は 121、122 行目は 122、…、154 行目は 154 です。
4. 「今期」「前期」「前々期」は整数のみとし、カンマ区切りや単位（円、千円など）は付けません。金額が無い場合は 0 とします。
5. 「区分」は、変動費なら "V"、固定費なら "F"、該当しない行は空文字 "" とします。
6. 区切り文字として使用する全角縦棒「｜」は、フィールドの中（特に「集計方法」）では絶対に使わないでください。
7. ヘッダ行（「行番号｜勘定科目｜…」など）は出力してはいけません。1 行目からいきなり「121｜役員報酬｜…」の形式で始めてください。
8. 34 行ちょうど出力してください。35 行以上や 33 行以下になってはいけません。

以上のフォーマットに違反すると、後段の処理が失敗します。
"""

spec_text_pl = """
■目的
与えられた data.json の PL 部分および「販売費」セクションを用いて、
121〜154 行のフォーマットに集計します。

■前提
- data.json は概ね次のような構造を持つとします:
  {
    "BS": [...],
    "PL": [...],
    "販売費": [...]
  }
- 金額が空文字の場合は 0 とみなします。
- 「表記ゆれ」は、あなたの判断で同一科目として扱ってください。

------------------------------------------------------------
■121〜139 行：販売費及び一般管理費の内訳
------------------------------------------------------------

121,役員報酬,役員報酬の合計,F
122,給与・賞与,給与・賞与の合計,F
123,退職金,退職金の合計,F
124,法定福利費・福利厚生費,法定福利費・福利厚生費の合計,F
125,減価償却費,減価償却費の合計,F
126,貸倒償却費,貸倒償却費の合計,F
127,取扱手数料,取扱手数料の合計,F
128,旅費交通費,旅費交通費の合計,F
129,支払手数料,支払手数料の合計,F
130,荷造運賃,荷造運賃の合計,F
131,地代家賃,地代家賃の合計,F
132,保険料,保険料の合計,F
133,租税公課,租税公課の合計,F
134,広告宣伝費,広告宣伝費の合計,F
135,水道光熱費,水道光熱費の合計,F
136,事務用・備品消耗品費,事務用・備品消耗品費の合計,F
137,通信費,通信費の合計,F
138,その他雑費,その他雑費の合計,F
139,販売費及び一般管理費,販売費及び一般管理費,F

- "販売費" 配列の科目を、上記行にマッピングします。
- 行121〜137の定義に該当する科目は、それぞれの行に集約して合算してください。
- 行121〜137のいずれにも該当しない販売費科目は、すべて行138「その他雑費」に集計します。


------------------------------------------------------------
■140〜154 行：営業利益以降の PL
------------------------------------------------------------

140,営業利益,営業利益
141,受取利息・配当,受取利息・配当の合計
142,雑収入,雑収入の合計
143は営業外収入で141,142以外の科目が入る。空の場合は""とする
144は営業外収入で141,142,143以外の科目が入る。複数残っている場合は、営業外収入（その他）として金額を集計する。空の場合は""とする
145,営業外収入合計,営業外収入の合計
146,支払利息割引料,支払利息割引料の合計
147,営業外支出その他,営業外支出その他の合計
148,営業外支出合計,営業外支出の合計
149,経常利益,経常利益の合計
150,特別利益,特別利益の合計
151,特別損失,特別損失の合計
152,税引前当期利益,税引前当期利益の合計
153,法人税等充当額,法人税及び住民税に該当する科目。表記ゆれを考慮すること。
154,当期利益,当期利益の合計

- 行140〜154の「区分」はすべて "" としてください。
- 営業外収入／営業外支出の科目分類は、PL の「営業外収益」「営業外費用」区分を参考にしてください。
"""

user_prompt_pl = (
    "以下が元データ(JSON)です。この PL および販売費データを、直前の仕様にしたがって 121〜154 行に集計してください。\n"
    "出力は必ず 34 行のテキストのみとし、各行を「行番号｜勘定科目｜今期｜前期｜前々期｜区分｜集計方法」の形式で出力してください。\n"
    "ヘッダ行や説明文は絶対に出力しないでください。\n"
    "=== 元データ(JSON) ===\n"
    "<JSON_START>\n"
    + json.dumps(source_data, ensure_ascii=False)
    + "\n<JSON_END>"
)

response_pl = client.responses.create(
    model=MODEL,
    input=[
        {"role": "system", "content": system_prompt_pl},
        {"role": "user", "content": spec_text_pl},
        {"role": "user", "content": user_prompt_pl},
    ],
    temperature=0.0,
    max_output_tokens=4096,
)

raw_text_pl = ""
for block in response_pl.output:
    for item in block.content:
        if isinstance(item, dict) and item.get("type") == "output_text":
            raw_text_pl += item.get("text", "")
        elif hasattr(item, "type") and item.type == "output_text":
            raw_text_pl += item.text

if not raw_text_pl.strip():
    print("---- デバッグ：response_pl.output ----")
    print(response_pl.output)
    raise RuntimeError("PL 用 LLM 出力が取得できませんでした。")

lines_pl = []
for line in raw_text_pl.splitlines():
    l = line.strip()
    if re.match(r"^\d{3}｜", l):
        lines_pl.append(l)

if len(lines_pl) != 34:
    print("---- デバッグ：raw_text_pl ----")
    print(raw_text_pl)
    raise ValueError(f"PL 部分の行頭が『数字｜』の行数が 34 行ではありません（{len(lines_pl)} 行でした）。")

rows_pl = []
for l in lines_pl:
    parts = l.split("｜", 6)
    if len(parts) != 7:
        raise ValueError(f"PL: 7 フィールドに分割できませんでした: {l}")

    line_no_str, account_name, now_str, prev_str, prev2_str, kubun_str, method = parts

    try:
        line_no = int(line_no_str)
    except ValueError:
        raise ValueError(f"PL: 行番号が整数ではありません: {line_no_str}")

    def to_int_safe_pl(s: str) -> int:
        s = s.strip()
        if s == "":
            return 0
        s = s.replace(",", "")
        return int(s)

    now_val = to_int_safe_pl(now_str)
    prev_val = to_int_safe_pl(prev_str)
    prev2_val = to_int_safe_pl(prev2_str)

    row_obj = {
        "行番号": line_no,
        "勘定科目": account_name.strip(),
        "今期": now_val,
        "前期": prev_val,
        "前々期": prev2_val,
        "区分": kubun_str.strip(),
        "集計方法": method.strip(),
    }
    rows_pl.append(row_obj)

expected_numbers_pl = list(range(121, 155))
actual_numbers_pl = sorted(r["行番号"] for r in rows_pl)
if actual_numbers_pl != expected_numbers_pl:
    raise ValueError(f"PL 部分の行番号が 121〜154 の連番になっていません: {actual_numbers_pl}")

# ============================================================
# 1〜154行を統合（rows が存在しないため直接統合）
# ============================================================

all_rows_list = []

# 112〜120
all_rows_list.extend(rows_pl_112_120)

# 121〜154
all_rows_list.extend(rows_pl)

# 行番号辞書化
row_dict = {r["行番号"]: r for r in all_rows_list}

# ============================================================
# 【汎用・解決版】AIの指定（科目名）に基づく動的再集計ロジック（PL行114/115）
# ※（PL正解）から移植
# ============================================================
import re

def dynamic_aggregate_by_llm_method(target_row_idx, f_map, source_recs):
    """
    f_map[target_row_idx] の『集計方法』に記載された科目名を読み取り、
    source_recs（全生データ）から動的に数値を合算する汎用関数
    """
    target_row = f_map.get(target_row_idx)
    if not target_row:
        return

    # 1. AIが指定した集計方法から科目名リストを抽出
    # 例：「商品材料仕入高、ネット仕入高、リベート等収入」 -> ['商品材料仕入高', 'ネット仕入高', 'リベート等収入']
    method_text = target_row.get("集計方法", "")
    # 補助説明（カッコ内）を一旦無視して、読点やスペースで分割
    clean_method = method_text.split(" (")[0]
    keywords = [k.strip() for k in re.split(r'[、, \s]+', clean_method) if k.strip()]

    if not keywords:
        return

    # 2. 合算用変数の初期化
    new_totals = {"前々期": 0, "前期": 0, "今期": 0}
    matched_subjects = []

    # 3. 生データを走査してキーワードに合致するものを集計
    for row in source_recs:
        if not isinstance(row, dict):
            continue

        orig_subject = str(row.get("勘定科目", "")).strip()
        # 比較用に記号を除去
        clean_subject = re.sub(r'[()\s　（）]', '', orig_subject)

        # いずれかのキーワードが部分一致
        for kw in keywords:
            clean_kw = re.sub(r'[()\s　（）]', '', kw)
            if clean_kw and clean_kw in clean_subject:
                # 3期分を加算
                for term in ["前々期", "前期", "今期"]:
                    new_totals[term] += to_int_safe_bs(row.get(term, 0))
                matched_subjects.append(orig_subject)
                break

    # 4. 結果で final_map を上書き
    if matched_subjects:
        f_map[target_row_idx]["前々期"] = new_totals["前々期"]
        f_map[target_row_idx]["前期"] = new_totals["前期"]
        f_map[target_row_idx]["今期"] = new_totals["今期"]
        f_map[target_row_idx]["集計方法"] = "再集計: " + "、".join(matched_subjects)
    else:
        f_map[target_row_idx]["集計方法"] = "再集計: 該当なし"


# ============================================================
# 114・115 を「集計方法（科目名列挙）」で動的再集計
# （PL正解）では 114 を再集計しているため、移植では 114/115 を対象化
# ============================================================

# source_data（data.json）内の各配列（BS/PL/製造原価など）をまとめて走査対象にする
_all_source_records = []
if isinstance(source_data, dict):
    for _k, _v in source_data.items():
        if isinstance(_v, list):
            _all_source_records.extend(_v)

# PL行114（商品仕入高）
dynamic_aggregate_by_llm_method(114, row_dict, _all_source_records)


# ============================================================
# ★修正：製造原価報告書（81〜111行）は、data.json の「製造原価」配列以外から金額を取得しない
#        （BS/PL/LLM出力由来の値が入っていても、ここで必ず上書きする）
#        ※修正対象外のロジックは変更しない
# ============================================================

def _apply_seizo_only_81_111(row_dict, source_data):
    seizo_list = source_data.get("製造原価", [])

    _SEIZO_DENY_WORDS = [
        "合計", "小計", "総計", "当期経費", "経費合計", "当期総製造費用", "当期製品製造原価",
        "期首", "期末", "仕掛品", "棚卸", "増減"
    ]

    def _set_row(line_no, account_name, vals, method):
        if line_no not in row_dict:
            row_dict[line_no] = {"行番号": line_no}
        row_dict[line_no]["行番号"] = line_no
        row_dict[line_no]["勘定科目"] = account_name
        row_dict[line_no]["今期"], row_dict[line_no]["前期"], row_dict[line_no]["前々期"] = vals
        # 製造原価報告書は区分が必要な行のみ設定（不明なら空）
        if "区分" not in row_dict[line_no]:
            row_dict[line_no]["区分"] = ""
        row_dict[line_no]["集計方法"] = method if (method and str(method).strip()) else "該当なし"

    def _norm(s):
        return _normalize_account_name(s)

    def _triplet(item):
        return _get_amount_triplet(item)

    def _sum_by_patterns(items, include_patterns, exclude_patterns=None):
        if exclude_patterns is None:
            exclude_patterns = []
        total = [0, 0, 0]
        matched = []
        for it in (items or []):
            nm = _norm(it.get("勘定科目", ""))
            if nm == "":
                continue
            excluded = False
            for ep in exclude_patterns:
                if re.search(ep, nm):
                    excluded = True
                    break
            if excluded:
                continue
            included = False
            for ip in include_patterns:
                if re.search(ip, nm):
                    included = True
                    break
            if not included:
                continue
            vals = _triplet(it)
            for j in range(3):
                total[j] += vals[j]
            matched.append(str(it.get("勘定科目", "")).strip())
        return total, matched

    def _has_any(raw_name, words):
        s = str(raw_name or "")
        for w in words:
            if w in s:
                return True
        return False

    # -----------------------------
    # 81〜84 材料費（製造原価配列のみ）
    # -----------------------------
    v81, m81 = _sum_by_patterns(
        seizo_list,
        include_patterns=[r"期首材料棚卸高", r"期首.*材料.*棚卸", r"材料棚卸高", r"期首材料"],
        exclude_patterns=[r"期末"]
    )
    _set_row(81, "材料棚卸高", v81, ("製造原価より: " + "、".join(m81)) if m81 else "製造原価より: 該当なし")

    v82, m82 = _sum_by_patterns(
        seizo_list,
        include_patterns=[r"当期材料仕入高", r"材料仕入高", r"原材料仕入", r"材料購入", r"原材料購入", r"購入高", r"仕入"],
        exclude_patterns=[r"期首", r"期末", r"棚卸", r"在庫", r"合計", r"小計", r"総計"]
    )
    _set_row(82, "当期材料仕入高", v82, ("製造原価より: " + "、".join(m82)) if m82 else "製造原価より: 該当なし")

    v83, m83 = _sum_by_patterns(
        seizo_list,
        include_patterns=[r"期末材料棚卸高", r"期末.*材料.*棚卸", r"期末材料", r"材料棚卸高"],
        exclude_patterns=[r"期首"]
    )
    _set_row(83, "期末材料棚卸高", v83, ("製造原価より: " + "、".join(m83)) if m83 else "製造原価より: 該当なし")

    v84_direct, m84_direct = _sum_by_patterns(
        seizo_list,
        include_patterns=[r"当期材料費", r"材料費"],
        exclude_patterns=[r"合計", r"小計", r"総計"]
    )
    if m84_direct and (v84_direct[0] != 0 or v84_direct[1] != 0 or v84_direct[2] != 0):
        _set_row(84, "当期材料費（Ｖ）", v84_direct, "製造原価より: " + "、".join(m84_direct))
    else:
        v84_calc = [v81[j] + v82[j] - v83[j] for j in range(3)]
        _set_row(84, "当期材料費（Ｖ）", v84_calc, "製造原価のみで計算（81+82-83）")
    row_dict[84]["区分"] = "V"

    # -----------------------------
    # 85〜89 労務費（製造原価配列のみ）
    # -----------------------------
    include_85 = [r"賃金", r"雑給", r"給料", r"給与", r"作業員給与", r"工員賃金", r"直接工賃金", r"臨時", r"パート", r"アルバイト", r"手当", r"役員報酬"]
    exclude_85 = [r"賞与", r"退職", r"法定福利", r"福利", r"厚生", r"当期労務費", r"労務費合計", r"合計", r"小計", r"総計"]
    v85, m85 = _sum_by_patterns(seizo_list, include_85, exclude_85)
    _set_row(85, "賃金", v85, ("製造原価より: " + "、".join(m85)) if m85 else "製造原価より: 該当なし")

    v86, m86 = _sum_by_patterns(
        seizo_list,
        include_patterns=[r"賞与", r"賞与手当", r"賞与引当金", r"賞与給付"],
        exclude_patterns=[r"雑給", r"給料", r"給与", r"役員報酬", r"合計", r"小計", r"総計"]
    )
    _set_row(86, "賞与", v86, ("製造原価より: " + "、".join(m86)) if m86 else "製造原価より: 該当なし")

    v87, m87 = _sum_by_patterns(
        seizo_list,
        include_patterns=[r"退職", r"退職金", r"退職給付"],
        exclude_patterns=[r"合計", r"小計", r"総計"]
    )
    _set_row(87, "退職金", v87, ("製造原価より: " + "、".join(m87)) if m87 else "製造原価より: 該当なし")

    v88, m88 = _sum_by_patterns(
        seizo_list,
        include_patterns=[r"法定福利", r"社会保険", r"健康保険", r"厚生年金", r"労働保険", r"雇用保険", r"福利厚生", r"厚生費"],
        exclude_patterns=[r"合計", r"小計", r"総計"]
    )
    _set_row(88, "厚生費", v88, ("製造原価より: " + "、".join(m88)) if m88 else "製造原価より: 該当なし")

    v89_calc = [v85[j] + v86[j] + v87[j] + v88[j] for j in range(3)]
    _set_row(89, "当期労務費", v89_calc, "製造原価のみで計算（85+86+87+88）")

    # 区分（労務費は原則F）
    for ln in [85, 86, 87, 88]:
        row_dict[ln]["区分"] = "F"

    # -----------------------------
    # 90〜104 製造経費（製造原価配列のみ）
    # -----------------------------
    v90, m90 = _sum_by_patterns(
        seizo_list,
        include_patterns=[r"減価償却", r"償却費"],
        exclude_patterns=[r"合計", r"小計", r"総計"]
    )
    _set_row(90, "減価償却費", v90, ("製造原価より: " + "、".join(m90)) if m90 else "製造原価より: 該当なし")

    v91, m91 = _sum_by_patterns(
        seizo_list,
        include_patterns=[r"外注加工", r"加工外注", r"外注費.*加工", r"外注費\(?加工\)?"],
        exclude_patterns=[r"合計", r"小計", r"総計"]
    )
    _set_row(91, "外注加工費", v91, ("製造原価より: " + "、".join(m91)) if m91 else "製造原価より: 該当なし")

    v92, m92 = _sum_by_patterns(
        seizo_list,
        include_patterns=[r"消耗品", r"副資材"],
        exclude_patterns=[r"合計", r"小計", r"総計"]
    )
    _set_row(92, "消耗品費", v92, ("製造原価より: " + "、".join(m92)) if m92 else "製造原価より: 該当なし")

    used_norm = set(_norm(x) for x in (m90 + m91 + m92))

    # 93〜104 に割り当てる候補：合計/小計/期首期末/仕掛品などは除外
    expense_candidates = []
    for it in (seizo_list or []):
        raw_nm = str(it.get("勘定科目", "")).strip()
        nm = _norm(raw_nm)
        if nm == "":
            continue
        if nm in used_norm:
            continue
        bunrui = str(it.get("分類", "")).strip()
        # 90〜105 は「製造経費（経費内訳）」のみを対象とするため、分類が経費以外（例: 労務費/材料）は除外
        # ★分類が空のものは「経費」と断定できないため除外（混入防止）
        if ("経費" not in bunrui) and ("製造経費" not in bunrui):
            continue
        if _has_any(raw_nm, _SEIZO_DENY_WORDS):
            continue
        # ★合計行の「経費」単独名は 93〜104 に入れない（105で扱う）
        if re.fullmatch(r"(経費|製造原価)", raw_nm):
            continue
        # 90-92で拾う典型語はここでは除外（二重計上抑止）
        if re.search(r"減価償却|償却費|外注加工|加工外注|消耗品|副資材", nm):
            continue
        expense_candidates.append(it)

    # 93〜103 に単独配置、足りなければ空
    slot_line_nos = list(range(93, 104))  # 93..103
    used_slot_names = []
    for ln in slot_line_nos:
        if expense_candidates:
            it = expense_candidates.pop(0)
            raw_nm = str(it.get("勘定科目", "")).strip()
            vals = _triplet(it)
            _set_row(ln, raw_nm, vals, "製造原価より: 単独計上")
            used_slot_names.append(raw_nm)
        else:
            _set_row(ln, "", [0, 0, 0], "製造原価より: 該当なし")

    # 104 は溢れ合算
    remain_total = [0, 0, 0]
    remain_names = []
    for it in expense_candidates:
        raw_nm = str(it.get("勘定科目", "")).strip()
        if raw_nm == "":
            continue
        if _has_any(raw_nm, _SEIZO_DENY_WORDS):
            continue
        nm = _norm(raw_nm)
        if nm == "":
            continue
        vals = _triplet(it)
        for j in range(3):
            remain_total[j] += vals[j]
        remain_names.append(raw_nm)

    if remain_names:
        _set_row(104, "製造経費スロット溢れ対応", remain_total, "製造原価より溢れ分合算: " + "、".join(remain_names))
    else:
        _set_row(104, "", [0, 0, 0], "製造原価より: 該当なし")

    # 区分（製造経費は原則V）
    row_dict[90]["区分"] = "F"
    for ln in range(91, 105):
        if ln not in row_dict:
            continue
        row_dict[ln]["区分"] = "V"

    # 105 当期製造経費：直接があれば採用、無ければ 90〜104 合算
    v105_direct, m105_direct = _sum_by_patterns(
        seizo_list,
        include_patterns=[r"当期経費", r"^経費$", r"^製造原価$"],
        exclude_patterns=[]
    )
    if m105_direct and (v105_direct[0] != 0 or v105_direct[1] != 0 or v105_direct[2] != 0):
        _set_row(105, "当期製造経費", v105_direct, "製造原価より: " + "、".join(m105_direct))
    else:
        v105_calc = [0, 0, 0]
        for ln in range(90, 105):
            vv = row_dict.get(ln, {})
            v = [vv.get("今期", 0), vv.get("前期", 0), vv.get("前々期", 0)]
            # row_dict の値は int 想定
            v105_calc[0] += int(v[0] or 0)
            v105_calc[1] += int(v[1] or 0)
            v105_calc[2] += int(v[2] or 0)
        _set_row(105, "当期製造経費", v105_calc, "製造原価のみで計算（90〜104合計）")
    row_dict[105]["区分"] = ""

    # -----------------------------
    # 106〜111 仕掛品・製造原価（製造原価配列のみ）
    # -----------------------------
    v106, m106 = _sum_by_patterns(
        seizo_list,
        include_patterns=[r"期首仕掛品", r"期首.*仕掛", r"期首WIP"],
        exclude_patterns=[]
    )
    _set_row(106, "期首仕掛品", v106, ("製造原価より: " + "、".join(m106)) if m106 else "製造原価より: 該当なし")

    v108, m108 = _sum_by_patterns(
        seizo_list,
        include_patterns=[r"期末仕掛品", r"期末.*仕掛", r"期末WIP"],
        exclude_patterns=[]
    )
    _set_row(108, "期末仕掛品", v108, ("製造原価より: " + "、".join(m108)) if m108 else "製造原価より: 該当なし")

    v109, m109 = _sum_by_patterns(
        seizo_list,
        include_patterns=[r"他勘定振替", r"振替高"],
        exclude_patterns=[]
    )
    _set_row(109, "他勘定振替高", v109, ("製造原価より: " + "、".join(m109)) if m109 else "製造原価より: 該当なし")

    # 107 小計 = 84 + 89 + 105 + 106
    v107_calc = [
        int(row_dict.get(84, {}).get("今期", 0) or 0) + int(row_dict.get(89, {}).get("今期", 0) or 0) + int(row_dict.get(105, {}).get("今期", 0) or 0) + int(row_dict.get(106, {}).get("今期", 0) or 0),
        int(row_dict.get(84, {}).get("前期", 0) or 0) + int(row_dict.get(89, {}).get("前期", 0) or 0) + int(row_dict.get(105, {}).get("前期", 0) or 0) + int(row_dict.get(106, {}).get("前期", 0) or 0),
        int(row_dict.get(84, {}).get("前々期", 0) or 0) + int(row_dict.get(89, {}).get("前々期", 0) or 0) + int(row_dict.get(105, {}).get("前々期", 0) or 0) + int(row_dict.get(106, {}).get("前々期", 0) or 0),
    ]
    _set_row(107, "小計", v107_calc, "製造原価のみで計算（84+89+105+106）")

    # 110 期首-期末仕掛品差額 = 106 - 108
    v110_calc = [
        int(row_dict.get(106, {}).get("今期", 0) or 0) - int(row_dict.get(108, {}).get("今期", 0) or 0),
        int(row_dict.get(106, {}).get("前期", 0) or 0) - int(row_dict.get(108, {}).get("前期", 0) or 0),
        int(row_dict.get(106, {}).get("前々期", 0) or 0) - int(row_dict.get(108, {}).get("前々期", 0) or 0),
    ]
    _set_row(110, "期首-期末仕掛品差額(V)", v110_calc, "製造原価のみで計算（106-108）")
    row_dict[110]["区分"] = "V"

    # 111 当期製造原価 = 107 - 108 - 109
    v111_calc = [
        int(row_dict.get(107, {}).get("今期", 0) or 0) - int(row_dict.get(108, {}).get("今期", 0) or 0) - int(row_dict.get(109, {}).get("今期", 0) or 0),
        int(row_dict.get(107, {}).get("前期", 0) or 0) - int(row_dict.get(108, {}).get("前期", 0) or 0) - int(row_dict.get(109, {}).get("前期", 0) or 0),
        int(row_dict.get(107, {}).get("前々期", 0) or 0) - int(row_dict.get(108, {}).get("前々期", 0) or 0) - int(row_dict.get(109, {}).get("前々期", 0) or 0),
    ]
    _set_row(111, "当期製造原価", v111_calc, "製造原価のみで計算（107-108-109）")

    # 製造原価報告書の合計行（84, 89, 105, 107, 110, 111）は total-row 想定だが、ここでは数値確定のみ
    # 区分の空欄行はそのまま維持（必要なら後段で表示用途に合わせて調整する）

# 81〜111 を必ず製造原価配列のみで上書き確定
_apply_seizo_only_81_111(row_dict, source_data)


def get_vals(line_no):
    r = row_dict.get(line_no, {"今期": 0, "前期": 0, "前々期": 0})
    def extract(val):
        if isinstance(val, dict):
            return to_int_safe_bs(val.get("金額", 0))
        return to_int_safe_bs(val)
    return [extract(r.get("今期", 0)), extract(r.get("前期", 0)), extract(r.get("前々期", 0))]

def set_vals(line_no, vals):
    if line_no in row_dict:
        row_dict[line_no]["今期"], row_dict[line_no]["前期"], row_dict[line_no]["前々期"] = vals
        row_dict[line_no]["集計方法"] = "自動計算"
# ============================================================
# PL原本優先セット関数
# ============================================================

def set_pl_if_exists(line_no, account_names):
    for item in source_data.get("PL", []):
        name = str(item.get("勘定科目", "")).strip()
        if name in account_names:
            vals = [
                to_int_safe_bs(item.get("今期", {}).get("金額", 0)),
                to_int_safe_bs(item.get("前期", {}).get("金額", 0)),
                to_int_safe_bs(item.get("前々期", {}).get("金額", 0))
            ]
            set_vals(line_no, vals)
            row_dict[line_no]["集計方法"] = "PL記載値を採用"
            return True
    return False

def _fmt_triplet(v):
    return f"今期={v[0]} 前期={v[1]} 前々期={v[2]}"

def verify_total(line_no, label, calc_vals, detail_lines=None, note=None):
    json_vals = get_vals(line_no)
    diff = [calc_vals[j] - json_vals[j] for j in range(3)]
    if diff != [0, 0, 0]:
        print(f"---- 検算ログ：{label}({line_no}) 差異 ----")
        print(f"  {line_no}(JSON/LLM): {_fmt_triplet(json_vals)}")
        print(f"  計算値(PY)       : {_fmt_triplet(calc_vals)}")
        print(f"  差異(PY-JSON)    : {_fmt_triplet(diff)}")
        if note: print(f"  注記: {note}")
        if detail_lines:
            print("  内訳:")
            for ln in detail_lines:
                r = row_dict.get(ln, {})
                v = get_vals(ln)
                print(f"    {ln}: {r.get('勘定科目','')} 今期={v[0]} 前期={v[1]} 前々期={v[2]}")
        print("--------------------------------------")

# --- 合計行の計算ロジック実行 ---

# BS各合計
set_vals(6, [sum(x) for x in zip(get_vals(1), get_vals(3), get_vals(4), get_vals(5))])
set_vals(11, [sum(x) for x in zip(get_vals(7), get_vals(8), get_vals(9), get_vals(10))])

res22 = [0, 0, 0]
for i in [12, 13, 14, 15, 16, 17, 18, 19, 21]:
    vals = get_vals(i)
    for j in range(3): res22[j] += vals[j]
v20 = get_vals(20)
for j in range(3): res22[j] -= abs(v20[j])
set_vals(22, res22)

set_vals(23, [sum(x) for x in zip(get_vals(6), get_vals(11), get_vals(22))])

res32 = [0, 0, 0]
for i in range(24, 32):
    v = get_vals(i)
    for j in range(3): res32[j] += v[j]
set_vals(32, res32)

res42 = [0, 0, 0]
for i in [34, 35, 36, 37, 38, 39, 41]:
    vals = get_vals(i)
    for j in range(3): res42[j] += vals[j]
v40 = get_vals(40)
for j in range(3): res42[j] -= abs(v40[j])
set_vals(42, res42)

set_vals(44, [sum(x) for x in zip(get_vals(32), get_vals(33), get_vals(42))])
set_vals(45, [sum(x) for x in zip(get_vals(23), get_vals(44), get_vals(43))])

res56 = [0, 0, 0]
for i in range(46, 56):
    v = get_vals(i)
    for j in range(3): res56[j] += v[j]
set_vals(56, res56)

calc64 = [0, 0, 0]
for i in range(57, 64):
    v = get_vals(i)
    for j in range(3): calc64[j] += v[j]
verify_total(64, "固定負債合計", calc64, range(57, 64))

set_vals(65, [sum(x) for x in zip(get_vals(56), get_vals(64))])
set_vals(71, [sum(x) for x in zip(get_vals(66), get_vals(67), get_vals(68))])
v71, v72, v73 = get_vals(71), get_vals(72), get_vals(73)
set_vals(74, [v71[j] - abs(v72[j]) + v73[j] for j in range(3)])
set_vals(75, [sum(x) for x in zip(get_vals(65), get_vals(74))])
set_vals(76, [get_vals(45)[j] - get_vals(75)[j] for j in range(3)])

# # 85: 賃金再集計
# seizo_list = source_data.get("製造原価", [])
# include_85 = [r"賃金", r"雑給", r"給料", r"給与", r"作業員給与", r"臨時雇", r"パート", r"アルバイト", r"手当"]
# exclude_85 = [r"賞与", r"退職", r"法定福利", r"厚生費", r"福利厚生", r"当期労務費", r"労務費合計"]
# # _sum_seizo_by_patterns -> _sum_bs_by_patterns (関数名の修正と引数合わせ)
# print(_sum_seizo_by_patterns(seizo_list, include_85, exclude_85))

# sum85, matched85 = _sum_seizo_by_patterns(seizo_list, include_85, exclude_85)

# if 85 in row_dict:
#     row_dict[85]["今期"], row_dict[85]["前期"], row_dict[85]["前々期"] = sum85
#     row_dict[85]["集計方法"] = "再集計: " + "、".join(matched85) if matched85 else "再集計: 該当なし"

# set_vals(89, [sum(x) for x in zip(get_vals(85), get_vals(86), get_vals(87), get_vals(88))])

# -------------------------------------------------
# 区分の強制補正（恒久対応）
# 85～88 行は必ず F
# -------------------------------------------------
for rn in range(85, 89):
    if rn not in row_dict:
        row_dict[rn] = {"行番号": rn}
    row_dict[rn]["区分"] = "F"


# 105: 当期製造経費
calc105 = [0, 0, 0]
for ln in range(90, 105):
    v = get_vals(ln)
    for j in range(3): calc105[j] += v[j]
set_vals(105, calc105)

# 107: 小計
set_vals(107, [get_vals(84)[j] + get_vals(89)[j] + get_vals(105)[j] + get_vals(106)[j] for j in range(3)])
row_dict[107]["勘定科目"] = "小計"

set_vals(110, [get_vals(106)[j] - get_vals(108)[j] for j in range(3)])
set_vals(111, [get_vals(107)[j] - get_vals(108)[j] - get_vals(109)[j] for j in range(3)])

# ============================================================
# 114 商品仕入高を強制再集計（売上原価分類のみ対象）
# ※ リベート等収入は data.json 上でマイナス値なので
#    そのまま加算すれば正しい合計になる
# ============================================================

sum114 = {"前々期": 0, "前期": 0, "今期": 0}
matched114 = []

for item in source_data.get("PL", []):

    name = str(item.get("勘定科目", "")).strip()
    bunrui = str(item.get("分類", "")).strip()

    # 売上原価のみ対象
    if bunrui != "売上原価":
        continue

    # 仕入関連のみ対象（合計行は除外）
    if any(keyword in name for keyword in [
        "仕入",
        "購入",
        "商品材料仕入高",
        "ネット仕入",
        "リベート"
    ]) and "合計" not in name:

        for term in ["前々期", "前期", "今期"]:

            term_obj = item.get(term, {})
            if isinstance(term_obj, dict):
                val = to_int_safe_bs(term_obj.get("金額", 0))
            else:
                val = to_int_safe_bs(term_obj)

            sum114[term] += val

        matched114.append(name)

# 行114を上書き
if 114 in row_dict:
    row_dict[114]["前々期"] = sum114["前々期"]
    row_dict[114]["前期"] = sum114["前期"]
    row_dict[114]["今期"] = sum114["今期"]
    row_dict[114]["集計方法"] = "PL売上原価より再集計: " + "、".join(matched114)

# ============================================================
# 116: 他勘定振替高（PL再構築）
# 製品売上原価 + 原価算入諸費用 + 労務費
# ============================================================

sum116 = [0, 0, 0]
matched116 = []

for item in source_data.get("PL", []):
    name = str(item.get("勘定科目", "")).strip()
    bunrui = str(item.get("分類", "")).strip()

    if bunrui == "売上原価" and name in [
        "製品売上原価",
        "原価算入諸費用",
        "労務費"
    ]:
        vals = [
            to_int_safe_bs(item.get("今期", {}).get("金額", 0)),
            to_int_safe_bs(item.get("前期", {}).get("金額", 0)),
            to_int_safe_bs(item.get("前々期", {}).get("金額", 0))
        ]
        for j in range(3):
            sum116[j] += vals[j]

        matched116.append(name)

set_vals(116, sum116)

if matched116:
    row_dict[116]["集計方法"] = "PLより合算: " + "、".join(matched116)
else:
    row_dict[116]["集計方法"] = "該当なし"


# # --- 124: 法定福利費・福利厚生費の修正集計 ---
# # 販管費リスト(rows_pl)から抽出
# # rows_pl を辞書化し、勘定科目名で検索できるように簡易化
# hankan_rows = source_data.get("販売費", []) # 元データから直接取得

# sum124, matched124 = _sum_seizo_by_patterns(hankan_rows, [r"法定福利", r"福利厚生", r"厚生費"], [])

# # 製造原価側に同科目の金額があるか確認（備考出力の判定用）
# sum124_seizo, _ = _sum_seizo_by_patterns(seizo_list, [r"法定福利", r"福利厚生", r"厚生費"], [])

# if 124 in row_dict:
#     row_dict[124]["今期"], row_dict[124]["前期"], row_dict[124]["前々期"] = sum124

#     # 判定：販管費側が0で、製造原価側に金額がある場合
#     is_zero_pl = (sum124[0] == 0 and sum124[1] == 0 and sum124[2] == 0)
#     has_val_seizo = (sum124_seizo[0] != 0 or sum124_seizo[1] != 0 or sum124_seizo[2] != 0)

#     if is_zero_pl and has_val_seizo:
#         row_dict[124]["集計方法"] = "製造原価に集計"
#     else:
#         row_dict[124]["集計方法"] = "再集計(販管費のみ): " + "、".join(matched124) if matched124 else "再集計: 該当なし"

# --- PL残りのロジック ---
set_vals(118, [get_vals(113)[j] - get_vals(117)[j] for j in range(3)])


# ============================================================
# 119: 売上原価（PL原本優先・上書き禁止）
# ============================================================

pl_119_found = False

for item in source_data.get("PL", []):
    name = str(item.get("勘定科目", "")).strip()
    bunrui = str(item.get("分類", "")).strip()

    if name == "売上原価" and bunrui == "売上原価":
        vals = [
            to_int_safe_bs(item.get("今期", {}).get("金額", 0)),
            to_int_safe_bs(item.get("前期", {}).get("金額", 0)),
            to_int_safe_bs(item.get("前々期", {}).get("金額", 0))
        ]
        set_vals(119, vals)
        row_dict[119]["集計方法"] = "PL記載値を採用"
        pl_119_found = True
        break

if not pl_119_found:
    calc119 = [0, 0, 0]
    for i in range(113, 119):
        v = get_vals(i)
        for j in range(3):
            calc119[j] += v[j]
    set_vals(119, calc119)
    row_dict[119]["集計方法"] = "自動計算"

# 120は119確定後に計算
# ============================================================
# 120: 売上総利益（PL原本優先・上書き禁止）
# ============================================================

pl_120_found = False

for item in source_data.get("PL", []):
    name = str(item.get("勘定科目", "")).strip()

    if name == "売上総利益":
        vals = [
            to_int_safe_bs(item.get("今期", {}).get("金額", 0)),
            to_int_safe_bs(item.get("前期", {}).get("金額", 0)),
            to_int_safe_bs(item.get("前々期", {}).get("金額", 0))
        ]
        set_vals(120, vals)
        row_dict[120]["集計方法"] = "PL記載値を採用"
        pl_120_found = True
        break

# PLに存在しない場合のみ自動計算
if not pl_120_found:
    set_vals(120, [
        get_vals(112)[j] - get_vals(119)[j]
        for j in range(3)
    ])
    row_dict[120]["集計方法"] = "自動計算（売上高−売上原価）"


# ============================================================
# 販売費及び一般管理費のみ抽出する関数
# 営業外費用・売上原価などは絶対除外
# ============================================================

def get_hankan_items(source_data):
    valid_items = []
    for item in source_data.get("PL", []):
        bunrui = str(item.get("分類", "")).strip()
        if bunrui == "販売費及び一般管理費":
            valid_items.append(item)
    return valid_items

# ============================================================
# 125: 減価償却費（販売費内訳あり／PL直記載 両対応版）
# ============================================================

sum125 = {"前々期": 0, "前期": 0, "今期": 0}
matched125 = []

def add_values_from_item(item):
    for term in ["前々期", "前期", "今期"]:
        term_obj = item.get(term, {})

        if isinstance(term_obj, dict):
            val = to_int_safe_bs(term_obj.get("金額", 0))
        else:
            val = to_int_safe_bs(term_obj)

        sum125[term] += val

# ------------------------------------------------------------
# ① まず「販売費」配列を確認
# ------------------------------------------------------------

if isinstance(source_data, dict) and "販売費" in source_data:

    for item in source_data.get("販売費", []):

        if not isinstance(item, dict):
            continue

        name = str(item.get("勘定科目", "")).strip()

        if "減価償却" in name:
            add_values_from_item(item)
            matched125.append(name)

# ------------------------------------------------------------
# ② 販売費に無ければ PL を確認
# ------------------------------------------------------------

if not matched125 and isinstance(source_data, dict) and "PL" in source_data:

    for item in source_data.get("PL", []):

        if not isinstance(item, dict):
            continue

        name = str(item.get("勘定科目", "")).strip()

        if "減価償却" in name:
            add_values_from_item(item)
            matched125.append(name)

# ------------------------------------------------------------
# 行125へ反映
# ------------------------------------------------------------

if 125 in row_dict:
    row_dict[125]["前々期"] = sum125["前々期"]
    row_dict[125]["前期"] = sum125["前期"]
    row_dict[125]["今期"] = sum125["今期"]

    if matched125:
        row_dict[125]["集計方法"] = "減価償却費抽出: " + "、".join(matched125)
    else:
        row_dict[125]["集計方法"] = "減価償却費該当なし"

# --- PL合計（139行目）を先に取得 ---
# PLデータ(rows_pl)から「合計」行を探す
pl_total_vals = [0, 0, 0]
for item in rows_pl:
    if "合計" in item.get("勘定科目", ""):
        pl_total_vals = [
            to_int_safe_bs(item.get("今期", 0)),
            to_int_safe_bs(item.get("前期", 0)),
            to_int_safe_bs(item.get("前々期", 0))
        ]
        break

# 139行目にセット
set_vals(139, pl_total_vals)

# --- 138: その他雑費を「差し引き」で算出 ---
# 121〜137までの合計を計算
sum_121_137 = [0, 0, 0]
for i in range(121, 138):
    v = get_vals(i)
    for j in range(3):
        sum_121_137[j] += v[j]

# ============================================================
# 138: その他雑費はPL内訳を直接採用
# 差額計算は絶対に行わない
# ============================================================

set_vals(138, [0, 0, 0])  # 初期化

for item in source_data.get("PL", []):
    name = str(item.get("勘定科目", "")).strip()
    bunrui = str(item.get("分類", "")).strip()

    if name == "その他販売費及び一般管理費" and bunrui == "販売費及び一般管理費":
        vals = [
            to_int_safe_bs(item.get("今期", {}).get("金額", 0)),
            to_int_safe_bs(item.get("前期", {}).get("金額", 0)),
            to_int_safe_bs(item.get("前々期", {}).get("金額", 0))
        ]
        set_vals(138, vals)
        row_dict[138]["集計方法"] = "PL内訳科目を直接採用"
        break


# ============================================================
# 139: 販売費及び一般管理費はPL記載値を最優先
# 自動計算で上書きしない
# ============================================================

pl_139_found = False

for item in source_data.get("PL", []):
    if str(item.get("勘定科目", "")).strip() == "販売費及び一般管理費":
        vals = [
            to_int_safe_bs(item.get("今期", {}).get("金額", 0)),
            to_int_safe_bs(item.get("前期", {}).get("金額", 0)),
            to_int_safe_bs(item.get("前々期", {}).get("金額", 0))
        ]
        set_vals(139, vals)
        row_dict[139]["集計方法"] = "PL記載値を採用"
        pl_139_found = True
        break

# ★ PL値が存在する場合は絶対に再計算しない
if not pl_139_found:
    calc139 = [0, 0, 0]
    for i in range(121, 139):
        v = get_vals(i)
        for j in range(3):
            calc139[j] += v[j]
    set_vals(139, calc139)
    row_dict[139]["集計方法"] = "自動計算（121〜138合計）"

if not set_pl_if_exists(140, ["営業利益"]):
    set_vals(140, [
        get_vals(120)[j] - get_vals(139)[j]
        for j in range(3)
    ])
    row_dict[140]["集計方法"] = "自動計算"

if not set_pl_if_exists(145, ["営業外収益"]):
    set_vals(145, [
        sum(get_vals(i)[j] for i in range(141,145))
        for j in range(3)
    ])
    row_dict[145]["集計方法"] = "自動計算"

# ============================================================
# 143: 営業外収入（その他）＝賃貸料収入のみ
# ============================================================

set_vals(143, [0, 0, 0])

for item in source_data.get("PL", []):
    name = str(item.get("勘定科目", "")).strip()
    bunrui = str(item.get("分類", "")).strip()

    if name == "賃貸料収入" and bunrui == "営業外収益":
        vals = [
            to_int_safe_bs(item.get("今期", {}).get("金額", 0)),
            to_int_safe_bs(item.get("前期", {}).get("金額", 0)),
            to_int_safe_bs(item.get("前々期", {}).get("金額", 0))
        ]
        set_vals(143, vals)
        row_dict[143]["集計方法"] = "賃貸料収入を採用"
        break

# ============================================================
# 147: 営業外支出その他（合計行除外）
# ============================================================

sum147 = [0, 0, 0]
matched147 = []

for item in source_data.get("PL", []):
    name = str(item.get("勘定科目", "")).strip()
    bunrui = str(item.get("分類", "")).strip()

    # 営業外費用のみ対象
    if bunrui != "営業外費用":
        continue

    # 合計行は除外
    if name == "営業外費用":
        continue

    # 支払利息は146へ
    if name == "支払利息・割引料":
        continue

    vals = [
        to_int_safe_bs(item.get("今期", {}).get("金額", 0)),
        to_int_safe_bs(item.get("前期", {}).get("金額", 0)),
        to_int_safe_bs(item.get("前々期", {}).get("金額", 0))
    ]

    for j in range(3):
        sum147[j] += vals[j]

    matched147.append(name)

set_vals(147, sum147)
row_dict[147]["集計方法"] = "営業外費用内訳より合算（合計行除外）"


if not set_pl_if_exists(148, ["営業外費用"]):
    set_vals(148, [
        sum(get_vals(i)[j] for i in range(146,148))
        for j in range(3)
    ])
    row_dict[148]["集計方法"] = "自動計算"

if not set_pl_if_exists(149, ["経常利益"]):
    set_vals(149, [
        get_vals(140)[j] + get_vals(145)[j] - get_vals(148)[j]
        for j in range(3)
    ])
    row_dict[149]["集計方法"] = "自動計算"

if not set_pl_if_exists(152, ["税引前当期純利益"]):
    set_vals(152, [
        get_vals(149)[j] + get_vals(150)[j] - get_vals(151)[j]
        for j in range(3)
    ])
    row_dict[152]["集計方法"] = "自動計算"


# ============================================================
# 153: 法人税等充当額（PL原本優先・表記ゆれ完全対応版）
# ============================================================

sum153 = [0, 0, 0]
matched153 = []

for item in source_data.get("PL", []):

    name = str(item.get("勘定科目", "")).strip()
    bunrui = str(item.get("分類", "")).strip()

    # 正規化（空白・全角スペース除去）
    norm_name = name.replace(" ", "").replace("　", "")

    # --------------------------------------------------------
    # 法人税関連の表記ゆれ対応条件
    # --------------------------------------------------------
    # 「法人」と「税」を含むものを基本対象とする
    # 調整額も含める
    # --------------------------------------------------------

    if (
        ("法人" in norm_name and "税" in norm_name)
        or "法人税等" in norm_name
        or "法人税・住民税及び事業税" in norm_name
        or "法人税及び住民税" in norm_name
        or "法人税等調整額" in norm_name
    ):

        vals = [
            to_int_safe_bs(item.get("今期", {}).get("金額", 0)),
            to_int_safe_bs(item.get("前期", {}).get("金額", 0)),
            to_int_safe_bs(item.get("前々期", {}).get("金額", 0))
        ]

        for j in range(3):
            sum153[j] += vals[j]

        matched153.append(name)

# 行153へ反映
if 153 in row_dict:
    row_dict[153]["前々期"] = sum153[2]
    row_dict[153]["前期"] = sum153[1]
    row_dict[153]["今期"] = sum153[0]

    if matched153:
        row_dict[153]["集計方法"] = "PLより合算: " + "、".join(matched153)
    else:
        row_dict[153]["集計方法"] = "該当なし"


# 出力
final_rows = [row_dict[i] for i in sorted(row_dict.keys())]
with _atomic_write_with_open(_p("79_154.json"), "w", encoding="utf-8") as f:
    json.dump(final_rows, f, ensure_ascii=False, indent=2)
with _atomic_write_with_open(_p("79_154.csv"), "w", encoding="cp932", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["行番号", "勘定科目", "前々期", "前期", "今期", "区分", "集計方法"])
    for row in final_rows:
        writer.writerow([row["行番号"], row["勘定科目"], row["前々期"], row["前期"], row["今期"], row["区分"], row["集計方法"]])

print("販管費の集計範囲を修正し、再計算を完了しました。")

# 79〜154行目の結果を統合用リストに追加します
# ファイルの最後の方にある「row_dict」からデータを取り出します
try:
    for row_num in sorted(row_dict.keys()):
        # すでにfinal_output_listにある行番号と重複しないように追加
        final_output_list.append(row_dict[row_num])
except NameError:
    print("警告: row_dict が見つかりません。変数名を確認してください。")

# ============================================================
# 3. 最終保存（ここがoutput.jsonを作る部分です）
# ============================================================

# 行番号で重複を削除＆ソート
seen_rows = set()
unique_list = []
for item in final_output_list:
    if item["行番号"] not in seen_rows:
        unique_list.append(item)
        seen_rows.add(item["行番号"])

unique_list.sort(key=lambda x: int(x["行番号"]))

# output.json として保存
OUTPUT_FILE = "output.json"
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(unique_list, f, ensure_ascii=False, indent=4)

print(f"完了！ {OUTPUT_FILE} を作成しました（合計 {len(unique_list)} 行）。")

import json
from pathlib import Path

# --- 数値変換ヘルパー (カンマ除去・None対応) ---
def to_f(val):
    try:
        if val is None or val == "": return 0.0
        # 文字列ならカンマを除去、数値ならそのままfloat化
        s_val = str(val).replace(',', '').replace('△', '-').replace('▲', '-')
        return float(s_val)
    except:
        return 0.0

def calc_ratio(val, total):
    if not total or total == 0: return 0.0
    return round((val / total) * 100, 2)


data_map = {int(row["行番号"]): row for row in final_output_list}

def get_v(no, p):
    """特定の行・期間の数値を取得"""
    return to_f(data_map.get(no, {}).get(p, 0))

periods = ["前々期", "前期", "今期"]

# --- 4. 全行の構成比を再計算 (79, 80行の追加処理を削除) ---
sorted_rows = []
for no in sorted(data_map.keys()):
    row = data_map[no]

    # セクションごとの分母設定
    if 1 <= no <= 45:
        denom_no = 45 # 資産基準
    elif 46 <= no <= 78:
        denom_no = 75 # 負債純資産基準
    elif 81 <= no <= 111:
        denom_no = 111 # 製造原価基準
    elif 112 <= no <= 154:
        denom_no = 112 # 売上高基準
    else:
        denom_no = None

    if denom_no and denom_no in data_map:
        for p in periods:
            v = to_f(row.get(p, 0))
            total = to_f(data_map[denom_no].get(p, 0))
            row[f"{p}構成比"] = calc_ratio(v, total)

    sorted_rows.append(row)

# --- 5. 保存 ---
with _atomic_write_with_open(_p("output.json"), "w", encoding="utf-8") as f:
    json.dump(sorted_rows, f, ensure_ascii=False, indent=2)

print("既存行に対する構成比計算が完了しました。")