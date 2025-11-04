# -*- coding: utf-8 -*-
"""
CT Finder - Booking-style backend (complete)
--------------------------------------------
Endpoints
- GET /api/search
    Query params:
      q, caseid, sex, tumor, age_from, age_to,
      ct_phase, manufacturer, study_type, site_nationality (or site_nat),
      model (or model[] / manufacturer_model),
      sort_by = top|shape_desc|spacing_asc|age_asc|age_desc|id|shape|spacing
      sort_dir = asc|desc
      per_page (default 24), page (default 1)

- GET /api/facets
    fields=ct_phase,manufacturer,year,sex,tumor (subset)
    top_k=6, guarantee=0|1

- GET /api/random
    n=3, k=100, offset=?, recent=csv, scope=filtered|all

- GET /api/health
- GET /        (若 --index 指向 HTML 就送檔，否則返回字串)

Run:
  python nApp.py --meta /path/to/metadata.xlsx --index /path/to/index.html
"""
import os, re, math, argparse
from typing import Any, Dict, Optional, Set, List, Tuple
from datetime import datetime

import numpy as np
import pandas as pd
from flask import Flask, jsonify, request, make_response, send_file
from flask_cors import CORS

# ---------------------------
# CLI
# ---------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--meta", required=True, help="Path to metadata.xlsx")
parser.add_argument("--index", default="", help="Path to index.html (optional)")
parser.add_argument("--host", default="0.0.0.0")
parser.add_argument("--port", default=8888, type=int)

args, _ = parser.parse_known_args()
META_FILE = args.meta
INDEX_FILE = args.index

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=False)

# ---------------------------
# Helpers
# ---------------------------
def _arg(name: str, default=None):
    return request.args.get(name, default)

def _to_int(x) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None

def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _to01_query(x) -> Optional[int]:
    if x is None: return None
    s = str(x).strip().lower()
    if s in ("1","true","yes","y"): return 1
    if s in ("0","false","no","n"): return 0
    return None

def _collect_list_params(names: List[str]) -> List[str]:
    out: List[str] = []
    for n in names:
        if n in request.args:
            out += request.args.getlist(n)
    tmp: List[str] = []
    for s in out:
        if "," in s:
            tmp += [t.strip() for t in s.split(",") if t.strip()]
        else:
            tmp.append(s.strip())
    return [t for t in tmp if t]

def _nan2none(v):
    try:
        if v is None: return None
        if pd.isna(v): return None
    except Exception:
        pass
    return v

def _clean_json_list(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _clean(v):
        if isinstance(v, (np.integer,)):  return int(v)
        if isinstance(v, (np.floating,)): return float(v)
        if isinstance(v, (np.bool_,)):    return bool(v)
        return v
    return [{k: _clean(v) for k, v in d.items()} for d in items]
# ---- Model canonicalization ----
MODEL_ALIASES = {
    # GE
    "lightspeed 16": "LightSpeed 16",
    "lightspeed16": "LightSpeed 16",
    "lightspeed vct": "LightSpeed VCT",
    "lightspeed qx/i": "LightSpeed QX/i",
    "lightspeed pro 16": "LightSpeed Pro 16",
    "lightspeed pro 32": "LightSpeed Pro 32",
    "lightspeed plus": "LightSpeed Plus",
    "lightspeed ultra": "LightSpeed Ultra",
    # Siemens
    "somatom definition as+": "SOMATOM Definition AS+",
    "somatom definition as": "SOMATOM Definition AS",
    "somatom definition flash": "SOMATOM Definition Flash",
    "somatom definition edge": "SOMATOM Definition Edge",
    "somatom force": "SOMATOM Force",
    "somatom go.top": "SOMATOM Go.Top",
    "somatom plus 4": "SOMATOM PLUS 4",
    "somatom scope": "SOMATOM Scope",
    "somatom definition": "SOMATOM Definition",
    "sensation 4": "Sensation 4",
    "sensation 10": "Sensation 10",
    "sensation 16": "Sensation 16",
    "sensation 40": "Sensation 40",
    "sensation 64": "Sensation 64",
    "sensation cardiac 64": "Sensation Cardiac 64",
    "sensation open": "Sensation Open",
    "emotion 16": "Emotion 16",
    "emotion 6 (2007)": "Emotion 6 (2007)",
    "perspective": "Perspective",
    # Philips
    "brilliance 10": "Brilliance 10",
    "brilliance 16": "Brilliance 16",
    "brilliance 16p": "Brilliance 16P",
    "brilliance 40": "Brilliance 40",
    "brilliance 64": "Brilliance 64",
    "ingenuity core 128": "Ingenuity Core 128",
    "iqon - spectral ct": "IQon - Spectral CT",
    "philips ct aura": "Philips CT Aura",
    "precedence 16p": "Precedence 16P",
    # Canon / Toshiba
    "aquilion one": "Aquilion ONE",
    "aquilion": "Aquilion",
    # GE 其他
    "optima ct540": "Optima CT540",
    "optima ct660": "Optima CT660",
    "optima ct520 series": "Optima CT520 Series",
    "revolution ct": "Revolution CT",
    "revolution evo": "Revolution EVO",
    "discovery st": "Discovery ST",
    "discovery ste": "Discovery STE",
    "discovery mi": "Discovery MI",
    "hispeed ct/i": "HiSpeed CT/i",
    # PET/CT
    "biograph128": "Biograph128",
    "biograph 128": "Biograph128",
}

def _canon_letters_digits(s: str) -> str:
    # 把 "LightSpeed16" 變成 "LightSpeed 16"
    s2 = re.sub(r"([A-Za-z])(\d)", r"\1 \2", s)
    s2 = re.sub(r"(\d)([A-Za-z])", r"\1 \2", s2)
    return re.sub(r"\s+", " ", s2).strip()

def canon_model(s: str) -> str:
    if not s: return ""
    base = str(s).strip()
    # 標準化空白/底線/大小寫
    low = re.sub(r"[_\-]+", " ", base).strip().lower()
    low = _canon_letters_digits(low)
    # 套用別名表
    if low in MODEL_ALIASES:
        return MODEL_ALIASES[low]
    # 沒有在別名表時：維持「字母數字分隔 + 每字首大寫」的安全格式
    spaced = _canon_letters_digits(base)
    # 常見廠牌固定大寫
    spaced = re.sub(r"(?i)^somatom", "SOMATOM", spaced)
    spaced = re.sub(r"(?i)^iqon", "IQon", spaced)
    return spaced

# ---------------------------
# Load & normalize
# ---------------------------
def _norm_cols(df_raw: pd.DataFrame) -> pd.DataFrame:
    """標準化欄位，產出搜尋/排序需要的衍生欄位。"""
    df = df_raw.copy()

    # ---- Case ID ----
    case_cols = ["PanTS ID", "PanTS_ID", "case_id", "id", "case", "CaseID"]
    def _first_nonempty(row, cols):
        for c in cols:
            if c in row.index and pd.notna(row[c]) and str(row[c]).strip():
                return str(row[c]).strip(), c
        return "", None

    cases, mapping = [], []
    for _, r in df.iterrows():
        s, c = _first_nonempty(r, case_cols)
        cases.append(s); mapping.append({"case": c} if c else {})
    df["__case_str"] = cases
    df["_orig_cols"] = mapping

    # ---- Tumor -> __tumor01 ----
    def _canon(s: str) -> str: return re.sub(r"[^a-z]+", "", str(s).lower())
    tumor_names = [c for c in df.columns if "tumor" in _canon(c)] or []
    tcol = tumor_names[0] if tumor_names else None

    def _to01_v(v):
        if pd.isna(v): return np.nan
        s = str(v).strip().lower()
        if s in ("1","yes","y","true","t"): return 1
        if s in ("0","no","n","false","f"): return 0
        try:
            iv = int(float(s))
            return 1 if iv == 1 else (0 if iv == 0 else np.nan)
        except Exception:
            return np.nan

    df["__tumor01"] = (df[tcol].map(_to01_v) if tcol else pd.Series([np.nan]*len(df), index=df.index))
    if tcol:
        df["_orig_cols"] = [{**(df["_orig_cols"].iat[i] or {}), "tumor": tcol} for i in range(len(df))]

    # ---- Sex -> __sex ----
    df["__sex"] = df.get("sex", pd.Series([""]*len(df))).astype(str).str.strip().str.upper()
    df["__sex"] = df["__sex"].where(df["__sex"].isin(["F","M"]), "")

    # ---- Generic column finder ----
    def _find_col(prefer, keyword_sets=None):
        for c in prefer:
            if c in df.columns: return c
        if keyword_sets:
            canon_map = {c: re.sub(r"[^a-z0-9]+", "", str(c).lower()) for c in df.columns}
            for c, cs in canon_map.items():
                for ks in keyword_sets:
                    if all(k in cs for k in ks): return c
        return None

    # ---- CT phase -> __ct / __ct_lc ----
    ct_col = _find_col(
        prefer=["ct phase","CT phase","ct_phase","CT_phase","ct"],
        keyword_sets=[["ct","phase"],["phase"]],
    )
    if ct_col:
        df["__ct"] = df[ct_col].astype(str).str.strip()
        df["__ct_lc"] = df["__ct"].str.lower()
        df["_orig_cols"] = [{**(df["_orig_cols"].iat[i] or {}), "ct_phase": ct_col} for i in range(len(df))]
    else:
        df["__ct"], df["__ct_lc"] = "", ""

    # ---- Manufacturer -> __mfr / __mfr_lc ----
    mfr_col = _find_col(
        prefer=["manufacturer","Manufacturer","mfr","MFR","vendor","Vendor","manufacturer name","Manufacturer Name"],
        keyword_sets=[["manufactur"],["vendor"],["brand"],["maker"]],
    )
    if mfr_col:
        df["__mfr"] = df[mfr_col].astype(str).str.strip()
        df["__mfr_lc"] = df["__mfr"].str.lower()
        df["_orig_cols"] = [{**(df["_orig_cols"].iat[i] or {}), "manufacturer": mfr_col} for i in range(len(df))]
    else:
        df["__mfr"], df["__mfr_lc"] = "", ""

        # ---- Manufacturer model -> model / __model_lc ----
    model_col = _find_col(
        prefer=["manufacturer model", "Manufacturer model", "model", "Model"],
        keyword_sets=[["model"]],
    )
    if model_col:
        # 保留原始字串以便追蹤
        df["model_raw"] = df[model_col].astype(str).str.strip()
        # 規則化為標準型號（大小寫、空白、數字黏在一起等）
        df["model"] = df["model_raw"].map(canon_model)
        df["__model_lc"] = df["model"].str.lower()
        df["_orig_cols"] = [
            {**(df["_orig_cols"].iat[i] or {}), "model": model_col}
            for i in range(len(df))
        ]
    else:
        # 以免前端讀不到欄位
        df["model_raw"] = ""
        df["model"] = ""
        df["__model_lc"] = ""

    # ---- Year -> __year_int ----
    year_col = _find_col(prefer=["study year", "Study year", "study_year", "year", "Year"],
                         keyword_sets=[["year"]])
    df["__year_int"] = (
        pd.to_numeric(df[year_col], errors="coerce")
        if year_col else pd.Series([np.nan] * len(df), index=df.index)
    )
    if year_col:
        df["_orig_cols"] = [
            {**(df["_orig_cols"].iat[i] or {}), "year": year_col}
            for i in range(len(df))
        ]

    # ---- Age -> __age ----
    age_col = _find_col(prefer=["age", "Age"], keyword_sets=[["age"]])
    df["__age"] = (
        pd.to_numeric(df[age_col], errors="coerce")
        if age_col else pd.Series([np.nan] * len(df), index=df.index)
    )
    if age_col:
        df["_orig_cols"] = [
            {**(df["_orig_cols"].iat[i] or {}), "age": age_col}
            for i in range(len(df))
        ]

    # ---- Study type -> study_type / __st_lc ----
    st_col = _find_col(
        prefer=["study type", "Study type", "study_type", "Study_type"],
        keyword_sets=[["study", "type"]],
    )
    if st_col:
        df["study_type"] = df[st_col].astype(str)
        df["__st_lc"] = df["study_type"].astype(str).str.strip().str.lower()
        df["_orig_cols"] = [
            {**(df["_orig_cols"].iat[i] or {}), "study_type": st_col}
            for i in range(len(df))
        ]
    else:
        df["study_type"] = ""
        df["__st_lc"] = ""

    # ---- Site nationality -> site_nationality / __sn_lc ----
    sn_col = _find_col(
        prefer=[
            "site nationality", "Site nationality", "site_nationality", "Site_nationality",
            "nationality", "Nationality", "site country", "Site country", "country", "Country"
        ],
        keyword_sets=[["site", "national"], ["nationality"], ["site", "country"], ["country"]],
    )
    if sn_col:
        df["site_nationality"] = df[sn_col].astype(str)
        df["__sn_lc"] = df["site_nationality"].astype(str).str.strip().str.lower()
        df["_orig_cols"] = [
            {**(df["_orig_cols"].iat[i] or {}), "site_nationality": sn_col}
            for i in range(len(df))
        ]
    else:
        df["site_nationality"] = ""
        df["__sn_lc"] = ""

    return df


def _safe_float(x) -> Optional[float]:
    try:
        if x is None: return None
        if isinstance(x, float) and np.isnan(x): return None
        if isinstance(x, str):
            s = x.strip().replace(",", " ")
            if not s: return None
            return float(s)
        return float(x)
    except Exception:
        return None

def _take_first_str(row, cols: List[str]) -> str:
    for c in cols:
        if c in row and pd.notna(row[c]) and str(row[c]).strip():
            return str(row[c]).strip()
    return ""

def _case_key(row) -> int:
    s = _take_first_str(row, ["PanTS ID","PanTS_ID","case_id","id","__case_str"])
    if not s: return 0
    m = re.search(r"(\d+)", str(s))
    return int(m.group(1)) if m else 0

def _parse_3tuple_from_row(row, name_candidates: List[str]) -> List[Optional[float]]:
    # 3 個獨立欄
    for base in name_candidates:
        cx, cy, cz = f"{base}_x", f"{base}_y", f"{base}_z"
        if cx in row and cy in row and cz in row:
            xs = [_safe_float(row[c]) for c in (cx, cy, cz)]
            if all(v is not None for v in xs):
                return xs
    # 單欄字串
    seps = [",", "x", " ", "×", "X", ";", "|"]
    str_cols = []
    for base in name_candidates:
        str_cols += [base, f"{base}_str", base.replace(" ", "_")]
    for c in str_cols:
        if c in row and pd.notna(row[c]):
            s = str(row[c]).strip()
            if not s: continue
            s2 = re.sub(r"[\[\]\(\)\{\}]", " ", s)
            for sep in seps:
                s2 = s2.replace(sep, " ")
            parts = [p for p in s2.split() if p]
            vals = [_safe_float(p) for p in parts[:3]]
            if len(vals) == 3 and all(v is not None for v in vals):
                return vals
    return [None, None, None]

def _spacing_sum(row) -> Optional[float]:
    vals = _parse_3tuple_from_row(row, ["spacing","voxel_spacing","voxel_size","pixel_spacing"])
    if any(v is None for v in vals): return None
    return float(vals[0] + vals[1] + vals[2])

def _shape_sum(row) -> Optional[float]:
    vals = _parse_3tuple_from_row(row, ["shape","dim","size","image_shape","resolution"])
    if any(v is None for v in vals): return None
    return float(vals[0] + vals[1] + vals[2])

def _ensure_sort_cols(df: pd.DataFrame) -> pd.DataFrame:
    if "__case_sortkey" not in df.columns:
        df["__case_sortkey"] = df.apply(_case_key, axis=1)
    if "__spacing_sum" not in df.columns:
        df["__spacing_sum"] = df.apply(_spacing_sum, axis=1)
    if "__shape_sum" not in df.columns:
        df["__shape_sum"] = df.apply(_shape_sum, axis=1)

    # 完整度：Browse 與 top 排序會用到
    need_cols = ["__spacing_sum", "__shape_sum", "__sex", "__age"]
    complete = pd.Series(True, index=df.index)
    for c in need_cols:
        if c not in df.columns:
            complete &= False
        elif c == "__sex":
            complete &= (df[c].astype(str).str.strip() != "")
        else:
            complete &= df[c].notna()
    df["__complete"] = complete
    return df

# load meta
if not os.path.exists(META_FILE):
    raise FileNotFoundError(f"metadata not found: {META_FILE}")
DF_RAW = pd.read_excel(META_FILE)
DF = _norm_cols(DF_RAW)

# ---------------------------
# Filters
# ---------------------------
def apply_filters(base: pd.DataFrame, exclude: Optional[Set[str]] = None) -> pd.DataFrame:
    exclude = exclude or set()
    df = base

    # --- Case ID / keyword（精準匹配） ---
    q = (_arg("q") or _arg("caseid") or "").strip()
    if q and "caseid" not in exclude and "__case_str" in df.columns:
        s = df["__case_str"].astype(str)
        if q.isdigit():
            # 把每列所有數字 token 抓出來，做數值等號；77 不會吃 177/077（前導 0 忽略）
            qq = int(q)
            nums = s.str.findall(r"\d+")
            mask_num = nums.apply(lambda xs: any(int(x) == qq for x in xs))
            # 備援：允許 "Case 77"（不必留可刪）
            patt = rf"(?i)\b(?:case\s*)?{re.escape(q)}\b"
            mask_regex = s.str.contains(patt, na=False, regex=True)
            df = df[mask_num | mask_regex]
        else:
            # 一般文字搜尋（忽略大小寫；避免把查詢當正則）
            df = df[s.str.contains(re.escape(q), na=False, case=False, regex=False)]

    # --- Tumor ---
    tv = _to01_query(_arg("tumor"))
    tnull = _to01_query(_arg("tumor_is_null"))
    if (_arg("tumor", "").strip().lower() == "unknown"):
        tnull, tv = 1, None
    if "__tumor01" in df.columns and "tumor" not in exclude:
        if tnull in (0, 1) and "tumor_is_null" not in exclude:
            df = df[df["__tumor01"].isna()] if tnull == 1 else df[df["__tumor01"].notna()]
        elif tv in (0, 1):
            df = df[df["__tumor01"] == tv]

    # --- Sex（多選 + Unknown）---
    sv_list = _collect_list_params(["sex", "sex[]"])
    snull = _to01_query(_arg("sex_is_null"))
    if not sv_list:
        sv = (_arg("sex", "") or "").strip().upper()
        if sv:
            sv_list = [sv]
    sv_norm = []
    for s_ in sv_list:
        s2 = (s_ or "").strip().upper()
        if s2 in ("M", "F"):
            sv_norm.append(s2)
        elif s2 in ("U", "UNKNOWN"):
            sv_norm.append("UNKNOWN")
    if "__sex" in df.columns and "sex" not in exclude and (sv_norm or snull in (0, 1)):
        ser = df["__sex"].fillna("").str.strip().str.upper()
        take = pd.Series(False, index=df.index)
        vals = [s for s in sv_norm if s in ("F", "M")]
        if vals:
            take |= ser.isin(vals)
        if ("UNKNOWN" in sv_norm) or (snull == 1):
            take |= (ser == "")
        df = df[take]

    # --- Age：支援 age_bin[]（含 90+ / UNKNOWN），否則回退 age_from/age_to ---
    bins = _collect_list_params(["age_bin", "age_bin[]"])
    age_null = _to01_query(_arg("age_is_null"))
    if "__age" in df.columns and bins:
        age_series = pd.to_numeric(df["__age"], errors="coerce")
        mask = pd.Series(False, index=df.index)
        for b in bins:
            s = (b or "").strip()
            m_plus = re.match(r"^\s*(\d+)\s*\+\s*$", s)
            if m_plus:
                lo = int(m_plus.group(1))
                mask |= (age_series >= lo)
                continue
            m_rng = re.match(r"^\s*(\d+)\s*[-–—]\s*(\d+)\s*$", s)
            if m_rng:
                lo, hi = int(m_rng.group(1)), int(m_rng.group(2))
                mask |= age_series.between(lo, hi, inclusive="both")
        if (age_null == 1) or any((t or "").strip().upper() == "UNKNOWN" for t in bins):
            mask |= age_series.isna() | (df["__age"].astype(str).str.strip().str.upper() == "UNKNOWN")
        df = df[mask]
    elif "__age" in df.columns:
        af = _to_float(_arg("age_from")); at = _to_float(_arg("age_to"))
        age_series = pd.to_numeric(df["__age"], errors="coerce")
        if "age_from" not in exclude and af is not None:
            df = df[age_series >= af]
        if "age_to" not in exclude and at is not None:
            df = df[age_series <= at]

    # --- CT phase ---
    ct = (_arg("ct_phase", "") or "").strip().lower()
    ct_list = _collect_list_params(["ct_phase", "ct_phase[]"])
    if ct == "unknown" or any((s or "").lower() == "unknown" for s in ct_list):
        if "__ct" in df.columns:
            s_ct = df["__ct"].astype(str).str.strip().str.lower()
            tokens_null_ct = {'', 'unknown', 'nan', 'n/a', 'na', 'none', '(blank)', '(null)'}
            df = df[df["__ct"].isna() | s_ct.isin(tokens_null_ct)]
    elif (ct or ct_list) and "__ct_lc" in df.columns:
        parts = []
        if ct:
            parts += [p.strip() for p in re.split(r"[;,/]+", ct) if p.strip()]
        parts += [p.strip().lower() for p in ct_list if p.strip()]
        patt = "|".join(re.escape(p) for p in parts)
        df = df[df["__ct_lc"].str.contains(patt, na=False)]

    # --- Manufacturer ---
    m_list = _collect_list_params(["manufacturer", "manufacturer[]", "mfr"])
    m_raw = (_arg("manufacturer", "") or "").strip()
    if m_raw and not m_list:
        m_list = [p.strip() for p in m_raw.split(",") if p.strip()]
    if m_list and "__mfr_lc" in df.columns:
        m_lc = [s.lower() for s in m_list]
        df = df[df["__mfr_lc"].isin(m_lc)]

    # --- Model（canonical；可 fuzzy）---
    model_list = _collect_list_params(["model", "model[]", "manufacturer_model"])
    model_raw = (_arg("model", "") or "").strip()
    if model_raw and not model_list:
        model_list = [p.strip() for p in re.split(r"[;,/|]+", model_raw) if p.strip()]
    if model_list and "__model_lc" in df.columns and "model" not in exclude:
        wants = [canon_model(p).lower() for p in model_list if p]
        wants = [w for w in wants if w]
        fuzzy = str(_arg("model_fuzzy", "0")).lower() in ("1", "true", "yes")
        if fuzzy:
            patt = "|".join(re.escape(w) for w in wants)
            df = df[df["__model_lc"].str.contains(patt, na=False)]
        else:
            df = df[df["__model_lc"].isin(set(wants))]

    # --- Study type ---
    st_list = _collect_list_params(["study_type", "study_type[]"])
    st_raw = (_arg("study_type", "") or "").strip()
    if st_raw and not st_list:
        st_list = [p.strip() for p in re.split(r"[;,/|]+", st_raw) if p.strip()]
    if st_list and "__st_lc" in df.columns and "study_type" not in exclude:
        parts = [p.lower() for p in st_list]
        patt = "|".join(re.escape(p) for p in parts)
        df = df[df["__st_lc"].str.contains(patt, na=False)]

    # --- Site nationality ---
    nat_list = _collect_list_params(["site_nat", "site_nat[]", "site_nationality", "site_nationality[]"])
    nat_raw = (_arg("site_nationality", "") or _arg("site_nat", "") or "").strip()
    if nat_raw and not nat_list:
        nat_list = [p.strip() for p in re.split(r"[;,/|]+", nat_raw) if p.strip()]
    if nat_list and "__sn_lc" in df.columns and "site_nationality" not in exclude:
        parts = [p.lower() for p in nat_list]
        patt = "|".join(re.escape(p) for p in parts)
        df = df[df["__sn_lc"].str.contains(patt, na=False)]

    # --- Year（新增）---
    # 支援 year / year[]（多選精確）、year_from / year_to（範圍）與 year_is_null（Unknown）
    if "year" not in exclude:
        _year_cols_pref = ["__year_int", "study_year", "Study year", "study year", "Year", "year"]
        _found_cols = [c for c in _year_cols_pref if c in df.columns]
        if _found_cols:
            yser = pd.to_numeric(df[_found_cols[0]], errors="coerce")

            # 1) 多選年份
            year_list = _collect_list_params(["year", "year[]"])
            year_raw = (_arg("year", "") or "").strip()
            if year_raw and not year_list:
                year_list = [p.strip() for p in re.split(r"[;,/|]+", year_raw) if p.strip()]

            # 2) 範圍
            y_from = _to_int(_arg("year_from"))
            y_to   = _to_int(_arg("year_to"))

            # 3) Unknown / Null
            y_is_null = _to01_query(_arg("year_is_null"))
            _unk_tokens = {"unknown", "nan", "none", "n/a", "na", "(blank)", "(null)"}
            wants_unknown = (y_is_null == 1) or any(
                (s or "").strip().lower() in _unk_tokens for s in year_list
            )

            mask = pd.Series(True, index=df.index)

            # 多選精確年份
            exact_years = []
            for s in year_list:
                try:
                    exact_years.append(int(s))
                except Exception:
                    pass
            if exact_years:
                mask &= yser.isin(set(exact_years))

            # 範圍條件
            if y_from is not None:
                mask &= (yser >= y_from)
            if y_to is not None:
                mask &= (yser <= y_to)

            # Unknown 合併進來
            if wants_unknown:
                mask = mask | yser.isna()

            df = df[mask]


    return df
    

# ---------------------------
# /api/search
# ---------------------------
@app.get("/api/search")
def api_search():
    df = apply_filters(DF).copy()
    df = _ensure_sort_cols(df)

    # ---- 排序參數 ----
    sort_by  = (_arg("sort_by", "top") or "top").strip().lower()
    sort_dir = (_arg("sort_dir", "asc") or "asc").strip().lower()

    if sort_by in ("top", "quality"):
        by  = ["__complete", "__spacing_sum", "__shape_sum", "__case_sortkey"]
        asc = [False, True, False, True]
    elif sort_by in ("id", "id_asc"):
        by, asc = ["__case_sortkey"], [True]
    elif sort_by == "id_desc":
        by, asc = ["__case_sortkey"], [False]
    elif sort_by in ("shape_desc", "shape"):
        by, asc = ["__shape_sum", "__case_sortkey"], [False, True]
    elif sort_by in ("spacing_asc", "spacing"):
        by, asc = ["__spacing_sum", "__case_sortkey"], [True, True]
    elif sort_by == "age_asc":
        by, asc = ["__age", "__case_sortkey"], [True, True]
    elif sort_by == "age_desc":
        by, asc = ["__age", "__case_sortkey"], [False, True]
    else:
        key_map = {"id": "__case_sortkey", "spacing": "__spacing_sum", "shape": "__shape_sum"}
        k = key_map.get(sort_by, "__case_sortkey")
        by, asc = [k, "__case_sortkey"], [(sort_dir != "desc"), True]

    # ---- 排序 ----
    df = df.sort_values(by=by, ascending=asc, na_position="last", kind="mergesort")

    # ---- 分頁：注意 total 先算完篩選後的完整筆數 ----
    total    = int(len(df))
    page     = max(_to_int(_arg("page", "1")) or 1, 1)
    per_page = _to_int(_arg("per_page", "10000")) or 24
    per_page = max(1, min(per_page, 1_000_000))

    pages = max(1, int(math.ceil(total / per_page)))
    page  = max(1, min(page, pages))
    start, end = (page - 1) * per_page, (page - 1) * per_page + per_page

    # ---- 轉成前端想要的 items ----
    items = [_row_to_item(r) for _, r in df.iloc[start:end].iterrows()]
    items = _clean_json_list(items)

    return jsonify({
        "items": items,         # ← 前端只讀這個渲染卡片
        "total": total,         # ← 正確的最終數量
        "page": page,
        "per_page": per_page,
        "query": request.query_string.decode(errors="ignore") or ""
    })



# ---------------------------
# /api/facets
# ---------------------------

def _facet_counts_with_unknown(df: pd.DataFrame, col_key: str, top_k: int = 6) -> Dict[str, Any]:
    """Compute facet rows + unknown count, with robust handling for NaN/strings."""
    rows: List[Dict[str, Any]] = []
    unknown: int = 0

    key_to_col = {
        "ct_phase": ("__ct", str),
        "manufacturer": ("__mfr", str),
        "year": ("__year_int", int),
        "sex": ("__sex", str),
        "tumor": ("__tumor01", int),
        "model": ("model", str),
        "study_type": ("study_type", str),
        "site_nat": ("site_nationality", str),
        "site_nationality": ("site_nationality", str),
    }
    if col_key not in key_to_col:
        return {"rows": [], "unknown": 0}

    col_name, _typ = key_to_col[col_key]
    if col_name not in df.columns:
        return {"rows": [], "unknown": 0}

    ser = df[col_name]

    # ---- Year：數值化、NaN 視為 unknown ----
    if col_key == "year":
        s_num = pd.to_numeric(ser, errors="coerce")
        unknown = int(s_num.isna().sum())
        vc = s_num.dropna().astype(int).value_counts()
        rows = [{"value": int(v), "count": int(c)} for v, c in vc.items()]
        rows.sort(key=lambda x: (-x["count"], x["value"]))
        if top_k and top_k > 0:
            rows = rows[:top_k]
        return {"rows": rows, "unknown": unknown}

    # ---- 其他欄位：把空字串/unknown 類型歸入 unknown ----
    s_str = ser.astype(str).str.strip()
    s_lc = s_str.str.lower()
    unknown_mask = ser.isna() | (s_str == "") | (s_lc.isin({"unknown", "nan", "none", "n/a", "na"}))
    unknown = int(unknown_mask.sum())

    vals = ser[~unknown_mask]
    vc = vals.value_counts(dropna=False)

    tmp_rows: List[Dict[str, Any]] = []
    for v, c in vc.items():
        if col_key == "tumor":
            # tumor 僅接受 0/1
            try:
                iv = int(v)
            except Exception:
                continue
            if iv not in (0, 1):
                continue
            tmp_rows.append({"value": iv, "count": int(c)})
        else:
            tmp_rows.append({"value": v, "count": int(c)})

    # 排序：count desc，再 value 升（字串比較避免型別問題）
    tmp_rows.sort(key=lambda x: (-x["count"], str(x["value"])))
    if top_k and top_k > 0:
        tmp_rows = tmp_rows[:top_k]

    rows = tmp_rows
    return {"rows": rows, "unknown": unknown}


def _prune_zero_rows(rows: List[Dict[str, Any]], keep_zero: bool) -> List[Dict[str, Any]]:
    """依需求濾掉 count<=0；當 keep_zero=True（對應 guarantee=1）則不濾。"""
    if keep_zero:
        return rows
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        try:
            c = int(r.get("count") or 0)
        except Exception:
            c = 0
        if c > 0:
            out.append(r)
    return out


@app.get("/api/facets")
def api_facets():
    try:
        fields_raw = (_arg("fields","ct_phase,manufacturer") or "").strip()
        fields = [f.strip().lower() for f in fields_raw.split(",") if f.strip()]

        valid  = {
            "ct_phase","manufacturer","year","sex","tumor",
            "model","study_type","site_nat","site_nationality"
        }
        fields = [f for f in fields if f in valid] or ["ct_phase","manufacturer"]
        top_k  = _to_int(_arg("top_k","6")) or 6
        guarantee = (_arg("guarantee","0") or "0").strip().lower() in ("1","true","yes","y")

        # 先應用目前的過濾條件
        df_now = apply_filters(DF)
        base_for_ranges = df_now if len(df_now) else DF

        facets: Dict[str, List[Dict[str, Any]]] = {}
        unknown_counts: Dict[str, int] = {}

        # 為每個 facet 準備自我排除的條件（避免自我影響）
        exclude_map = {
            "ct_phase": {"ct_phase"},
            "manufacturer": {"manufacturer","mfr_is_null","manufacturer_is_null"},
            "year": {"year_from","year_to"},
            "sex": {"sex"},
            "tumor": {"tumor"},
            "model": {"model"},
            "study_type": {"study_type"},
            "site_nat": {"site_nat","site_nationality"},
            "site_nationality": {"site_nat","site_nationality"},
        }

        for f in fields:
            ex = exclude_map.get(f, set())
            # 若 guarantee=1 且目前篩完為空，改用全量 DF 以「保證列出所有可能值」
            src = (DF if (guarantee and len(df_now) == 0) else df_now)
            df_facet = apply_filters(src, exclude=ex)
            res = _facet_counts_with_unknown(df_facet, f, top_k=top_k)

            # guarantee=0 時砍掉 count<=0 的項目
            rows = _prune_zero_rows(res.get("rows") or [], keep_zero=guarantee)
            facets[f] = rows
            unknown_counts[f] = int(res.get("unknown") or 0)

        # 年齡/年份範圍（原樣保留）
        def _minmax(series: pd.Series):
            s = series.dropna()
            if not len(s): return (None, None)
            return (float(s.min()), float(s.max()))

        age_min = age_max = None
        year_min = year_max = None
        if "__age" in base_for_ranges:
            age_min, age_max = _minmax(base_for_ranges["__age"])
        if "__year_int" in base_for_ranges:
            yr = base_for_ranges["__year_int"].dropna().astype(int)
            if len(yr):
                year_min, year_max = int(yr.min()), int(yr.max())

        return jsonify({
            "facets": facets,
            "unknown_counts": unknown_counts,
            "age_range": {"min": age_min, "max": age_max},
            "year_range": {"min": year_min, "max": year_max},
            "total": int(len(df_now)),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# ---------------------------
# /api/random (Browse)
# ---------------------------
@app.get("/api/random")
def api_random_topk_rotate_norand():
    """
    推薦：完整資料優先 → 取 Top-K(預設100) → 環狀位移 → 可排除最近看過
    排序：__spacing_sum ↑, __shape_sum ↓, __case_sortkey ↑
    """
    try:
        scope = (request.args.get("scope", "filtered") or "filtered").strip().lower()
        base_df = apply_filters(DF)
        if len(base_df) == 0 and scope == "all":
            base_df = DF.copy()

        base_df = _ensure_sort_cols(base_df)

        # 只取完整資料；若沒有完整的就退回全部
        df_full = base_df[base_df["__complete"]] if "__complete" in base_df.columns else base_df
        if len(df_full) == 0:
            df_full = base_df
        df = df_full.sort_values(
            by=["__spacing_sum","__shape_sum","__case_sortkey"],
            ascending=[True, False, True],
            na_position="last",
            kind="mergesort",
        )

        if len(df) == 0:
            return jsonify({"items": [], "total": 0, "meta": {"k": 0, "used_recent": 0}}), 200

        # n, k
        try: n = int(request.args.get("n") or 3)
        except Exception: n = 3
        n = max(1, min(n, len(df)))

        try: K = int(request.args.get("k") or 100)
        except Exception: K = 100
        K = max(n, min(K, len(df)))

        # recent 排除
        recent_raw = (request.args.get("recent") or "").strip()
        used_recent = 0
        if recent_raw:
            recent_ids = {s.strip() for s in recent_raw.split(",") if s.strip()}
            key = df["__case_str"].astype(str) if "__case_str" in df.columns else None
            if key is not None:
                mask = ~key.isin(recent_ids)
                used_recent = int((~mask).sum())
                df2 = df[mask]
                if len(df2): df = df2

        topk = df.iloc[:K]
        if len(topk) == 0:
            return jsonify({"items": [], "total": 0, "meta": {"k": 0, "used_recent": used_recent}}), 200

        off_arg = request.args.get("offset")
        if off_arg is not None:
            try: offset = int(off_arg) % len(topk)
            except Exception: offset = 0
        else:
            now = datetime.utcnow()
            offset = ((now.minute * 60) + now.second) % len(topk)

        idx = list(range(len(topk))) + list(range(len(topk)))
        pick = idx[offset:offset + min(n, len(topk))]
        sub = topk.iloc[pick]

        items = [_row_to_item(r) for _, r in sub.iterrows()]
        resp = jsonify({
            "items": _clean_json_list(items),
            "total": int(len(df)),
            "meta": {"k": int(len(topk)), "used_recent": used_recent, "offset": int(offset)}
        })
        r = make_response(resp)
        r.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        r.headers["Pragma"] = "no-cache"
        r.headers["Expires"] = "0"
        return r

    except Exception as e:
        return jsonify({"error": str(e)}), 400

# ---------------------------
# Row → JSON
# ---------------------------
def _row_to_item(row: pd.Series) -> Dict[str, Any]:
    cols = row.get("_orig_cols")
    cols = cols if isinstance(cols, dict) else {}

    def pick(k, fallback=None):
        col = cols.get(k)
        if col and col in row.index:
            return row[col]
        return fallback

    return {
        "PanTS ID": _nan2none(pick("case") or row.get("__case_str")),
        "case_id":  _nan2none(pick("case") or row.get("__case_str")),
        "tumor":    (int(row.get("__tumor01")) if pd.notna(row.get("__tumor01")) else None),
        "sex":      _nan2none(row.get("__sex")),
        "age":      _nan2none(row.get("__age")),
        "ct phase": _nan2none(pick("ct_phase") or row.get("__ct")),
        "manufacturer": _nan2none(pick("manufacturer") or row.get("__mfr")),
        "manufacturer model": _nan2none(pick("model") or row.get("model")),
        "study year": _nan2none(row.get("__year_int")),
        "study type": _nan2none(pick("study_type") or row.get("study_type")),
        "site nationality": _nan2none(pick("site_nationality") or row.get("site_nationality")),
        # 排序輔助輸出
        "spacing_sum": _nan2none(row.get("__spacing_sum")),
        "shape_sum":   _nan2none(row.get("__shape_sum")),
        "complete":    bool(row.get("__complete")) if "__complete" in row else None,
    }

# ---------------------------
# Health & index
# ---------------------------
@app.get("/api/health")
def api_health():
    return jsonify({"ok": True})

@app.get("/")
def index():
    if not INDEX_FILE or not os.path.exists(INDEX_FILE):
        return "Backend OK (HTML not found or not provided)", 200
    return send_file(INDEX_FILE)

# ---------------------------
# main
# ---------------------------
if __name__ == "__main__":
    # 這裡直接用前面 argparse 解析到的參數
    app.run(host=args.host, port=args.port, debug=True)