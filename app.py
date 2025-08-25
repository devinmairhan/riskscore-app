# app_optimized_31d.py
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
import plotly.express as px
from bisect import bisect_left, bisect_right
from datetime import timedelta
from functools import reduce

st.set_page_config(page_title="Risk Rating Dashboard (31d pairs)", layout="wide")

# --------------------------
# Auth sederhana
# --------------------------
AUTH_USERS = {"admin": "admin123", "viewer": "viewer123"}

def login():
    if "auth" not in st.session_state:
        st.session_state.auth = False
    with st.sidebar:
        st.header("Login")
        u = st.text_input("Username")
        p = st.text_input("Password", type="password")
        if st.button("Masuk"):
            if u in AUTH_USERS and p == AUTH_USERS[u]:
                st.session_state.auth = True
                st.success("Login sukses.")
            else:
                st.error("Username/Password salah.")
    return st.session_state.auth

if not login():
    st.stop()

st.title("FARIS – your trusted eye on fraud")
st.caption("Upload Excel berisi sheet: Complaint, CBC, Switching, AccountBalance (PolicyMap opsional)")

uploaded = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"])

with st.sidebar:
    st.subheader("Bobot Risk Rating")
    w_complaint = st.number_input("Complaint (%)", 0, 100, 40, step=1) / 100
    w_cbc       = st.number_input("CBC (%)",       0, 100, 30, step=1) / 100
    w_balance   = st.number_input("Account Balance (%)", 0, 100, 10, step=1) / 100
    w_switch    = st.number_input("Switching (%)", 0, 100, 20, step=1) / 100
    if abs((w_complaint+w_cbc+w_balance+w_switch) - 1.0) > 1e-9:
        st.warning("Total bobot harus 100%.")
    invert_balance = st.checkbox("Balance lebih tinggi = risiko lebih RENDAH? (invert)", value=False)

    st.divider()  # garis pemisah biar rapi
    top_n = st.number_input(
        "Tampilkan Top N (untuk chart)",
        min_value=5, max_value=50,
        value=10, step=5
    )

    st.divider()
    st.write("**CBC yang dihitung** → `flag_type` mengandung “with alert” & (confirmed/unconfirmed)")
    allowed_phrase = "with alert"
    st.code(allowed_phrase)

if not uploaded:
    st.info("Silakan upload file Excel terlebih dahulu.")
    st.stop()

# --------------------------
# Helpers & Cache
# --------------------------
def _norm_policy(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()

    # hilangkan ".0" efek Excel & spasi
    s = s.str.replace(r"\.0$", "", regex=True)
    s = s.str.replace(r"\s+", "", regex=True)

    # samakan kapital
    s = s.str.upper()

    # kalau numeric → buang leading zero
    is_digit = s.str.fullmatch(r"\d+")
    s.loc[is_digit] = s.loc[is_digit].str.lstrip("0").replace({"": "0"})

    # bersihkan nilai null
    return s.replace({"NAN": "", "NA": "", "NAT": "", "NONE": ""})

@st.cache_data(show_spinner=False)
def load_excel(file):
    xls = pd.ExcelFile(file)
    return {n: pd.read_excel(xls, n) for n in xls.sheet_names}

@st.cache_data(show_spinner=False)
def preprocess_tables(sheets: dict, allowed_phrase: str):
    # Ambil sheet (toleran)
    df_complaint = sheets.get("Complaint", pd.DataFrame()).copy()
    df_cbc       = sheets.get("CBC", pd.DataFrame()).copy()
    df_switch    = sheets.get("Switching", pd.DataFrame()).copy()
    df_acc       = sheets.get("AccountBalance", pd.DataFrame()).copy()
    df_map       = sheets.get("PolicyMap", pd.DataFrame()).copy()

    to_date = lambda s: pd.to_datetime(s, errors="coerce")

    # Konversi tanggal bila kolom ada
    if "complaint_date" in df_complaint.columns: df_complaint["complaint_date"] = to_date(df_complaint["complaint_date"])
    if "cbc_date"       in df_cbc.columns:       df_cbc["cbc_date"]             = to_date(df_cbc["cbc_date"])
    if "switching_date" in df_switch.columns:    df_switch["switching_date"]    = to_date(df_switch["switching_date"])
    if "txn_date"       in df_acc.columns:       df_acc["txn_date"]             = to_date(df_acc["txn_date"])

    # Normalisasi policy_no bila ada
    for df in [df_complaint, df_cbc, df_switch, df_acc, df_map]:
        if "policy_no" in df.columns:
            df["policy_no"] = _norm_policy(df["policy_no"])

    # PolicyMap fallback minimal
    if "policy_no" not in df_map.columns:
        all_polis = pd.concat([
            df_complaint.get("policy_no", pd.Series(dtype=object)),
            df_cbc.get("policy_no", pd.Series(dtype=object)),
            df_switch.get("policy_no", pd.Series(dtype=object)),
            df_acc.get("policy_no", pd.Series(dtype=object)),
        ], ignore_index=True).dropna().astype(str).unique()
        df_map = pd.DataFrame({"policy_no": all_polis})

    else:
        df_map["policy_no"] = _norm_policy(df_map["policy_no"])

    # ==== CBC filter berbasis flag_type: hanya ... with alert & (confirmed/unconfirmed) ====
    df_cbc["flag_type"] = df_cbc.get("flag_type", "").fillna("").astype(str)
    lt = df_cbc["flag_type"].str.lower()
    mask_alert = lt.str.contains("with alert", na=False)
    mask_conf  = lt.str.contains("confirmed", na=False)
    mask_unconf= lt.str.contains("unconfirmed", na=False)
    df_cbc = df_cbc[ mask_alert & (mask_conf | mask_unconf) ].copy()

    # Casting numerik ringan
    if "amount_idr" in df_switch.columns:
        df_switch["amount_idr"] = pd.to_numeric(df_switch["amount_idr"], errors="coerce").fillna(0.0).astype("float32")
    if "amount_idr" in df_acc.columns:
        df_acc["amount_idr"] = pd.to_numeric(df_acc["amount_idr"], errors="coerce").fillna(0.0).astype("float32")

    # Map IN/OUT untuk metrik balance (tetap seperti semula)
    IN_WORDS  = {"adhoc","deposit","in","money in","topup","top up","credit"}
    OUT_WORDS = {"withdrawal","out","money out","debit","wd","tarik"}
    df_acc["transaction_type"] = df_acc.get("transaction_type", "").astype(str)

    def to_signed_fast(t, a):
        t = t.strip().lower()
        if any(w in t for w in IN_WORDS):  return +abs(a)
        if any(w in t for w in OUT_WORDS): return -abs(a)
        return +a

    if not df_acc.empty:
        df_acc["signed_amount"] = [to_signed_fast(t, a) for t, a in zip(df_acc["transaction_type"], df_acc["amount_idr"])]
        df_acc["signed_amount"] = pd.to_numeric(df_acc["signed_amount"], errors="coerce").fillna(0.0).astype("float32")
        df_acc["abs_amount"]    = df_acc["signed_amount"].abs().astype("float32")

    # ==== Perkaya df_map dengan PFC Code & Name dari sheet lain bila ada ====
    def pick_col(df, candidates):
        cand_lc = [c.lower() for c in candidates]
        for c in df.columns:
            if c.lower().strip() in cand_lc:
                return c
        return None

    # kandidat kolom PFC di beberapa sheet
    pfc_code_col_map = pick_col(df_map, ["pfc_code", "pfc code"])
    pfc_name_col_map = pick_col(df_map, ["pfc_name", "pfc name", "pfc"])

    pfc_code_col_acc = pick_col(df_acc, ["pfc code", "pfc_code"])
    pfc_name_col_acc = pick_col(df_acc, ["pfc", "pfc name", "pfc_name"])

    pfc_name_col_cbc = pick_col(df_cbc, ["nama pfc", "pfc", "pfc name", "pfc_name"])

    branch_code_col_map = pick_col(df_map, ["branch_code","branch code","kode_cabang","kode cabang"])   # NEW
    branch_name_col_map = pick_col(df_map, ["branch_name","branch name","cabang","nama_cabang"])       # NEW
    branch_code_col_acc = pick_col(df_acc, ["branch_code","branch code","kode_cabang","kode cabang"])  # NEW
    branch_name_col_acc = pick_col(df_acc, ["branch_name","branch name","cabang","nama_cabang"]) 

    tmp_maps = []

    # sumber dari PolicyMap (kalau sudah ada)
    if pfc_code_col_map or pfc_name_col_map:
        cols = ["policy_no"]
        if pfc_code_col_map: cols.append(pfc_code_col_map)
        if pfc_name_col_map: cols.append(pfc_name_col_map)
        tmp = (df_map[cols]
            .dropna(subset=["policy_no"])
            .drop_duplicates("policy_no")
            .copy())
        if pfc_code_col_map: tmp = tmp.rename(columns={pfc_code_col_map: "pfc_code"})
        if pfc_name_col_map: tmp = tmp.rename(columns={pfc_name_col_map: "pfc_name"})
        tmp_maps.append(tmp)

    # sumber dari AccountBalance
    if pfc_code_col_acc or pfc_name_col_acc or branch_code_col_acc or branch_name_col_acc:
        cols = ["policy_no"]
        if pfc_code_col_acc: cols.append(pfc_code_col_acc)
        if pfc_name_col_acc: cols.append(pfc_name_col_acc)
        if branch_code_col_acc: cols.append(branch_code_col_acc)   # NEW
        if branch_name_col_acc: cols.append(branch_name_col_acc)   # NEW
        tmp = (df_acc[cols]
            .dropna(subset=["policy_no"])
            .drop_duplicates("policy_no")
            .copy())
        ren = {}
        if pfc_code_col_acc:    ren[pfc_code_col_acc]    = "pfc_code"
        if pfc_name_col_acc:    ren[pfc_name_col_acc]    = "pfc_name"
        if branch_code_col_acc: ren[branch_code_col_acc] = "branch_code"   # NEW
        if branch_name_col_acc: ren[branch_name_col_acc] = "branch_name"   # NEW
        tmp = tmp.rename(columns=ren)
        tmp_maps.append(tmp)


    # sumber dari CBC (nama PFC saja)
    if pfc_name_col_cbc:
        tmp = (df_cbc[["policy_no", pfc_name_col_cbc]]
            .dropna(subset=["policy_no"])
            .drop_duplicates("policy_no")
            .rename(columns={pfc_name_col_cbc: "pfc_name"})
            .copy())
        tmp_maps.append(tmp)

    # >>> Semua langkah yang pakai map_all harus DI DALAM blok ini <<<
    if tmp_maps:
        map_all = reduce(lambda a, b: pd.merge(a, b, on="policy_no", how="outer"), tmp_maps)

        # coalesce kolom duplikat pfc_code*/pfc_name* jadi satu
        def coalesce(df, base):
            cols = [c for c in df.columns if c == base or c.startswith(base + "_")]
            if not cols:
                df[base] = np.nan
                return
            s = None
            for c in cols:
                s = df[c] if s is None else s.combine_first(df[c])
            df[base] = s
            drop = [c for c in cols if c != base]
            if drop:
                df.drop(columns=drop, inplace=True)

        for col in ["pfc_code", "pfc_name", "branch_code", "branch_name"]:  # NEW: branch_*
            if col not in map_all.columns:
                map_all[col] = np.nan
            coalesce(map_all, col)
            map_all[col] = map_all[col].astype(str).str.strip()

        # gabungkan ke df_map
        df_map = df_map.merge(
            map_all[["policy_no", "pfc_code", "pfc_name", "branch_code", "branch_name"]],
            on="policy_no", how="left", suffixes=("", "_m")
        )
        # pilih nilai yang terisi jika ada _m
        for col in ["pfc_code", "pfc_name", "branch_code", "branch_name"]:
            if f"{col}_m" in df_map.columns:
                df_map[col] = df_map[col].where(
                    df_map[col].astype(str).str.strip() != "", df_map[f"{col}_m"]
                )
                df_map.drop(columns=[f"{col}_m"], inplace=True)

    # rakit MARKETER DISPLAY: PFC_CODE - PFC_NAME bila ada; kalau tidak -> policy_no
    if "marketer_name" not in df_map.columns:
        df_map["marketer_name"] = df_map["policy_no"]

    for c in ["pfc_code", "pfc_name", "branch_code", "branch_name"]:
        if c not in df_map.columns: 
            df_map[c] = np.nan

    has_pfc = (df_map["pfc_code"].fillna("").str.strip() != "") | (df_map["pfc_name"].fillna("").str.strip() != "")
    pfc_code_str = df_map["pfc_code"].fillna("").astype(str).str.strip()
    pfc_name_str = df_map["pfc_name"].fillna("").astype(str).str.strip()
    pfc_label = (pfc_code_str + " - " + pfc_name_str).str.strip(" -")
    df_map.loc[has_pfc, "marketer_name"] = pfc_label[has_pfc]

    # pastikan hanya 1 baris per policy_no supaya join tidak gandakan baris
    df_map = (
        df_map
        .dropna(subset=["policy_no"])
        .drop_duplicates(subset=["policy_no"], keep="first")
        .copy()
    )


    # Join marketer + month (month untuk agregasi; pairs pakai tanggal asli)
    def add_marketer(df, date_col):
        if df.empty:
            return df.assign(marketer_name=pd.Series(dtype="category"), month=pd.PeriodIndex([], freq="M"))
        merge_cols = ["policy_no", "marketer_name"]
        if "pfc_code" in df_map.columns: merge_cols.append("pfc_code")
        if "pfc_name" in df_map.columns: merge_cols.append("pfc_name")
        out = df.merge(df_map[merge_cols], on="policy_no", how="left")
        # fallback terakhir
        out["marketer_name"] = out["marketer_name"].fillna(out["policy_no"].astype(str)).astype("category")
        if date_col in out.columns:
            out["month"] = pd.to_datetime(out[date_col], errors="coerce").dt.to_period("M")
        else:
            out["month"] = pd.PeriodIndex([], freq="M")
        return out

    df_complaint = add_marketer(df_complaint, "complaint_date")
    df_cbc       = add_marketer(df_cbc, "cbc_date")
    df_switch    = add_marketer(df_switch, "switching_date")
    df_acc       = add_marketer(df_acc, "txn_date")

    # ==== Flag khusus pairing: Withdrawal (OUT, non-reversal) vs Adhoc/Top Up (IN, non-reversal) ====
    tt = df_acc["transaction_type"].str.lower()
    df_acc["pair_is_out"] = tt.str.contains("withdrawal", na=False) & ~tt.str.contains("reversal", na=False)
    df_acc["pair_is_in"]  = (tt.str.contains("adhoc", na=False) | tt.str.contains(r"top ?up", regex=True, na=False)) \
                            & ~tt.str.contains("reversal", na=False)

    # pastikan id unik ada; kalau tidak, buatkan
    if "txn_id" not in df_acc.columns:
        df_acc["txn_id"] = np.arange(1, len(df_acc) + 1)

    # pastikan kolom opsional ada (biar tidak KeyError saat display)
    for col, default in {"channel": "-", "notes": ""}.items():
        if col not in df_acc.columns:
            df_acc[col] = default

    return df_complaint, df_cbc, df_switch, df_acc, df_map

@st.cache_data(show_spinner=False)
def build_monthly_aggs(df_complaint, df_cbc, df_switch, df_acc):
    def gby(df, cols, agg_map):
        if df.empty: return pd.DataFrame(columns=[*cols, *[k for k in agg_map]])
        return (df.dropna(subset=["month"])
                  .groupby(["marketer_name","month"], dropna=False)
                  .agg(**agg_map).reset_index())

    aggC_m   = gby(df_complaint, ["marketer_name","month"], {"total_complaint":("complaint_id","count")})
    aggB_m   = gby(df_cbc,       ["marketer_name","month"], {"total_cbc":("cbc_id","count")})
    aggS_m   = gby(df_switch, ["marketer_name","month"], {
                            "total_switching": ("switch_id","nunique"),
                            "switching_amount": ("amount_idr","sum")
                        })
    aggBal_m = gby(df_acc,       ["marketer_name","month"], {"total_balance":("signed_amount","sum")})
    return aggC_m, aggB_m, aggS_m, aggBal_m

@st.cache_data(show_spinner=False)
def build_pairs_31d(df_acc, sim_threshold=0.75, window_days=31):
    if df_acc.empty:
        return pd.DataFrame(columns=[
            "marketer_name","policy_no",
            "out_txn_id","out_date","out_type","out_amount",
            "in_txn_id","in_date","in_type","in_amount","similarity",
            "month_out"
        ])

    pairs = []
    for (mk, pol), grp in df_acc.groupby(["marketer_name","policy_no"]):
        g = grp.dropna(subset=["txn_date"]).copy()

        outs = g[g["pair_is_out"]][["txn_id","txn_date","transaction_type","amount_idr"]].copy()
        ins  = g[g["pair_is_in"] ][["txn_id","txn_date","transaction_type","amount_idr"]].copy()
        if outs.empty or ins.empty:
            continue

        ins = ins.sort_values("amount_idr").reset_index(drop=True)
        ins_amts = ins["amount_idr"].astype(float).values
        used_in = set()

        for _, o in outs.iterrows():
            o_amt  = float(abs(o["amount_idr"]))
            o_date = pd.to_datetime(o["txn_date"])
            if not np.isfinite(o_amt) or o_amt <= 0 or pd.isna(o_date):
                continue

            # window tanggal ≤ 31 hari setelah OUT
            in_window = ins[
                (pd.to_datetime(ins["txn_date"]) >= o_date) &
                (pd.to_datetime(ins["txn_date"]) <= o_date + timedelta(days=window_days))
            ]
            if in_window.empty:
                continue

            lo, hi = sim_threshold * o_amt, o_amt / sim_threshold
            L = bisect_left(ins_amts, lo)
            R = bisect_right(ins_amts, hi)

            idxs_window = set(in_window.index.to_list())
            best_idx, best_sim = -1, 0.0
            for j in range(L, R):
                if j not in idxs_window: 
                    continue
                if ins.loc[j, "txn_id"] in used_in:
                    continue
                a = float(abs(ins.loc[j, "amount_idr"]))
                if not np.isfinite(a) or a <= 0:
                    continue
                sim = min(a, o_amt) / max(a, o_amt)
                if sim > best_sim:
                    best_sim, best_idx = sim, j

            if best_idx >= 0 and best_sim >= sim_threshold:
                used_in.add(ins.loc[best_idx, "txn_id"])
                pairs.append({
                    "marketer_name": mk,
                    "policy_no": pol,
                    "out_txn_id": o["txn_id"],
                    "out_date": o_date,
                    "out_type": o["transaction_type"],
                    "out_amount": o_amt,
                    "in_txn_id": ins.loc[best_idx, "txn_id"],
                    "in_date": pd.to_datetime(ins.loc[best_idx, "txn_date"]),
                    "in_type": ins.loc[best_idx, "transaction_type"],
                    "in_amount": float(abs(ins.loc[best_idx, "amount_idr"])),
                    "similarity": float(best_sim),
                    "month_out": pd.to_datetime(o_date).to_period("M")
                })

    return pd.DataFrame(pairs)

@st.cache_data(show_spinner=False)
def to_excel_bytes_cached(df_dict):
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as w:
        for name, df in df_dict.items():
            df.to_excel(w, sheet_name=name[:31], index=False)
    bio.seek(0)
    return bio

# --------------------------
# Load + Preprocess (cached)
# --------------------------
try:
    sheets = load_excel(uploaded)
    df_complaint, df_cbc, df_switch, df_acc, df_map = preprocess_tables(sheets, allowed_phrase)
except Exception as e:
    st.error(f"Gagal membaca/menyiapkan data: {e}")
    st.stop()

# --------------------------
# Filter waktu (slider bulan untuk agregasi & month_out untuk pairs)
# --------------------------
all_months_series = pd.concat([
    df_complaint.get("month", pd.Series(dtype="period[M]")),
    df_cbc.get("month", pd.Series(dtype="period[M]")),
    df_switch.get("month", pd.Series(dtype="period[M]")),
    df_acc.get("month", pd.Series(dtype="period[M]")),
]).dropna()

all_months = sorted(pd.Series(all_months_series.unique()))
with st.sidebar:
    st.subheader("Filter Waktu (agregasi & month_out untuk pairs)")
    if len(all_months) == 0:
        month_from = month_to = None
        st.caption("Tidak ada data bertanggal untuk difilter.")
    elif len(all_months) == 1:
        month_from = month_to = all_months[0]
        st.caption(f"Data hanya bulan {month_from}. Filter rentang dinonaktifkan.")
    else:
        month_from, month_to = st.select_slider(
            "Rentang bulan", options=all_months, value=(all_months[0], all_months[-1])
        )

def month_filter(df, mfrom, mto, month_col="month"):
    if df.empty or mfrom is None: return df
    return df[(df[month_col] >= mfrom) & (df[month_col] <= mto)]

# --------------------------
# Agregasi bulanan (cached) → filter → sum
# --------------------------
aggC_m, aggB_m, aggS_m, aggBal_m = build_monthly_aggs(df_complaint, df_cbc, df_switch, df_acc)

# ---- Risk score per BULAN per MARKETER (untuk line chart) ----
monthly_all = (
    aggC_m.merge(aggB_m,  on=["marketer_name","month"], how="outer")
          .merge(aggS_m,  on=["marketer_name","month"], how="outer")
          .merge(aggBal_m,on=["marketer_name","month"], how="outer")
)

# pastikan kolom ada & isi nol kalau NaN
for c in ["total_complaint","total_cbc","total_switching","switching_amount","total_balance"]:
    if c not in monthly_all.columns:
        monthly_all[c] = 0
monthly_all[["total_complaint","total_cbc","total_switching"]] = \
    monthly_all[["total_complaint","total_cbc","total_switching"]].fillna(0).astype(int)
monthly_all["switching_amount"] = pd.to_numeric(monthly_all["switching_amount"], errors="coerce").fillna(0.0)
monthly_all["total_balance"]    = pd.to_numeric(monthly_all["total_balance"],    errors="coerce").fillna(0.0)

# normalisasi per-bulan (min–max across all marketer di bulan tsb), lalu hitung risk score bulanan
def _minmax_series(s: pd.Series) -> pd.Series:
    s = s.astype(float)
    if s.max() == s.min():
        return pd.Series(np.zeros(len(s)), index=s.index, dtype=float)
    return (s - s.min()) / (s.max() - s.min())

def _risk_per_month(g: pd.DataFrame) -> pd.DataFrame:
    n_comp = _minmax_series(g["total_complaint"])
    n_cbc  = _minmax_series(g["total_cbc"])
    n_sw   = _minmax_series(g["total_switching"])
    n_bal_raw = _minmax_series(g["total_balance"])
    n_bal = 1 - n_bal_raw if invert_balance else n_bal_raw
    g = g.copy()
    g["risk_score_month"] = 100 * (
        w_complaint * n_comp +
        w_cbc       * n_cbc +
        w_switch    * n_sw   +
        w_balance   * n_bal
    )
    return g

monthly_risk_all = monthly_all.groupby("month", group_keys=False).apply(_risk_per_month)
monthly_risk_all["risk_score_month"] = monthly_risk_all["risk_score_month"].round(2)

# filter sesuai rentang bulan sidebar untuk tampilan detail (pakai month kolom)
monthly_risk_view = month_filter(monthly_risk_all, month_from, month_to, month_col="month").copy()

aggC   = month_filter(aggC_m,   month_from, month_to).groupby("marketer_name", dropna=False)["total_complaint"].sum().reset_index()
aggB   = month_filter(aggB_m,   month_from, month_to).groupby("marketer_name", dropna=False)["total_cbc"].sum().reset_index()
aggS   = month_filter(aggS_m,   month_from, month_to).groupby("marketer_name", dropna=False)[["total_switching","switching_amount"]].sum().reset_index()
aggBal = month_filter(aggBal_m, month_from, month_to).groupby("marketer_name", dropna=False)["total_balance"].sum().reset_index()

# --------------------------
# Pairs 31 hari (lintas bulan) → filter pakai month_out → sum
# --------------------------
pairs_all = build_pairs_31d(df_acc, sim_threshold=0.75, window_days=31)
pairs_view = month_filter(pairs_all, month_from, month_to, month_col="month_out")
pairs_f = pairs_view.groupby("marketer_name", dropna=False).size().reset_index(name="money_out_in_pairs")

# --------------------------
# Build Summary
# --------------------------
# kunci daftar marketer diambil dari df_map['marketer_name'] (sudah “PFC_CODE - PFC_NAME” jika tersedia)
summary_keys = df_map.copy()
if "marketer_name" not in summary_keys.columns:
    summary_keys["marketer_name"] = summary_keys["policy_no"].astype(str)

# simpan juga pfc_code/pfc_name kalau ada (untuk tampilan)
for c in ["pfc_code", "pfc_name"]:
    if c not in summary_keys.columns:
        summary_keys[c] = ""

summary = summary_keys[["marketer_name","pfc_code","pfc_name"]].drop_duplicates().copy()

# gabungkan agregasi
for part in [aggC, aggB, aggS, aggBal, pairs_f]:
    summary = summary.merge(part, on="marketer_name", how="left")

# pastikan SEMUA kolom metrik ada
for col in ["total_complaint","total_cbc","total_switching","money_out_in_pairs","total_balance","switching_amount"]:
    if col not in summary.columns:
        summary[col] = 0
# tipe & NaN handling
summary[["total_complaint","total_cbc","total_switching","money_out_in_pairs"]] = \
    summary[["total_complaint","total_cbc","total_switching","money_out_in_pairs"]].fillna(0).astype(int)
summary["total_balance"]    = pd.to_numeric(summary["total_balance"], errors="coerce").fillna(0.0)
summary["switching_amount"] = pd.to_numeric(summary["switching_amount"], errors="coerce").fillna(0.0)

# ---- Risk score (selalu dihitung setelah kolom di atas aman) ----
def minmax(col):
    x = summary[col].astype(float).values
    if len(x) == 0 or np.nanmax(x) == np.nanmin(x):
        return np.zeros_like(x)
    return (x - np.nanmin(x)) / (np.nanmax(x) - np.nanmin(x))

norm_complaint = minmax("total_complaint")
norm_cbc       = minmax("total_cbc")
norm_balance_r = minmax("total_balance")
norm_balance   = 1 - norm_balance_r if invert_balance else norm_balance_r
norm_switch    = minmax("total_switching")

summary["risk_score"] = 100 * (
    w_complaint * norm_complaint +
    w_cbc       * norm_cbc +
    w_balance   * norm_balance +
    w_switch    * norm_switch
)
summary["risk_score"] = summary["risk_score"].round(2)

# kalau karena suatu hal risk_score belum ada, fallback nol
if "risk_score" not in summary.columns:
    summary["risk_score"] = 0.0


# --------------------------
# KPI
# --------------------------
kcols = st.columns(6)
kcols[0].metric("Total Marketer", len(summary))
kcols[1].metric("Total Complaint", int(summary["total_complaint"].sum()))
kcols[2].metric("Total CBC (with alert)", int(summary["total_cbc"].sum()))
kcols[3].metric("Total Switching", int(summary["total_switching"].sum()))
kcols[4].metric("Net Account Balance", f"{summary['total_balance'].sum():,.0f}")
kcols[5].metric("MoneyOut–In Pairs (≤31 hari)", int(summary["money_out_in_pairs"].sum()))

# --------------------------
# Table & Charts
# --------------------------
st.subheader("Risk Rating per Tenaga Pemasar")

# urutkan dulu, pakai juga buat tabel
summary_sorted = summary.sort_values(["risk_score","money_out_in_pairs"], ascending=[False, False]).copy()

# PAKSA kategorikal/string agar sumbu X tidak dianggap numerik
summary_sorted["marketer_name"] = summary_sorted["marketer_name"].astype(str)

cols_show = [
    "marketer_name","pfc_code","pfc_name",     # <- tanpa branch di sini
    "total_complaint","total_cbc","total_switching","switching_amount",
    "total_balance","money_out_in_pairs","risk_score"
]
cols_show = [c for c in cols_show if c in summary_sorted.columns]

st.dataframe(summary_sorted.head(1000), use_container_width=True)

tab1, tab2 = st.tabs(["📈 Risk Score (Bar)", "📈 Complaint / CBC / Switching / Pairs"])

with tab1:
    top_df = summary_sorted.head(top_n).copy()
    if top_df.empty:
        st.info("Tidak ada data untuk ditampilkan.")
    else:
        # penting: tegaskan sumbu X kategori
        top_df["marketer_name"] = top_df["marketer_name"].astype(str)
        fig = px.bar(
            top_df,
            x="marketer_name",
            y="risk_score",
            hover_data=["total_complaint","total_cbc","total_switching","total_balance","money_out_in_pairs"],
            labels={"marketer_name":"Marketer","risk_score":"Risk Score"},
            text="risk_score"
        )
        fig.update_traces(texttemplate="%{text:.2f}", textposition="outside", cliponaxis=False)
        fig.update_xaxes(type="category", tickangle=45)  # ← bikin kategori & miringkan label
        fig.update_layout(xaxis_title="", yaxis_title="Risk Score (0-100)", transition={'duration': 0},
                          margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    top_df = summary_sorted.head(top_n).copy()
    if top_df.empty:
        st.info("Tidak ada data untuk ditampilkan.")
    else:
        top_df["marketer_name"] = top_df["marketer_name"].astype(str)
        melted = top_df.melt(
            id_vars=["marketer_name"],
            value_vars=["total_complaint","total_cbc","total_switching","money_out_in_pairs"],
            var_name="metric", value_name="count"
        )
        fig2 = px.bar(melted, x="marketer_name", y="count", color="metric", barmode="group")
        fig2.update_xaxes(type="category", tickangle=45)
        fig2.update_layout(xaxis_title="", yaxis_title="Count", transition={'duration': 0},
                           margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig2, use_container_width=True)

# --------------------------
# Detail Per Marketer
# --------------------------
st.subheader("Detail Per Marketer")
opt = st.selectbox("Pilih marketer", ["(semua)"] + sorted(summary["marketer_name"].dropna().astype(str).tolist()))
if opt != "(semua)":
    st.write("**Ringkasan**")
    st.dataframe(summary[summary["marketer_name"]==opt], use_container_width=True)

    brancches = (
        df_map[df_map["marketer_name"] == opt][["branch_code","branch_name"]]
        .dropna(how="all")              # buang baris yang kedua kolomnya NaN
        .drop_duplicates()              # unik per cabang
        .sort_values(["branch_code","branch_name"], na_position="last")
    )
    if brancches.empty:
        st.caption("Tidak ada data cabang untuk marketer ini.")
    else:
        st.write("**Cabang terkait**")
        st.dataframe(brancches, use_container_width=True,hide_index=True)

    st.write("**Money Out – Money In (≤31 hari, similarity ≥ 75%)**")
    pairs_opt = pairs_view[pairs_view["marketer_name"]==opt].copy()
    if pairs_opt.empty:
        st.info("Tidak ada pasangan Withdrawal → Adhoc sesuai kriteria pada rentang bulan terpilih (bulan mengacu ke OUT).")
    else:
        pairs_opt = pairs_opt.sort_values(["month_out","similarity"], ascending=[True, False])
        st.dataframe(pairs_opt, use_container_width=True)

        involved_ids = pd.unique(pairs_opt[["out_txn_id","in_txn_id"]].values.ravel("K"))
        txns = df_acc[df_acc["txn_id"].isin(involved_ids)][
            ["txn_id","policy_no","txn_date","transaction_type","amount_idr","signed_amount","marketer_name"]
        ].sort_values("txn_date")

            # join cabang (opsional)
        txns = txns.merge(
            df_map[["policy_no","branch_code","branch_name"]].drop_duplicates("policy_no"),
            on="policy_no", how="left"
        )

        with st.expander("🔎 Lihat transaksi yang terlibat"):
            st.dataframe(txns, use_container_width=True)   

    st.write("**Complaint rows**")
    comp_view = (df_complaint[df_complaint["marketer_name"]==opt])
    comp_view = month_filter(comp_view, month_from, month_to)
    st.dataframe(comp_view.head(500), use_container_width=True)

    st.write("**CBC rows (with alert only)**")
    cbc_view = (df_cbc[df_cbc["marketer_name"]==opt])
    cbc_view = month_filter(cbc_view, month_from, month_to)
    st.dataframe(cbc_view.head(500), use_container_width=True)

    st.write("**Switching rows**")
    sw_view = (df_switch[df_switch["marketer_name"]==opt])
    sw_view = month_filter(sw_view, month_from, month_to)
    st.dataframe(sw_view.head(500), use_container_width=True)

    st.write("**Account Balance rows (signed_amount)**")
    acc_view = (df_acc[df_acc["marketer_name"]==opt])
    acc_view = month_filter(acc_view, month_from, month_to)
    st.dataframe(acc_view[["txn_id","policy_no","txn_date","transaction_type","amount_idr","signed_amount","channel","notes"]].head(500),
                 use_container_width=True)
    
# =========================
# Risk Score per Bulan (Last 12 months)
# =========================

st.markdown("**📈 Risk Score per Bulan (semua bulan pada rentang filter)**")

if opt != "(semua)":
    # monthly_risk_view sudah disiapkan lebih atas:
    # -> itu hasil normalisasi & perhitungan risk_score_month per marketer per bulan
    rsel = monthly_risk_view[monthly_risk_view["marketer_name"] == opt].copy()

    if rsel.empty:
        st.info("Tidak ada data bulanan untuk marketer ini pada rentang bulan terpilih.")
    else:
        rsel = rsel.sort_values("month").copy()
        rsel["month_str"] = rsel["month"].astype(str)

        # garis utama (pakai semua bulan yang lolos filter)
        fig_raw = px.line(
            rsel,
            x="month_str",
            y="risk_score_month",
            markers=True,
            labels={"month_str": "Bulan", "risk_score_month": "Risk Score (0–100)"},
            title=f"Risk Score per Bulan — {opt}"
        )
        fig_raw.update_layout(margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(fig_raw, use_container_width=True)

        # (opsional) garis smoothed 3 bulan untuk lihat tren
        rsel["risk_score_month_smooth"] = (
            rsel["risk_score_month"].rolling(3, min_periods=1).mean()
        )
        with st.expander("Tampilkan garis rata-rata 3 bulan (opsional)"):
            fig_sm = px.line(
                rsel,
                x="month_str",
                y="risk_score_month_smooth",
                labels={"month_str": "Bulan", "risk_score_month_smooth": "Risk Score (3M avg)"},
            )
            fig_sm.update_layout(margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig_sm, use_container_width=True)
else:
    st.caption("Pilih marketer untuk melihat grafik per-bulan seluruh rentang yang difilter.")

# --------------------------
# Export hasil
# --------------------------
st.subheader("Export Summary")
export_bytes = to_excel_bytes_cached({
    "SummaryPerMarketer": summary.sort_values(["risk_score","money_out_in_pairs"], ascending=[False, False]),
    "PairsAll": pairs_all,
    "PairsFilteredByMonthOut": pairs_view,
})
st.download_button("⬇️ Download hasil (Excel)", data=export_bytes, file_name="risk_master_output.xlsx",
                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
