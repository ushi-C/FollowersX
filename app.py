import calendar
import csv
import io
import random
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
import streamlit.components.v1 as components

# =========================
# 1. Page Configuration
# =========================
st.set_page_config(page_title="X Intelligence", layout="wide")

# =========================
# 2. i18n (trilingual)
# =========================
LANG_LABELS = {
    "中文": {
        "title": "🏛️ X关注统计局",
        "pulse": "系统脉冲",
        "monitored": "VTuber",
        "agencies": "企业",
        "total_reach": "影响力",
        "active_nodes": "收录总数",
        "authorities": "事务所",
        "total_followers": "总关注数",
        "follower_level": "🎚️ 关注数分级",
        "level_all": "全部",
        "level_10k": "1 万以下",
        "level_100k": "10 万以下",
        "level_1m": "100 万以下",
        "new_members": "🆕 最新加入：{}名",
        "power_ranking": "🏆 总关注数排名",
        "distribution": "📊 占比饼",
        "efficiency": "⚖️ 人均关注数排名",
        "top50": "🎯 Top 50",
        "institution": "事务所",
        "total_influence": "总影响力",
        "avg_account": "平均",
        "accounts": "成员",
        "account_id": "X",
        "office_col": "所属",
        "followers_col": "关注数",
        "no_db": "数据库无数据。",
        "last_update": "最后更新",
        "7d_trend": "7日增长",
        "mtd_label": "月度预估增长",
        "daily_avg": "日均增长",
        "last_val": "最新关注数",
        "trend_stagnant": "超30天未更新",
        "trend_insufficient": "数据不足",
        "user_trend_title": "📈 增长热度",
        "trend_scope_all": "🌐 综合",
        "trend_scope_label": "范围",
        "rank_col": "热度",
        "username_col": "成员",
        "trend_7d_col": "7日增长率",
        "trend_mtd_col": "月度预估增长率",
        "daily_avg_col": "日均增长",
        "last_followers_col": "最新关注数",
        "tab_7d": "🔥 7日增长热度",
        "tab_mtd": "📅 月度增长热度",
        "tab_abs": "📊 7日增长数",
        "growth_7d_abs_col": "7日涨粉估测",
        "no_trend_data": "暂无可计算趋势的数据（至少需2条记录）",
        "stagnant_list": "停更成员",
        "insuf_list": "数据不足成员",
        "forecast_label": "预计",
    },
    "English": {
        "title": "🏛️ X Influence Intelligence",
        "pulse": "System Pulse",
        "monitored": "RECORD",
        "agencies": "AGENCIES",
        "total_reach": "TOTAL REACH",
        "active_nodes": "Active Nodes",
        "authorities": "Authorities",
        "total_followers": "Total Followers",
        "follower_level": "🎚️ Follower Tier",
        "level_all": "All",
        "level_10k": "Under 10K",
        "level_100k": "Under 100K",
        "level_1m": "Under 1M",
        "new_members": "🆕 New Members: {}",
        "power_ranking": "🏆 Office Power Ranking",
        "distribution": "📊 Distribution",
        "efficiency": "⚖️ per captia influence Ranking",
        "top50": "🎯 Top 50",
        "institution": "INSTITUTION",
        "total_influence": "TOTAL INFLUENCE",
        "avg_account": "AVG",
        "accounts": "ACCOUNTS",
        "account_id": "X ID",
        "office_col": "OFFICE",
        "followers_col": "FOLLOWERS",
        "no_db": "No data in database.",
        "last_update": "Last Update",
        "7d_trend": "7D Growth",
        "mtd_label": "Projected MTD Growth",
        "daily_avg": "Daily Avg",
        "last_val": "Latest Followers",
        "trend_stagnant": "Stale 30d+",
        "trend_insufficient": "Insufficient data",
        "user_trend_title": "📈 Trend Ranking",
        "trend_scope_all": "🌐 All Members",
        "trend_scope_label": "Scope",
        "rank_col": "RANK",
        "username_col": "MEMBER",
        "trend_7d_col": "7D Growth Rate",
        "trend_mtd_col": "Projected MTD Rate",
        "daily_avg_col": "Daily Avg Growth",
        "last_followers_col": "Latest Followers",
        "tab_7d": "📊 7D Growth Rate Estimate",
        "tab_mtd": "📅 MTD Growth Rate Estimate",
        "tab_abs": "📊 7D Growth Forecast",
        "growth_7d_abs_col": "7D Growth Rate Estimate",
        "no_trend_data": "No members with enough data to compute trends (need ≥ 2 records each).",
        "stagnant_list": "Stale members",
        "insuf_list": "Insufficient data members",
        "forecast_label": "forecast",
    },
    "日本語": {
        "title": "🏛️ X インフルエンス・データー",
        "pulse": "システムパルス",
        "monitored": "監視対象",
        "agencies": "企業",
        "total_reach": "総フォロワー数",
        "active_nodes": "活動中",
        "authorities": "収録",
        "total_followers": "トータル",
        "follower_level": "🎚️ フォロワー層",
        "level_all": "全て",
        "level_10k": "1万以内",
        "level_100k": "10万以内",
        "level_1m": "100万以内",
        "new_members": "🆕 新規メンバー：{}名",
        "power_ranking": "🏆 統計❘事務所",
        "distribution": "📊 ドーナツグラフ",
        "efficiency": "⚖️ メンバー影響力|平均",
        "top50": "🎯 トップ 50",
        "institution": "事務所",
        "total_influence": "影響力",
        "avg_account": "平均値",
        "accounts": "所属メンバー",
        "account_id": "Ｘアカウント",
        "office_col": "事務所",
        "followers_col": "フォロワー",
        "no_db": "データがありません。",
        "last_update": "最終更新",
        "7d_trend": "7日増加",
        "mtd_label": "本月増加予測",
        "daily_avg": "日均成長",
        "last_val": "最新フォロワー",
        "trend_stagnant": "30日以上更新なし",
        "trend_insufficient": "データ不足",
        "user_trend_title": "📈 トレンド",
        "trend_scope_all": "🌐 総合",
        "trend_scope_label": "スコープ",
        "rank_col": "順位",
        "username_col": "メンバー",
        "trend_7d_col": "7日伸び率|予測",
        "trend_mtd_col": "月間伸び率|予測",
        "daily_avg_col": "日均増加",
        "last_followers_col": "最新フォロワー",
        "tab_7d": "📊 7日伸び率",
        "tab_mtd": "📅 月間伸び率",
        "tab_abs": "📊 7日增加推定",
        "growth_7d_abs_col": "7日増加推定",
        "no_trend_data": "トレンド計算に必要なデータが不足しています（各メンバー最低2件必要）",
        "stagnant_list": "更新停止メンバー",
        "insuf_list": "データ不足メンバー",
        "forecast_label": "予想",
    },
}

# =========================
# 3. Global Styling (CSS)
# =========================
st.markdown(
    """
<style>
@keyframes meshGradient {
    0%   { background-position: 0%   50%; }
    50%  { background-position: 100% 50%; }
    100% { background-position: 0%   50%; }
}

.stApp {
    background-color: transparent !important;
}

.stApp::before {
    content: "";
    position: fixed;
    inset: 0;
    z-index: -1;
    pointer-events: none;

    background: linear-gradient(
        45deg,
        #cfe9ff,
        #ffd6e7,
        #bde3ff,
        #f5c6ff
    );

    background-size: 400% 400%;
    animation: meshGradient 40s ease infinite;

    opacity: 0.2;
}

@media (prefers-color-scheme: dark) {
    .stApp::before {
        background: linear-gradient(
            45deg,
            #05050a,
            #140a1f,
            #0a0714,
            #1c0f2a
        );

        background-size: 400% 400%;
        animation: meshGradient 70s ease infinite;

        opacity: 0.1;
    }
}

.stApp {
    color: inherit;
}

.gold-text-gradient {
    background: linear-gradient(135deg,
        #8A6E2F 0%, #D4AF37 25%, #C5A059 50%, #B8860B 75%, #8A6E2F 100%) !important;
    -webkit-background-clip: text !important;
    -webkit-text-fill-color: transparent !important;
    background-clip: text !important;
    display: inline-block;
}

.blue-text-gradient {
    background: linear-gradient(135deg, #003366, #0077B6) !important;
    -webkit-background-clip: text !important;
    -webkit-text-fill-color: transparent !important;
    background-clip: text !important;
    font-weight: 600;
    font-size: 0.9rem;
    display: inline-block;
}

div[data-testid="stMetricValue"] > div {
    background: linear-gradient(135deg, #8A6E2F, #D4AF37, #B8860B, #8A6E2F) !important;
    -webkit-background-clip: text !important;
    -webkit-text-fill-color: transparent !important;
    background-clip: text !important;
    font-family: 'Georgia', serif;
    font-weight: bold;
}

h1, h2, h3 {
    border-bottom: 2px solid #D4AF37;
    padding-bottom: 5px;
    margin-bottom: 15px;
}

div[data-testid="stProgress"] > div > div > div > div {
    background-image: linear-gradient(135deg, #8A6E2F, #D4AF37) !important;
}

[data-testid="stMetricLabel"] {
    color: color-mix(in srgb, currentColor 55%, transparent) !important;
    font-size: 1rem !important;
    letter-spacing: 1px;
}

.stCaption {
    color: color-mix(in srgb, currentColor 55%, transparent) !important;
}

/* DataFrame */
.stDataFrame {
    border: 1px solid rgba(212, 175, 55, 0.4);
    border-radius: 12px;
    background-color: color-mix(in srgb, currentColor 4%, transparent);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
}

[data-testid="stDataFrame"] th {
    color: color-mix(in srgb, currentColor 55%, transparent) !important;
    font-weight: bold !important;
    background-color: color-mix(in srgb, currentColor 6%, transparent) !important;
}

/* Bubble cards are rendered via components.html() — no global CSS needed here */
</style>
""",
    unsafe_allow_html=True,
)

# =========================
# 4. Database + Safe Download
# =========================
DB_PATH = Path("followers.db")
FILE_ID = "1oeHtcJNItV4eHmwr3ZAGudk3DgpOvi1G"
MIN_TIME_INTERVAL_DAYS = 0.01


def download_db():
    url = f"https://drive.google.com/uc?export=download&id={FILE_ID}"
    temp_path = DB_PATH.with_suffix(".tmp")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(temp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    temp_path.replace(DB_PATH)


if not DB_PATH.exists():
    download_db()


def get_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


# =========================
# 5. Card Profile Layer
# =========================
CARD_CSV = Path("card.csv")


@st.cache_data(ttl=3600)
def load_card_profiles() -> pd.DataFrame:
    if not CARD_CSV.exists():
        return pd.DataFrame(columns=["username", "name", "youtube_url", "avatar_url"])
    try:
        with open(CARD_CSV, "rb") as f:
            raw = f.read()

        encodings = ["utf-8-sig", "utf-8", "cp932", "gbk", "euc-jp"]
        text = None
        for enc in encodings:
            try:
                text = raw.decode(enc)
                print(f"✅ card.csv decoded with: {enc}")
                break
            except Exception:
                continue
        if text is None:
            text = raw.decode("utf-8", errors="replace")
            print("⚠️ fallback utf-8 replace")

        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
        df = pd.DataFrame(rows)
        if "username" in df.columns:
            df["username"] = df["username"].astype(str).str.strip()
        return df
    except Exception as e:
        print("❌ card.csv load failed:", e)
        return pd.DataFrame(columns=["username", "name", "youtube_url", "avatar_url"])


# =========================
# 6. Data Layer
# =========================
@st.cache_data(ttl=300)
def load_available_dates():
    con = get_connection()
    try:
        df = pd.read_sql("SELECT DISTINCT date FROM followers ORDER BY date DESC", con)
        return df["date"].tolist()
    finally:
        con.close()


@st.cache_data(ttl=300)
def load_offices():
    con = get_connection()
    try:
        df = pd.read_sql("SELECT DISTINCT office FROM followers ORDER BY office", con)
        return df["office"].tolist()
    finally:
        con.close()


@st.cache_data(ttl=300)
def load_current(selected_date: str):
    con = get_connection()
    try:
        df = pd.read_sql(
            "SELECT username, office, followers FROM followers WHERE date = ?",
            con,
            params=(selected_date,),
        )
        if not df.empty:
            df["followers"] = (
                pd.to_numeric(df["followers"], errors="coerce").fillna(0).astype(int)
            )
        return df
    finally:
        con.close()


@st.cache_data(ttl=300)
def load_new_members(selected_date: str):
    con = get_connection()
    try:
        df = pd.read_sql(
            """
            SELECT username, office, followers FROM followers t
            WHERE date = ?
              AND NOT EXISTS (
                  SELECT 1 FROM followers
                  WHERE username = t.username AND date < ?
              )
            ORDER BY followers DESC
            """,
            con,
            params=(selected_date, selected_date),
        )
        if not df.empty:
            df["followers"] = (
                pd.to_numeric(df["followers"], errors="coerce").fillna(0).astype(int)
            )
        return df
    finally:
        con.close()


@st.cache_data(ttl=300)
def load_all_history():
    con = get_connection()
    try:
        df = pd.read_sql(
            "SELECT username, office, date, followers FROM followers ORDER BY username, date",
            con,
        )
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df["followers"] = (
                pd.to_numeric(df["followers"], errors="coerce").fillna(0).astype(int)
            )
        return df
    finally:
        con.close()


def style_dataframe(df):
    return df.style.set_properties(
        **{
            "color": "#008B8B",
            "font-style": "italic",
            "font-weight": "500",
        }
    )


# =========================
# 7. Trend Logic
# =========================
def get_trends_single(user_df):
    """
    Compute member trend metrics for irregular update intervals.

    - 7D trend: find a baseline point around 7 days, estimate daily growth,
      derive 7-day absolute growth, then compute relative 7-day rate.
    - MTD trend: use first and latest point within current month, estimate
      daily growth, project full-month growth, then compute relative MTD rate.
    """

    if len(user_df) < 2:
        return {
            "status": "insufficient",
            "7d": None,
            "mtd": None,
            "growth_7d_abs": None,
            "growth_mtd_abs": None,
            "last_update": None,
            "last_val": None,
            "prev_val": None,
            "daily_avg_7d": None,
            "daily_avg_mtd": None,
        }

    # Ensure rows are sorted by date.
    user_df = user_df.sort_values("date").reset_index(drop=True)

    last_row = user_df.iloc[-1]
    last_update = pd.to_datetime(last_row["date"])
    last_val = float(last_row["followers"])

    prev_row = user_df.iloc[-2]
    prev_val = float(prev_row["followers"])

    today = pd.Timestamp.today()

    # Mark stale records if the latest update is older than 30 days.
    if (today - last_update).days > 30:
        return {
            "status": "stagnant",
            "7d": None,
            "mtd": None,
            "growth_7d_abs": None,
            "growth_mtd_abs": None,
            "last_update": last_update.date(),
            "last_val": int(last_val),
            "prev_val": int(prev_val),
            "daily_avg_7d": None,
            "daily_avg_mtd": None,
        }

    # =====================================================
    # Part A: 7D trend (supports irregular update cadence).
    # =====================================================

    latest_date = last_update
    latest_val = last_val

    hist_before_latest = user_df.iloc[:-1].copy()

    if hist_before_latest.empty:
        return {
            "status": "insufficient",
            "7d": None,
            "mtd": None,
            "growth_7d_abs": None,
            "growth_mtd_abs": None,
            "last_update": last_update.date(),
            "last_val": int(last_val),
            "prev_val": int(prev_val),
            "daily_avg_7d": None,
            "daily_avg_mtd": None,
        }

    hist_before_latest["day_diff"] = (
        latest_date - pd.to_datetime(hist_before_latest["date"])
    ).dt.days

    # Prefer the farthest (earliest) point within 7 days.
    within_7d = hist_before_latest[
        (hist_before_latest["day_diff"] >= 0) & (hist_before_latest["day_diff"] <= 7)
    ]

    if not within_7d.empty:
        # Select farthest row (max day_diff).
        base_row_7d = within_7d.sort_values("day_diff", ascending=False).iloc[0]
    else:
        # If none within 7 days, choose nearest point beyond 7 days.
        over_7d = hist_before_latest[hist_before_latest["day_diff"] > 7]

        if not over_7d.empty:
            base_row_7d = over_7d.sort_values("day_diff", ascending=True).iloc[0]
        else:
            base_row_7d = hist_before_latest.iloc[-1]

    old_date_7d = pd.to_datetime(base_row_7d["date"])
    old_val_7d = float(base_row_7d["followers"])

    delta_days_7d = max((latest_date - old_date_7d).days + 1, 1)

    daily_avg_7d = (latest_val - old_val_7d) / delta_days_7d

    growth_7d_abs = daily_avg_7d * 7
    base_7d = latest_val - growth_7d_abs

    if base_7d > 0:
        trend_7d = growth_7d_abs / base_7d
    else:
        trend_7d = None

    # =====================================================
    # Part B: MTD trend (full-month projection).
    # =====================================================

    current_year = latest_date.year
    current_month = latest_date.month

    days_in_month = calendar.monthrange(current_year, current_month)[1]

    month_df = user_df[
        (pd.to_datetime(user_df["date"]).dt.year == current_year)
        & (pd.to_datetime(user_df["date"]).dt.month == current_month)
    ].copy()

    trend_mtd = None
    growth_mtd_abs = None
    daily_avg_mtd = None

    if len(month_df) >= 2:
        month_df = month_df.sort_values("date").reset_index(drop=True)

        # Use earliest and latest points in current month.
        first_month_row = month_df.iloc[0]
        latest_month_row = month_df.iloc[-1]

        first_month_date = pd.to_datetime(first_month_row["date"])
        first_month_val = float(first_month_row["followers"])

        latest_month_date = pd.to_datetime(latest_month_row["date"])
        latest_month_val = float(latest_month_row["followers"])

        delta_days_mtd = max((latest_month_date - first_month_date).days + 1, 1)

        daily_avg_mtd = (latest_month_val - first_month_val) / delta_days_mtd

        # latest_day: e.g., day 20 -> 20.
        elapsed_days = latest_month_date.day

        # Back-calculate month-start baseline.
        base_mtd = latest_month_val - (daily_avg_mtd * elapsed_days)

        # Project full-month absolute growth.
        growth_mtd_abs = daily_avg_mtd * days_in_month

        if base_mtd > 0:
            trend_mtd = growth_mtd_abs / base_mtd
        else:
            trend_mtd = None

    return {
        "status": "active",
        "7d": trend_7d,
        "mtd": trend_mtd,
        "growth_7d_abs": growth_7d_abs,
        "growth_mtd_abs": growth_mtd_abs,
        "last_update": last_update.date(),
        "last_val": int(last_val),
        "prev_val": int(prev_val),
        "daily_avg_7d": daily_avg_7d,
        "daily_avg_mtd": daily_avg_mtd,
    }


@st.cache_data(ttl=300)
def compute_all_trends():
    hist = load_all_history()
    if hist.empty:
        return pd.DataFrame()
    rows = []
    for username, grp in hist.groupby("username", sort=False):
        grp = grp.sort_values("date")
        office = grp["office"].iloc[-1]
        t = get_trends_single(grp)
        rows.append(
            {
                "username": username,
                "office": office,
                "last_val": t["last_val"],
                "last_update": t["last_update"],
                "trend_7d": t["7d"],
                "trend_mtd": t["mtd"],
                "growth_7d_abs": t["growth_7d_abs"],
                "growth_mtd_abs": t["growth_mtd_abs"],
                "daily_avg_7d": t["daily_avg_7d"],
                "daily_avg_mtd": t["daily_avg_mtd"],
                "status": t["status"],
            }
        )
    return pd.DataFrame(rows)


# =========================
# 8. Color / Format Helpers
# =========================
def soft_gradient_bar():
    return "linear-gradient(90deg, #6CA6FF, #FF6B6B, #A66BFF)"


def safe_rainbow_colors(n):
    palette = [
        "#FF4D4D",
        "#FF6A00",
        "#FF1493",
        "#1E90FF",
        "#8A2BE2",
        "#DC143C",
        "#7B68EE",
    ]
    return [random.choice(palette) for _ in range(n)]


def fmt_num(val):
    if val is None:
        return "—"
    if val > 0:
        return f"🔺 {val:,.0f}"
    elif val < 0:
        return f"🔻 {abs(val):,.0f}"
    else:
        return f"{val:,.0f}"


def _pct_display(row, col):
    if row["status"] == "stagnant":
        return "⚠️"
    if row["status"] == "insufficient" or row[col] is None:
        return "—"
    v = row[col]
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.2%}"


def _daily_display(row, col):
    if row["status"] != "active" or row[col] is None:
        return "—"
    return fmt_num(row[col])


# =========================
# 9. Bubble Card Renderer
# =========================

# CSS injected inside each components.html iframe so it is never sanitized
BUBBLE_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@400;700;900&display=swap');

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

body {
    background: transparent;
    font-family: 'Noto Sans JP', system-ui, sans-serif;
    color: #1a1a2e;
    padding: 4px 2px;
}

@media (prefers-color-scheme: dark) {
    body { color: #e8e8f0; }
}

@keyframes cardFadeIn {
    from { opacity: 0; }
    to   { opacity: 1; }
}

.bubble-table-wrapper {
    display: flex;
    flex-direction: column;
    gap: 6px;
}

.bubble-card {
    display: grid;
    grid-template-columns: 44px 1fr auto;
    gap: 0 10px;
    align-items: center;
    padding: 7px 10px 7px 8px;
    border-radius: 14px;
    background: rgba(255,255,255,0.55);
    backdrop-filter: blur(20px);
    -webkit-backdrop-filter: blur(20px);
    border: 1px solid rgba(212,175,55,0.25);
    box-shadow: 0 2px 14px rgba(0,0,0,0.07), inset 0 1px 0 rgba(255,255,255,0.6);
    transition: box-shadow 0.2s, opacity 0.2s;
    animation: cardFadeIn 0.32s ease both;
    position: relative;
    overflow: hidden;
    /* ── overlap fix ──────────────────────────────────────────────────────
       will-change:opacity  → compositor promotes each card to its own layer,
                              so the translateY in cardFadeIn never bleeds into
                              a neighbour's paint rect.
       flex-shrink:0        → prevents the flex engine from squashing cards
                              when many are rendered simultaneously.
       min-height:52px      → guarantees a stable floor height even if inner
                              content hasn't fully painted yet.
    ──────────────────────────────────────────────────────────────────── */
    will-change: opacity;
    flex-shrink: 0;
    min-height: 52px;
}

@media (prefers-color-scheme: dark) {
    .bubble-card {
        background: rgba(20,14,35,0.6);
        border-color: rgba(212,175,55,0.2);
        box-shadow: 0 2px 14px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.06);
    }
}

.bubble-card::before {
    content: "";
    position: absolute;
    inset: 0;
    border-radius: 16px;
    background: linear-gradient(135deg, rgba(212,175,55,0.07) 0%, transparent 55%);
    pointer-events: none;
}

.bubble-card:hover {
    /* No translateY — vertical shift in a packed list causes neighbours to
       visually overlap.  Use box-shadow + opacity lift instead. */
    box-shadow: 0 4px 22px rgba(212,175,55,0.28), inset 0 1px 0 rgba(255,255,255,0.7);
    opacity: 0.92;
}

/* Rank badge — top-left corner */
.bubble-rank {
    position: absolute;
    top: 7px;
    left: 9px;
    font-size: 9px;
    font-weight: 900;
    opacity: 0.4;
    line-height: 1;
    letter-spacing: 0.3px;
}

/* Top-3 accents */
.rank-1 { border-color: rgba(212,175,55,0.6) !important; box-shadow: 0 0 0 1px rgba(212,175,55,0.3), 0 4px 18px rgba(212,175,55,0.15) !important; }
.rank-2 { border-color: rgba(192,192,192,0.55) !important; box-shadow: 0 0 0 1px rgba(192,192,192,0.28), 0 3px 14px rgba(192,192,192,0.12) !important; }
.rank-3 { border-color: rgba(205,127,50,0.5) !important; box-shadow: 0 0 0 1px rgba(205,127,50,0.25), 0 3px 12px rgba(205,127,50,0.1) !important; }
.rank-1 .bubble-rank { opacity: 1; }
.rank-2 .bubble-rank { opacity: 0.85; }
.rank-3 .bubble-rank { opacity: 0.75; }

/* ── Avatar ── */
.bubble-avatar-col {
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
}
.bubble-avatar {
    width: 36px;
    height: 36px;
    border-radius: 50%;
    object-fit: cover;
    border: 1.5px solid rgba(212,175,55,0.5);
    display: block;
    background: rgba(212,175,55,0.08);
}
.bubble-avatar-placeholder {
    width: 36px;
    height: 36px;
    border-radius: 50%;
    background: linear-gradient(135deg, rgba(212,175,55,0.18), rgba(138,110,47,0.1));
    border: 1.5px solid rgba(212,175,55,0.35);
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 15px;
    color: rgba(212,175,55,0.65);
    flex-shrink: 0;
}

/* ── Identity ── */
.bubble-identity-col {
    display: flex;
    flex-direction: column;
    gap: 3px;
    min-width: 0;
}
.bubble-name-row {
    display: flex;
    align-items: center;
    gap: 5px;
    min-width: 0;
    overflow: hidden;
}
.bubble-jp-name {
    font-size: 12px;
    font-weight: 700;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    letter-spacing: 0.2px;
    line-height: 1.2;
    flex-shrink: 1;
    min-width: 0;
}
.bubble-yt-icon {
    display: inline-flex;
    align-items: center;
    flex-shrink: 0;
    opacity: 0.85;
    transition: opacity 0.15s;
    line-height: 1;
}
.bubble-yt-icon:hover { opacity: 1; }
.bubble-meta-row {
    display: flex;
    align-items: center;
    gap: 4px;
    flex-wrap: nowrap;
    overflow: hidden;
}
.bubble-username {
    font-size: 10px;
    opacity: 0.45;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    flex-shrink: 1;
    min-width: 0;
}
.bubble-office-dot {
    font-size: 10px;
    opacity: 0.3;
    flex-shrink: 0;
}
.bubble-office-tag {
    font-size: 10px;
    opacity: 0.5;
    font-weight: 600;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    flex-shrink: 1;
    min-width: 0;
}

/* ── Stats ── */
.bubble-stats-col {
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    gap: 3px;
    flex-shrink: 0;
    min-width: 80px;
    /* Hard cap prevents a long absstr badge from crushing the identity col */
    max-width: 140px;
    overflow: hidden;
}
.bubble-followers {
    font-size: 13px;
    font-weight: 900;
    letter-spacing: -0.3px;
    background: linear-gradient(135deg, #8A6E2F, #D4AF37, #B8860B);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    white-space: nowrap;
    font-variant-numeric: tabular-nums;
}
.bubble-metrics-row {
    display: flex;
    gap: 4px;
    align-items: center;
    /* nowrap: wrapping inflates card height unpredictably → iframe clip bug */
    flex-wrap: nowrap;
    justify-content: flex-end;
    overflow: hidden;
    max-width: 100%;
}
.bubble-badge {
    font-size: 9px;
    font-weight: 600;
    padding: 1px 5px;
    border-radius: 6px;
    white-space: nowrap;
    border: 1px solid transparent;
    line-height: 1.5;
    font-variant-numeric: tabular-nums;
}
.badge-positive  { color: #15803d; background: rgba(21,128,61,0.1);   border-color: rgba(21,128,61,0.22); }
.badge-negative  { color: #dc2626; background: rgba(220,38,38,0.1);   border-color: rgba(220,38,38,0.22); }
.badge-neutral   { color: #6b7280; background: rgba(107,114,128,0.1); border-color: rgba(107,114,128,0.18); }
.badge-stagnant  { color: #d97706; background: rgba(217,119,6,0.1);   border-color: rgba(217,119,6,0.22); }

@media (prefers-color-scheme: dark) {
    .badge-positive { color: #4ade80; background: rgba(74,222,128,0.1);  border-color: rgba(74,222,128,0.2); }
    .badge-negative { color: #f87171; background: rgba(248,113,113,0.1); border-color: rgba(248,113,113,0.2); }
    .badge-neutral  { color: #9ca3af; background: rgba(156,163,175,0.1); border-color: rgba(156,163,175,0.18); }
    .badge-stagnant { color: #fbbf24; background: rgba(251,191,36,0.1);  border-color: rgba(251,191,36,0.2); }
    .bubble-jp-name { color: #f0f0f8; }
}
</style>
"""


def _badge_html(text: str, badge_type: str = "neutral") -> str:
    return f'<span class="bubble-badge badge-{badge_type}">{text}</span>'


def _classify_badge(value_str: str) -> str:
    if value_str in ("—", ""):
        return "neutral"
    if value_str == "⚠️":
        return "stagnant"
    if value_str.startswith("+") or value_str.startswith("🔺"):
        return "positive"
    if value_str.startswith("-") or value_str.startswith("🔻"):
        return "negative"
    return "neutral"


def fmt_followers(n):
    if n is None:
        return "—"
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if n >= 10_000:
        return f"{n/1_000:.1f}K"
    return f"{n:,}"


def render_bubble_table(
    df: pd.DataFrame,
    profiles: pd.DataFrame,
    followers_col: str = "followers",
    extra_badges: list = None,
    max_rows: int = 50,
    L: dict = None,
    card_height_px: int = 58,
    fixed_iframe_height: int = 640,
):
    """
    Render a Bubble Table via components.html() so HTML is never sanitized by Streamlit.
    """
    if L is None:
        L = {}
    if extra_badges is None:
        extra_badges = []

    merged = df.merge(profiles, on="username", how="left")
    merged = merged.reset_index(drop=True)

    cards_html = []

    for i in range(min(len(merged), max_rows)):
        row = merged.iloc[i]
        rank = i + 1

        username = row.get("username", "")
        office = row.get("office", "")
        followers = row.get(followers_col, None)
        jp_name = row.get("name", None)
        youtube_url = row.get("youtube_url", None)
        avatar_url = row.get("avatar_url", None)

        rank_class = f"rank-{rank}" if rank <= 3 else ""

        # Avatar
        if avatar_url and str(avatar_url).startswith("http"):
            av = str(avatar_url).strip()
            avatar_html = (
                f'<img class="bubble-avatar" src="{av}" alt="" loading="lazy" '
                f"onerror=\"this.style.display='none';this.nextElementSibling.style.display='flex';\">"
                f'<div class="bubble-avatar-placeholder" style="display:none;">◉</div>'
            )
        else:
            avatar_html = '<div class="bubble-avatar-placeholder">◉</div>'

        # Name row: jp_name + YT icon
        name_raw = (
            str(jp_name)
            if jp_name and str(jp_name).strip() and str(jp_name) != "nan"
            else username
        )
        name_display = name_raw.replace("<", "&lt;").replace(">", "&gt;")

        # YouTube icon only (SVG)
        if youtube_url and str(youtube_url).strip().startswith("http"):
            yt_url = str(youtube_url).strip()
            yt_html = (
                f'<a class="bubble-yt-icon" href="{yt_url}" target="_blank" rel="noopener" title="YouTube">'
                f'<svg viewBox="0 0 24 24" width="13" height="13" fill="#cc0000" xmlns="http://www.w3.org/2000/svg">'
                f'<path d="M23.5 6.2a3 3 0 0 0-2.1-2.1C19.5 3.5 12 3.5 12 3.5s-7.5 0-9.4.6A3 3 0 0 0 .5 6.2 31 31 0 0 0 0 12a31 31 0 0 0 .5 5.8 3 3 0 0 0 2.1 2.1c1.9.6 9.4.6 9.4.6s7.5 0 9.4-.6a3 3 0 0 0 2.1-2.1A31 31 0 0 0 24 12a31 31 0 0 0-.5-5.8z"/>'
                f'<polygon points="9.75,15.02 15.5,12 9.75,8.98" fill="white"/>'
                f"</svg>"
                f"</a>"
            )
        else:
            yt_html = ""

        uname_display = username if username.startswith("@") else f"@{username}"
        office_display = str(office).replace("<", "&lt;") if office else ""
        fol_str = fmt_followers(int(followers)) if followers is not None else "—"

        # Extra badges
        badges_html = ""
        for col_name, _label in extra_badges:
            val_str = row[col_name] if col_name in merged.columns else "—"
            if val_str is None or (isinstance(val_str, float) and val_str != val_str):
                val_str = "—"
            # absstr always positive-styled (forecast value)
             if col_name == "absstr":
                val_s = str(val_str)
                if val_s == "—":
                    badges_html += _badge_html("—", "neutral")
                else:
                    badges_html += val_s
            else:
                btype = _classify_badge(str(val_str))
                badges_html += _badge_html(str(val_str).replace("<", "&lt;"), btype)

        rank_labels = {1: "🥇", 2: "🥈", 3: "🥉"}
        rank_display = rank_labels.get(rank, "")
        # Cap delay at 0.5 s so cards deep in the list don't sit invisible
        # for a distracting amount of time when many rows are rendered.
        delay = f"{min(i * 0.025, 0.5):.3f}s"

        # office separator dot
        office_sep = (
            f'<span class="bubble-office-dot">·</span>'
            f'<span class="bubble-office-tag">{office_display}</span>'
            if office_display
            else ""
        )

        cards_html.append(
            f"""
<div class="bubble-card {rank_class}" style="animation-delay:{delay}">
  <span class="bubble-rank">{rank_display}</span>
  <div class="bubble-avatar-col">{avatar_html}</div>
  <div class="bubble-identity-col">
    <div class="bubble-name-row"><span class="bubble-jp-name">{name_display}</span>{yt_html}</div>
    <div class="bubble-meta-row"><span class="bubble-username">{uname_display}</span>{office_sep}</div>
  </div>
  <div class="bubble-stats-col">
    <div class="bubble-followers">{fol_str}</div>
    <div class="bubble-metrics-row">{badges_html}</div>
  </div>
</div>"""
        )

    # ── Scrollable wrapper injected INSIDE the iframe ──────────────────────
    # Because components.html() renders in an isolated iframe, outer-page CSS
    # cannot control internal layout.  We fix this by:
    #   1. Capping the iframe at a comfortable fixed height (IFRAME_H).
    #   2. Wrapping all cards in an inner <div> with overflow-y: auto so the
    #      user gets a per-column scrollbar that always shows every card,
    #      regardless of how many rows the current data-range filter produces.
    # ────────────────────────────────────────────────────────────────────────
    CARD_H = card_height_px  # px — caller can override for wide-badge columns
    GAP = 6  # px — gap between cards (matches .bubble-table-wrapper gap)
    PADDING = 8  # px — body top+bottom padding
    MAX_SHOW = 10  # cards visible before scrolling kicks in

    n_cards = min(len(cards_html), max_rows)
    # Natural height if every card fits without scrolling
    natural_h = n_cards * CARD_H + (n_cards - 1) * GAP + PADDING
    # Cap: show at most MAX_SHOW cards before the column scrolls
    max_h = MAX_SHOW * CARD_H + (MAX_SHOW - 1) * GAP + PADDING
    # iframe height: whichever is smaller (no wasted blank space)
    iframe_h = fixed_iframe_height

    scroll_style = (
        f"max-height:{max_h}px;"
        "overflow-y:auto;"
        "overflow-x:hidden;"
        # Thin custom scrollbar (WebKit)
        "scrollbar-width:thin;"
        "scrollbar-color:rgba(212,175,55,0.45) transparent;"
    )

    inner_div = (
        f'<div class="bubble-table-wrapper" style="{scroll_style}">'
        + "".join(cards_html)
        + "</div>"
    )

    # WebKit scrollbar fine-tuning (must live inside the iframe <style>)
    scrollbar_css = """
<style>
.bubble-table-wrapper::-webkit-scrollbar        { width: 5px; }
.bubble-table-wrapper::-webkit-scrollbar-track  { background: transparent; }
.bubble-table-wrapper::-webkit-scrollbar-thumb  {
    background: rgba(212,175,55,0.4);
    border-radius: 999px;
}
.bubble-table-wrapper::-webkit-scrollbar-thumb:hover {
    background: rgba(212,175,55,0.7);
}
</style>
"""

    full_html = BUBBLE_CSS + scrollbar_css + inner_div
    components.html(full_html, height=iframe_h, scrolling=False)


# =========================
# 10. Sidebar
# =========================
with st.sidebar:
    lang = st.radio(
        "🌐 语言 / Language / 言語", ["中文", "English", "日本語"], horizontal=True
    )
    L = LANG_LABELS[lang]

    st.markdown("---")
    st.markdown(f"### {L['follower_level']}")
    level_options = [L["level_all"], L["level_10k"], L["level_100k"], L["level_1m"]]
    follower_level = st.radio("", level_options, index=0)


# =========================
# 11. Load Data
# =========================
dates = load_available_dates()
if not dates:
    st.error(L["no_db"])
    st.stop()

selected_date = dates[0]
current_df = load_current(selected_date)
new_members = load_new_members(selected_date)
card_profiles = load_card_profiles()

if current_df.empty:
    st.warning(L.get("no_data", "No data."))
    st.stop()

# Apply follower tier filter
filtered_df = current_df.copy()
if follower_level == L["level_10k"]:
    filtered_df = filtered_df[filtered_df["followers"] < 10_000]
elif follower_level == L["level_100k"]:
    filtered_df = filtered_df[filtered_df["followers"] < 100_000]
elif follower_level == L["level_1m"]:
    filtered_df = filtered_df[filtered_df["followers"] < 1_000_000]


# =========================
# 12. Header
# =========================
_title_emoji, _title_text = L["title"].split(" ", 1)
st.markdown(
    f'<h1>{_title_emoji} <span class="gold-text-gradient">{_title_text}</span></h1>',
    unsafe_allow_html=True,
)
st.markdown(
    f'<span class="blue-text-gradient">{L["pulse"]}: {selected_date}</span>',
    unsafe_allow_html=True,
)

# =========================
# 13. title KPI
# =========================
m1, m2, m3 = st.columns(3)
m1.metric(L["monitored"], f"{len(filtered_df):,}", L["active_nodes"])
m2.metric(L["agencies"], f"{filtered_df['office'].nunique()}", L["authorities"])
m3.metric(
    L["total_reach"], f"{int(filtered_df['followers'].sum()):,}", L["total_followers"]
)

st.markdown("<br>", unsafe_allow_html=True)

# =========================
# 14. New Members — Bubble Cards
# =========================
if not new_members.empty:
    with st.expander(L["new_members"].format(len(new_members)), expanded=True):
        render_bubble_table(
            new_members.sort_values("followers", ascending=False),
            card_profiles,
            followers_col="followers",
            L=L,
        )

# =========================
# 15. Aggregation
# =========================
office_rank = (
    filtered_df.groupby("office", as_index=False)
    .agg(
        followers=("followers", "sum"),
        AvgFollowers=("followers", "mean"),
        Count=("followers", "count"),
    )
    .assign(AvgFollowers=lambda x: x["AvgFollowers"].round(0).astype(int))
    .sort_values("followers", ascending=False)
)

avg_rank = (
    office_rank.sort_values("AvgFollowers", ascending=False)
    .head(20)
    .reset_index(drop=True)
)
label_colors = safe_rainbow_colors(len(avg_rank))

# =========================
# 16. Power Ranking + Pie
# =========================
col1, col2 = st.columns([1, 1])

with col1:
    st.markdown(
        f'<h3>🏆 <span class="gold-text-gradient">{L["power_ranking"].split(" ", 1)[1]}</span></h3>',
        unsafe_allow_html=True,
    )
    st.dataframe(
        style_dataframe(office_rank),
        use_container_width=True,
        hide_index=True,
        column_config={
            "office": L["institution"],
            "followers": st.column_config.ProgressColumn(
                L["total_influence"],
                format="%d",
                min_value=0,
                max_value=(
                    int(office_rank["followers"].max()) if not office_rank.empty else 1
                ),
            ),
            "AvgFollowers": st.column_config.NumberColumn(
                L["avg_account"], format="%d"
            ),
            "Count": st.column_config.NumberColumn(L["accounts"], format="%d"),
        },
    )

with col2:
    st.markdown(
        f'<h3>📊 <span class="gold-text-gradient">{L["distribution"].split(" ", 1)[1]}</span></h3>',
        unsafe_allow_html=True,
    )

    if not office_rank.empty:
        thermal_base = [
            "#FF2200",
            "#FF6600",
            "#FF9900",
            "#FFCC00",
            "#CC0066",
            "#8800AA",
            "#FF4400",
            "#FFB300",
            "#DD0033",
            "#FF5500",
            "#AA0055",
            "#FF7700",
        ]
        thermal_inner = [
            "#FF6644",
            "#FFAA44",
            "#FFDD44",
            "#FFEE88",
            "#FF44AA",
            "#BB44DD",
            "#FF8866",
            "#FFCC66",
            "#FF4466",
            "#FF9966",
            "#DD4488",
            "#FFBB44",
        ]
        text_colors_pool = [
            "#FFFF00",
            "#00FFFF",
            "#ADFF2F",
            "#FF69B4",
            "#FFFFFF",
            "#FFD700",
            "#7FFF00",
            "#FF4500",
            "#40E0D0",
            "#FFEC00",
            "#E0E0FF",
            "#FF1493",
        ]
        n = len(office_rank)

        def _cycle(lst, length):
            return (lst * ((length // len(lst)) + 1))[:length]

        bc = _cycle(thermal_base, n)
        brc = _cycle(thermal_inner, n)
        tc = _cycle(text_colors_pool, n)

        trace_outer = go.Pie(
            labels=office_rank["office"],
            values=office_rank["followers"],
            hole=0.30,
            textposition="inside",
            texttemplate="<b>%{label}<br>%{percent}</b>",
            insidetextorientation="radial",
            marker=dict(colors=bc, line=dict(color="rgba(255,255,255,0.18)", width=1)),
            textfont=dict(size=11, color=tc, family="Arial Black, sans-serif"),
            direction="clockwise",
            sort=False,
            name="outer",
        )
        trace_inner = go.Pie(
            labels=office_rank["office"],
            values=office_rank["followers"],
            hole=0.46,
            textinfo="none",
            marker=dict(colors=brc, line=dict(color="rgba(0,0,0,0)", width=0)),
            opacity=0.45,
            direction="clockwise",
            sort=False,
            name="inner",
        )
        fig_pie = go.Figure(data=[trace_outer, trace_inner])
        fig_pie.update_layout(
            height=420,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color=None),
            showlegend=False,
            margin=dict(t=30, b=30, l=10, r=10),
        )
        st.plotly_chart(fig_pie, use_container_width=True)

st.divider()

# =========================
# 17. Growth Trend Ranking — Bubble Cards
# =========================
st.markdown(
    f'<h3>📈 <span class="gold-text-gradient">{L["user_trend_title"].split(" ", 1)[1]}</span></h3>',
    unsafe_allow_html=True,
)

all_offices = load_offices()
scope_options = [L["trend_scope_all"]] + all_offices
trend_scope = st.selectbox(
    L["trend_scope_label"],
    scope_options,
    key="trend_scope_selector",
)

trends_df = compute_all_trends()

if trends_df.empty:
    st.info(L["no_trend_data"])
else:
    if trend_scope != L["trend_scope_all"]:
        scope_trends = trends_df[trends_df["office"] == trend_scope].copy()
    else:
        scope_trends = trends_df.copy()

    valid_users = set(filtered_df["username"].tolist())
    scope_trends = scope_trends[scope_trends["username"].isin(valid_users)].copy()

    if scope_trends.empty:
        st.info(L["no_trend_data"])
    else:
        scope_trends["d7str"] = scope_trends.apply(
            lambda r: _pct_display(r, "trend_7d"), axis=1
        )
        scope_trends["mtdstr"] = scope_trends.apply(
            lambda r: _pct_display(r, "trend_mtd"), axis=1
        )
        scope_trends["daily7dstr"] = scope_trends.apply(
            lambda r: _daily_display(r, "daily_avg_7d"), axis=1
        )
        scope_trends["dailymtdstr"] = scope_trends.apply(
            lambda r: _daily_display(r, "daily_avg_mtd"), axis=1
        )

        # 7D absolute growth as "[预计] 🔺 N" — forecast label bubble + arrow + number.
        def _abs_fmt(row, forecast_lbl):
            v = row["growth_7d_abs"]
            if v is None or (isinstance(v, float) and v != v):
                return "—"
            v = int(round(v))
            arrow = "🔺" if v >= 0 else "🔻"
            # Keep badge narrow: arrow + number only; label is in header.
            return f"{arrow} {abs(v):,}"

        # Three side-by-side columns: 7D / MTD / ABS.
        col7d, colmtd, colabs = st.columns(3)

        # 7D column.
        with col7d:
            st.markdown(f"**{L['tab_7d']}**")
            rank7d = (
                scope_trends[scope_trends["trend_7d"].notna()]
                .sort_values("trend_7d", ascending=False)
                .reset_index(drop=True)
            )
            stag7d = scope_trends[scope_trends["status"] == "stagnant"]
            insuf7d = scope_trends[
                (scope_trends["status"] == "insufficient")
                | (
                    (scope_trends["status"] == "active")
                    & scope_trends["trend_7d"].isna()
                )
            ]
            if not rank7d.empty:
                rank7d_display = rank7d.rename(columns={"last_val": "followers"})
                render_bubble_table(
                    rank7d_display,
                    card_profiles,
                    followers_col="followers",
                    extra_badges=[
                        ("d7str", L["trend_7d_col"]),
                        ("daily7dstr", L["daily_avg_col"]),
                    ],
                    L=L,
                )
            else:
                st.info(L["no_trend_data"])
            if not stag7d.empty:
                st.caption(
                    f"⚠️ {L['stagnant_list']} ({len(stag7d)}): "
                    + ", ".join(f"@{u}" for u in stag7d["username"].tolist())
                )
            if not insuf7d.empty:
                st.caption(f"— {L['insuf_list']}: {len(insuf7d)}")

        # MTD column.
        with colmtd:
            st.markdown(f"**{L['tab_mtd']}**")
            rankmtd = (
                scope_trends[scope_trends["trend_mtd"].notna()]
                .sort_values("trend_mtd", ascending=False)
                .reset_index(drop=True)
            )
            stagmtd = scope_trends[scope_trends["status"] == "stagnant"]
            insufmtd = scope_trends[
                (scope_trends["status"] == "insufficient")
                | (
                    (scope_trends["status"] == "active")
                    & scope_trends["trend_mtd"].isna()
                )
            ]
            if not rankmtd.empty:
                rankmtd_display = rankmtd.rename(columns={"last_val": "followers"})
                render_bubble_table(
                    rankmtd_display,
                    card_profiles,
                    followers_col="followers",
                    extra_badges=[
                        ("mtdstr", L["trend_mtd_col"]),
                        ("dailymtdstr", L["daily_avg_col"]),
                    ],
                    L=L,
                )
            else:
                st.info(L["no_trend_data"])
            if not stagmtd.empty:
                st.caption(
                    f"⚠️ {L['stagnant_list']} ({len(stagmtd)}): "
                    + ", ".join(f"@{u}" for u in stagmtd["username"].tolist())
                )
            if not insufmtd.empty:
                st.caption(f"— {L['insuf_list']}: {len(insufmtd)}")

        # 7D absolute growth column.
        with colabs:
            st.markdown(f"**{L['tab_abs']}**")
            rankabs = (
                scope_trends[scope_trends["growth_7d_abs"].notna()]
                .sort_values("growth_7d_abs", ascending=False)
                .reset_index(drop=True)
            )
            if not rankabs.empty:
                fc_lbl = L.get("forecast_label", "forecast")
                rankabs["absstr"] = rankabs.apply(lambda r: _abs_fmt(r, fc_lbl), axis=1)
                rankabs_display = rankabs.rename(columns={"last_val": "followers"})
                render_bubble_table(
                    rankabs_display,
                    card_profiles,
                    followers_col="followers",
                    extra_badges=[("absstr", L["growth_7d_abs_col"])],
                    L=L,
                    card_height_px=64,
                )
            else:
                st.info("None")

st.divider()

# =========================
# 18. Efficiency Ranking
# =========================
st.markdown(
    f'<h3>⚖️ <span class="gold-text-gradient">{L["efficiency"].split(" ", 1)[1]}</span></h3>',
    unsafe_allow_html=True,
)

if not avg_rank.empty:
    max_avg = avg_rank["AvgFollowers"].max()
    for i, row in avg_rank.iterrows():
        pct = row["AvgFollowers"] / max_avg if max_avg > 0 else 0
        left, right = st.columns([1.3, 5])
        with left:
            st.markdown(
                f"""
                <div style="color:{label_colors[i]};font-weight:700;font-size:15px;
                    padding-top:6px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">
                    {row["office"]}
                </div>
                """,
                unsafe_allow_html=True,
            )
        with right:
            st.markdown(
                f"""
                <div style="width:100%;height:14px;background:rgba(128,128,128,0.2);
                    border-radius:999px;overflow:hidden;box-shadow:inset 0 0 4px rgba(0,0,0,0.08);
                    margin-top:8px;">
                    <div style="width:{pct*100:.1f}%;height:100%;border-radius:999px;
                        background:{soft_gradient_bar()};"></div>
                </div>
                <div style="text-align:right;font-size:12px;color:inherit;opacity:0.6;
                    margin-top:2px;font-weight:600;">
                    {row["AvgFollowers"]:,}
                </div>
                """,
                unsafe_allow_html=True,
            )

st.divider()

# =========================
# 19. Top 50 — Bubble Cards
# =========================
st.markdown(
    f'<h3>🎯 <span class="gold-text-gradient">{L["top50"].split(" ", 1)[1]}</span></h3>',
    unsafe_allow_html=True,
)

top_50 = (
    filtered_df[["username", "office", "followers"]]
    .sort_values("followers", ascending=False)
    .head(50)
    .reset_index(drop=True)
)

render_bubble_table(
    top_50,
    card_profiles,
    followers_col="followers",
    L=L,
)
