"""
app.py -- BRVM Analytics Dashboard
Author: Kouame Ruben
Stack: Streamlit + Plotly
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import subprocess
import sys
import os
from pathlib import Path

st.set_page_config(page_title="BRVM Analytics", page_icon="", layout="wide")

# -- Auto-run pipeline if data missing --
def ensure_data():
    dashboard_dir = Path(__file__).resolve().parent
    project_root = dashboard_dir.parent
    os.chdir(project_root)
    
    check = project_root / "data" / "processed" / "brvm_scored.parquet"
    if not check.exists():
        with st.spinner("Generating BRVM data... Running pipeline (~10s)..."):
            env = os.environ.copy()
            env["PYTHONIOENCODING"] = "utf-8"
            result = subprocess.run(
                [sys.executable, str(project_root / "python" / "pipeline.py")],
                capture_output=True, text=True,
                cwd=str(project_root), env=env, encoding="utf-8", errors="replace"
            )
            if result.returncode != 0:
                st.error(f"Pipeline failed: {result.stderr[-300:]}")
                st.stop()

ensure_data()

# -- Load data --
@st.cache_data
def load():
    root = Path(__file__).resolve().parent.parent
    base = root / "data" / "processed"
    return {
        "scored": pd.read_parquet(base / "brvm_scored.parquet"),
        "prices": pd.read_parquet(base / "brvm_technical.parquet"),
        "sectors": pd.read_parquet(base / "brvm_sector_summary.parquet"),
    }

data = load()
scored = data["scored"]
prices = data["prices"]
sectors = data["sectors"]

# -- Header --
st.markdown("# BRVM Analytics Dashboard")
st.markdown("*Analyse du marche boursier regional - 20 actions, 8 pays UEMOA*")
st.markdown("---")

# -- KPIs --
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Actions suivies", len(scored))
c2.metric("Rdt Dividende moyen", f"{scored['div_yield_pct'].mean():.1f}%")
c3.metric("PER moyen", f"{scored['pe_ratio'].mean():.1f}")
buy_count = (scored["signal"] == "BUY").sum()
c4.metric("Signaux BUY", buy_count)
c5.metric("Score moyen", f"{scored['composite_score'].mean():.0f}/100")

st.markdown("---")

tab1, tab2, tab3, tab4 = st.tabs(["Ranking", "Graphiques", "Analyse Sectorielle", "Simulateur Dividendes"])

# -- TAB 1: RANKING --
with tab1:
    st.markdown("### Classement par score d'attractivite")
    
    sector_filter = st.multiselect("Filtrer par secteur", scored["sector"].unique().tolist(),
                                     default=scored["sector"].unique().tolist())
    filtered = scored[scored["sector"].isin(sector_filter)].copy()
    
    colors = filtered["composite_score"].apply(
        lambda s: "#10B981" if s >= 70 else "#F59E0B" if s >= 55 else "#EF4444")
    
    fig = go.Figure(go.Bar(
        x=filtered["composite_score"], y=filtered["ticker"],
        orientation="h", text=filtered["signal"],
        marker_color=colors, textposition="outside"
    ))
    fig.update_layout(height=max(400, len(filtered) * 35), template="plotly_white",
                       xaxis_title="Score /100", yaxis=dict(autorange="reversed"),
                       margin=dict(l=80, r=120))
    st.plotly_chart(fig, width='stretch')
    
    st.markdown("### Details")
    desired_cols = ["rank", "ticker", "company", "sector", "price",
                    "pe_ratio", "div_yield_pct", "roe_pct", "rsi_14", "composite_score", "signal"]
    display_cols = [c for c in desired_cols if c in filtered.columns]
    st.dataframe(filtered[display_cols].reset_index(drop=True), width='stretch')

# -- TAB 2: CHARTS --
with tab2:
    st.markdown("### Evolution des cours")
    
    selected_ticker = st.selectbox("Choisir une action", scored["ticker"].tolist())
    df_t = prices[prices["ticker"] == selected_ticker].sort_values("date").copy()
    
    if len(df_t) > 0:
        fig2 = go.Figure()
        fig2.add_trace(go.Candlestick(
            x=df_t["date"], open=df_t["open"], high=df_t["high"],
            low=df_t["low"], close=df_t["close"], name="OHLC"
        ))
        fig2.add_trace(go.Scatter(x=df_t["date"], y=df_t["ma_20"],
                                    mode="lines", name="MA 20", line=dict(color="#0EA5E9", width=1)))
        fig2.add_trace(go.Scatter(x=df_t["date"], y=df_t["ma_50"],
                                    mode="lines", name="MA 50", line=dict(color="#F59E0B", width=1)))
        
        info = scored[scored["ticker"] == selected_ticker].iloc[0]
        title_parts = [f"{info['company']} ({selected_ticker})", str(info.get('sector', ''))]
        fig2.update_layout(height=450, template="plotly_white",
                            title=" - ".join([p for p in title_parts if p]),
                            xaxis_rangeslider_visible=False)
        st.plotly_chart(fig2, width='stretch')
        
        # RSI chart
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**RSI (14)** - Dernier: {df_t['rsi_14'].iloc[-1]:.0f}")
            fig_rsi = go.Figure()
            fig_rsi.add_trace(go.Scatter(x=df_t["date"], y=df_t["rsi_14"],
                                          mode="lines", line=dict(color="#8B5CF6")))
            fig_rsi.add_hline(y=70, line_dash="dash", line_color="#EF4444", annotation_text="Survendu")
            fig_rsi.add_hline(y=30, line_dash="dash", line_color="#10B981", annotation_text="Survend")
            fig_rsi.update_layout(height=250, template="plotly_white", yaxis_range=[0, 100])
            st.plotly_chart(fig_rsi, width='stretch')
        
        with col2:
            st.markdown(f"**MACD**")
            fig_macd = go.Figure()
            fig_macd.add_trace(go.Scatter(x=df_t["date"], y=df_t["macd"],
                                           mode="lines", name="MACD", line=dict(color="#0EA5E9")))
            fig_macd.add_trace(go.Scatter(x=df_t["date"], y=df_t["macd_signal"],
                                           mode="lines", name="Signal", line=dict(color="#F59E0B")))
            fig_macd.add_trace(go.Bar(x=df_t["date"], y=df_t["macd_hist"], name="Histogramme",
                                       marker_color=df_t["macd_hist"].apply(
                                           lambda x: "#10B981" if x > 0 else "#EF4444")))
            fig_macd.update_layout(height=250, template="plotly_white")
            st.plotly_chart(fig_macd, width='stretch')

# -- TAB 3: SECTOR --
with tab3:
    st.markdown("### Performance par secteur")
    
    fig3 = px.scatter(scored, x="pe_ratio", y="div_yield_pct", size="composite_score",
                       color="sector", hover_data=["ticker", "company", "signal"],
                       labels={"pe_ratio": "Price-to-Earnings", "div_yield_pct": "Rendement Dividende (%)"},
                       size_max=40)
    fig3.update_layout(height=450, template="plotly_white")
    st.plotly_chart(fig3, width='stretch')
    
    st.markdown("### Comparaison sectorielle")
    fmt = {}
    if "avg_score" in sectors.columns: fmt["avg_score"] = "{:.0f}"
    if "avg_pe" in sectors.columns: fmt["avg_pe"] = "{:.1f}"
    if "avg_div_yield" in sectors.columns: fmt["avg_div_yield"] = "{:.1f}"
    if "avg_roe" in sectors.columns: fmt["avg_roe"] = "{:.1f}"
    if fmt:
        st.dataframe(sectors.style.format(fmt), width='stretch')
    else:
        st.dataframe(sectors, width='stretch')
    
    # Sector score distribution
    st.markdown("### Score par secteur")
    if "sector" in scored.columns:
        fig4 = px.box(scored, x="sector", y="composite_score", color="sector",
                       labels={"composite_score": "Score", "sector": "Secteur"})
        fig4.update_layout(height=350, template="plotly_white", showlegend=False)
        st.plotly_chart(fig4, width='stretch')

# -- TAB 4: DIVIDEND SIMULATOR --
with tab4:
    st.markdown("### Simulateur de portefeuille dividendes")
    
    total_invest = st.slider("Montant a investir (FCFA)", 500000, 50000000, 5000000, 500000)
    n_stocks = st.slider("Nombre d'actions dans le portefeuille", 3, 10, 5)
    
    strategy = st.radio("Strategie de selection",
                         ["Top rendement dividende", "Top score composite", "Mix (score + dividende)"])
    
    def _normalize(series):
        rng = series.max() - series.min()
        if rng == 0: return pd.Series(50, index=series.index)
        return ((series - series.min()) / rng * 100)
    
    if strategy == "Top rendement dividende":
        selected = scored.nlargest(n_stocks, "div_yield_pct").copy()
    elif strategy == "Top score composite":
        selected = scored.nlargest(n_stocks, "composite_score").copy()
    else:
        tmp = scored.copy()
        tmp["mix_score"] = tmp["composite_score"] * 0.5 + _normalize(tmp["div_yield_pct"]) * 0.5
        selected = tmp.nlargest(n_stocks, "mix_score").copy()
    
    alloc = total_invest / n_stocks
    selected["allocation_fcfa"] = alloc
    selected["nb_actions"] = (alloc / selected["price"]).astype(int)
    
    # Compute dividend: use 'dividend' column if exists, else estimate from div_yield_pct
    if "dividend" in selected.columns and selected["dividend"].notna().any():
        selected["dividende_annuel"] = selected["nb_actions"] * selected["dividend"]
    elif "div_yield_pct" in selected.columns:
        selected["dividende_annuel"] = (selected["nb_actions"] * selected["price"] * selected["div_yield_pct"].fillna(0) / 100).astype(int)
    else:
        selected["dividende_annuel"] = 0
    
    selected["valeur_investie"] = selected["nb_actions"] * selected["price"]
    
    sim_cols = [c for c in ["ticker", "company", "sector", "price", "div_yield_pct",
                            "composite_score", "signal", "nb_actions", "dividende_annuel",
                            "valeur_investie"] if c in selected.columns]
    st.dataframe(selected[sim_cols].reset_index(drop=True), width='stretch')
    
    total_div = selected["dividende_annuel"].sum()
    total_val = selected["valeur_investie"].sum()
    yield_pct = 100 * total_div / total_val if total_val > 0 else 0
    
    st.markdown("---")
    r1, r2, r3 = st.columns(3)
    r1.metric("Montant investi", f"{total_val:,.0f} FCFA")
    r2.metric("Dividende annuel estime", f"{total_div:,.0f} FCFA")
    r3.metric("Rendement du portefeuille", f"{yield_pct:.1f}%")
    
    # Projection 5 ans
    st.markdown("### Projection sur 5 ans (reinvestissement des dividendes)")
    years = list(range(0, 6))
    capital = [total_val]
    for y in range(1, 6):
        new_cap = capital[-1] * (1 + yield_pct / 100)
        capital.append(new_cap)
    
    fig5 = go.Figure(go.Bar(x=years, y=capital, text=[f"{c/1e6:.1f}M" for c in capital],
                              textposition="outside", marker_color="#10B981"))
    fig5.update_layout(height=350, template="plotly_white",
                        xaxis_title="Annee", yaxis_title="Capital (FCFA)",
                        title=f"Croissance du capital avec reinvestissement ({yield_pct:.1f}%/an)")
    st.plotly_chart(fig5, width='stretch')

st.markdown("---")
st.caption("BRVM Analytics Dashboard - Kouame Ruben | [GitHub](https://github.com/kouameruben) | [LinkedIn](https://linkedin.com/in/kouameruben)")
