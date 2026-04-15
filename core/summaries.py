from collections import defaultdict
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd


def _split_source_etfs(source_value: str) -> List[str]:
    return [p.strip() for p in str(source_value).split(",") if p and p.strip()]


def build_etf_rsl_summary(
    stock_results: List[Any],
    selected_syms: List[str],
    etf_options: Dict[str, Dict[str, Any]],
    top_percent_threshold: float,
    config: Dict[str, Any],
) -> pd.DataFrame:
    etf_syms = list(dict.fromkeys([str(s).strip() for s in selected_syms if str(s).strip()]))
    etf_rsl_map: Dict[str, List[float]] = {sym: [] for sym in etf_syms}
    etf_rsl_past_map: Dict[str, List[float]] = {sym: [] for sym in etf_syms}

    for stock in stock_results:
        source_tokens = _split_source_etfs(getattr(stock, 'source_etf', ""))
        listing_tokens = _split_source_etfs(getattr(stock, 'listing_source', ""))
        memberships = set(source_tokens) | set(listing_tokens)

        if not memberships:
            continue
        for etf_sym in memberships:
            if etf_sym in etf_rsl_map:
                try:
                    etf_rsl_map[etf_sym].append(float(stock.rsl))
                except (TypeError, ValueError):
                    pass
            if etf_sym in etf_rsl_past_map:
                try:
                    rsl_past = getattr(stock, 'rsl_past', None)
                    if rsl_past is not None:
                        etf_rsl_past_map[etf_sym].append(float(rsl_past))
                except (TypeError, ValueError):
                    pass

    rows = []
    breadth_min = float(config.get('industry_breadth_min', 0.0) or 0.0)
    min_size = int(config.get('industry_min_size', 0) or 0)
    score_min = float(config.get('industry_score_min', 0.0) or 0.0)
    trend_enabled = bool(config.get('industry_trend_enabled', True))
    trend_weeks = int(config.get('industry_trend_weeks', 4) or 4)
    trend_score_label = f"Score Trend {trend_weeks}W"
    trend_past_label = f"Score (vor {trend_weeks}W)"
    
    for etf_sym in etf_syms:
        rsl_values = sorted(etf_rsl_map.get(etf_sym, []), reverse=True)
        rsl_past_values = etf_rsl_past_map.get(etf_sym, [])
        n_values = len(rsl_values)

        breadth = 0.0
        strong_breadth = 0.0
        leader_ratio = 0.0
        avg_rsl = 0.0
        median_rsl = 0.0
        top_rsl = 0.0
        count_strong = 0
        count_very_strong = 0
        leader_count = 0
        if n_values > 0:
            threshold_rank = max(1, int(n_values * top_percent_threshold))
            threshold_rsl = rsl_values[threshold_rank - 1]
            avg_rsl = float(np.mean(rsl_values))
            median_rsl = float(np.median(rsl_values))
            top_rsl = float(rsl_values[0])
            count_strong = sum(1 for v in rsl_values if v > 1.1)
            count_very_strong = sum(1 for v in rsl_values if v > 1.2)
            leader_count = sum(1 for v in rsl_values if v > 1.3)
            breadth = count_strong / n_values
            strong_breadth = count_very_strong / n_values
            leader_ratio = leader_count / n_values
            avg_rsl_out = f"{avg_rsl:.4f}".replace(".", ",")
            threshold_rsl_out = f"{threshold_rsl:.4f}".replace(".", ",")
        else:
            threshold_rank = 0
            avg_rsl_out = ""
            threshold_rsl_out = ""

        avg_rsl_adj = avg_rsl - 1.0
        avg_rsl_cap = float(config.get('industry_avg_rsl_cap', 0.6) or 0.6)
        avg_rsl_adj = min(avg_rsl_adj, avg_rsl_cap)
        median_rsl_adj = median_rsl - 1.0
        score = (
            float(config.get('industry_score_w_breadth', 0.4) or 0.4) * breadth
            + float(config.get('industry_score_w_avg', 0.2) or 0.2) * avg_rsl_adj
            + float(config.get('industry_score_w_median', 0.2) or 0.2) * median_rsl_adj
            + float(config.get('industry_score_w_leader', 0.2) or 0.2) * leader_ratio
        )

        row = {
                "ETF": etf_sym,
                "ETF Name (voll)": etf_options.get(etf_sym, {}).get("name", etf_sym),
                "ISIN": etf_options.get(etf_sym, {}).get("isin", ""),
                "Sub Asset Class": etf_options.get(etf_sym, {}).get("sub_asset_class", ""),
                "Region": etf_options.get(etf_sym, {}).get("region", ""),
                "Market": etf_options.get(etf_sym, {}).get("market", ""),
                "Location": etf_options.get(etf_sym, {}).get("location", ""),
                "Investment Style": etf_options.get(etf_sym, {}).get("investment_style", ""),
                "Anzahl Werte (analysiert)": n_values,
                "Breadth > 1.1": count_strong,
                "Breadth Ratio": breadth,
                "Strong Breadth > 1.2": count_very_strong,
                "Strong Breadth Ratio": strong_breadth,
                "Leader > 1.3": leader_count,
                "Leader Ratio": leader_ratio,
                "Top-% Schwelle": f"{int(top_percent_threshold * 100)}%",
                "Grenzrang Top-%": threshold_rank,
                "RSL am Grenzrang": threshold_rsl_out,
                "Durchschnitt RSL": avg_rsl_out,
                "Median RSL": f"{median_rsl:.4f}".replace(".", ",") if n_values > 0 else "",
                "Top RSL": f"{top_rsl:.4f}".replace(".", ",") if n_values > 0 else "",
                "Score": score,
        }

        # Trend-Logik (wie in build_industry_rsl_summary)
        if trend_enabled:
            past_vals = [v for v in rsl_past_values if v is not None and v > 0]
            if len(past_vals) > 0:
                past_count = len(past_vals)
                past_breadth = float(sum(1 for v in past_vals if v > 1.1)) / past_count
                past_leader_ratio = float(sum(1 for v in past_vals if v > 1.3)) / past_count
                past_avg = float(np.mean(past_vals))
                past_median = float(np.median(past_vals))
                past_avg_adj = min(past_avg - 1.0, avg_rsl_cap)
                past_median_adj = past_median - 1.0
                past_score = (
                    float(config.get('industry_score_w_breadth', 0.4) or 0.4) * past_breadth
                    + float(config.get('industry_score_w_avg', 0.2) or 0.2) * past_avg_adj
                    + float(config.get('industry_score_w_median', 0.2) or 0.2) * past_median_adj
                    + float(config.get('industry_score_w_leader', 0.2) or 0.2) * past_leader_ratio
                )
                row[trend_past_label] = past_score
                row[trend_score_label] = score - past_score
            else:
                row[trend_past_label] = None
                row[trend_score_label] = None

        rows.append(row)

    selected_set = set(etf_syms)
    combined_values = []
    for stock in stock_results:
        source_tokens = _split_source_etfs(getattr(stock, 'source_etf', ""))
        listing_tokens = _split_source_etfs(getattr(stock, 'listing_source', ""))
        memberships = set(source_tokens) | set(listing_tokens)

        if memberships & selected_set:
            try:
                combined_values.append(float(stock.rsl))
            except (TypeError, ValueError):
                pass

    combined_values = sorted(combined_values, reverse=True)
    combined_n = len(combined_values)
    if combined_n > 0:
        combined_rank = max(1, int(combined_n * top_percent_threshold))
        combined_threshold_rsl = combined_values[combined_rank - 1]
        combined_avg_rsl = float(np.mean(combined_values))
        rows.append(
            {
                "ETF": "GESAMT",
                "ETF Name (voll)": "Alle selektierten ETFs",
                "ISIN": "",
                "Sub Asset Class": "",
                "Region": "",
                "Market": "",
                "Location": "",
                "Investment Style": "",
                "Anzahl Werte (analysiert)": combined_n,
                "Breadth > 1.1": "",
                "Breadth Ratio": "",
                "Strong Breadth > 1.2": "",
                "Strong Breadth Ratio": "",
                "Leader > 1.3": "",
                "Leader Ratio": "",
                "Top-% Schwelle": f"{int(top_percent_threshold * 100)}%",
                "Grenzrang Top-%": combined_rank,
                "RSL am Grenzrang": f"{combined_threshold_rsl:.4f}".replace(".", ","),
                "Durchschnitt RSL": f"{combined_avg_rsl:.4f}".replace(".", ","),
                "Median RSL": "",
                "Top RSL": "",
                "Score": "",
            }
        )

    df_etf = pd.DataFrame(rows)
    if df_etf.empty:
        return df_etf

    avg_col = "Durchschnitt RSL"
    df_etf["_score_sort"] = pd.to_numeric(
        df_etf["Score"].astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )
    is_total = df_etf["ETF"].astype(str).str.upper().eq("GESAMT")
    df_sorted = df_etf.loc[~is_total].sort_values(
        by=["_score_sort", "ETF"], ascending=[False, True], na_position="last"
    )
    df_total = df_etf.loc[is_total]
    df_etf = pd.concat([df_sorted, df_total], ignore_index=True)
    df_etf = df_etf.drop(columns=["_score_sort"], errors="ignore")

    df_etf["Rank"] = ""
    rank_idx = df_etf.index[~is_total]
    df_etf.loc[rank_idx, "Rank"] = range(1, len(rank_idx) + 1)

    ordered_cols = [
        "Score",
        "Rank",
        "Durchschnitt RSL",
        "Median RSL",
        "Top RSL",
        "ETF",
        "ETF Name (voll)",
        "ISIN",
        "Sub Asset Class",
        "Region",
        "Market",
        "Location",
        "Investment Style",
        "Anzahl Werte (analysiert)",
        "Breadth > 1.1",
        "Breadth Ratio",
        "Strong Breadth > 1.2",
        "Strong Breadth Ratio",
        "Leader > 1.3",
        "Leader Ratio",
        "Top-% Schwelle",
        "Grenzrang Top-%",
        "RSL am Grenzrang",
        trend_past_label,
        trend_score_label,
    ]
    existing_cols = [c for c in ordered_cols if c in df_etf.columns]
    rest_cols = [c for c in df_etf.columns if c not in existing_cols]
    return df_etf[existing_cols + rest_cols]


def build_sector_rsl_summary(stock_results: List[Any], top_percent_threshold: float) -> pd.DataFrame:
    sector_rsl_map: Dict[str, List[float]] = defaultdict(list)

    for stock in stock_results:
        sector_name = str(stock.sector).strip() if stock.sector is not None else ""
        if not sector_name:
            sector_name = "Unbekannt"
        try:
            sector_rsl_map[sector_name].append(float(stock.rsl))
        except (TypeError, ValueError):
            pass

    rows = []
    for sector_name, values in sector_rsl_map.items():
        rsl_values = sorted(values, reverse=True)
        n_values = len(rsl_values)

        if n_values > 0:
            threshold_rank = max(1, int(n_values * top_percent_threshold))
            threshold_rsl = rsl_values[threshold_rank - 1]
            avg_rsl = float(np.mean(rsl_values))
            avg_rsl_out = f"{avg_rsl:.4f}".replace(".", ",")
            threshold_rsl_out = f"{threshold_rsl:.4f}".replace(".", ",")
        else:
            threshold_rank = 0
            avg_rsl_out = ""
            threshold_rsl_out = ""

        rows.append(
            {
                "Sektor": sector_name,
                "Anzahl Werte (analysiert)": n_values,
                "Top-% Schwelle": f"{int(top_percent_threshold * 100)}%",
                "Grenzrang Top-%": threshold_rank,
                "RSL am Grenzrang": threshold_rsl_out,
                "Durchschnitt RSL": avg_rsl_out,
            }
        )

    df_sector = pd.DataFrame(rows)

    combined_values = []
    for stock in stock_results:
        try:
            combined_values.append(float(stock.rsl))
        except (TypeError, ValueError):
            pass
    combined_values = sorted(combined_values, reverse=True)
    combined_n = len(combined_values)

    if combined_n > 0:
        combined_rank = max(1, int(combined_n * top_percent_threshold))
        combined_threshold_rsl = combined_values[combined_rank - 1]
        combined_avg_rsl = float(np.mean(combined_values))
        total_row = pd.DataFrame(
            [
                {
                    "Sektor": "GESAMT",
                    "Anzahl Werte (analysiert)": combined_n,
                    "Top-% Schwelle": f"{int(top_percent_threshold * 100)}%",
                    "Grenzrang Top-%": combined_rank,
                    "RSL am Grenzrang": f"{combined_threshold_rsl:.4f}".replace(".", ","),
                    "Durchschnitt RSL": f"{combined_avg_rsl:.4f}".replace(".", ","),
                }
            ]
        )
        df_sector = pd.concat([df_sector, total_row], ignore_index=True)

    if df_sector.empty:
        return df_sector

    avg_col = "Durchschnitt RSL"
    df_sector["_avg_sort"] = pd.to_numeric(
        df_sector[avg_col].astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )
    is_total = df_sector["Sektor"].astype(str).str.upper().eq("GESAMT")
    df_sorted = df_sector.loc[~is_total].sort_values(
        by=["_avg_sort", "Sektor"], ascending=[False, True], na_position="last"
    )
    df_total = df_sector.loc[is_total]
    df_sector = pd.concat([df_sorted, df_total], ignore_index=True)
    df_sector = df_sector.drop(columns=["_avg_sort"], errors="ignore")

    ordered_cols = [
        "Durchschnitt RSL",
        "Sektor",
        "Anzahl Werte (analysiert)",
        "Top-% Schwelle",
        "Grenzrang Top-%",
        "RSL am Grenzrang",
    ]
    existing_cols = [c for c in ordered_cols if c in df_sector.columns]
    rest_cols = [c for c in df_sector.columns if c not in existing_cols]
    return df_sector[existing_cols + rest_cols]


def build_industry_rsl_summary(
    stock_results: List[Any],
    config: Dict[str, Any]
) -> pd.DataFrame:
    """ Erstellt eine Zusammenfassung auf Branchenebene mit Breadth- und Leadership-Score. """
    data = [s.to_dict() for s in stock_results]
    df = pd.DataFrame(data)
   
    if df.empty:
        return pd.DataFrame()
    # Sicherstellen, dass RSL numerisch ist
    df['rsl'] = pd.to_numeric(df['rsl'], errors='coerce').fillna(0.0)
    df['rsl_past'] = pd.to_numeric(df.get('rsl_past'), errors='coerce')
    
    df = df[df['rsl'] > 0] 
    
    df['industry'] = (
        df['industry']
        .fillna('Unknown')
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .replace("", "Unknown")
    )
    df['sector'] = (
        df['sector']
        .fillna('Unknown')
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .replace("", "Unknown")
    )
    
    if not config.get('industry_summary_include_unknown', True):
        df = df[~df['industry'].isin(['Unknown', 'ETF'])]
        
    grouped = df.groupby('industry')
    summary_list = []
    
    top_percent_threshold = float(config.get('top_percent_threshold', 0.25))
    # Parameter laden
    trend_enabled = bool(config.get('industry_trend_enabled', True))
    trend_weeks = int(config.get('industry_trend_weeks', 4) or 4)
    trend_score_label = f"Score Trend {trend_weeks}W"
    trend_past_label = f"Score (vor {trend_weeks}W)"
    
    # Score-Gewichte
    w_breadth = float(config.get('industry_score_w_breadth', 0.4) or 0.4)
    w_avg = float(config.get('industry_score_w_avg', 0.2) or 0.2)
    w_median = float(config.get('industry_score_w_median', 0.2) or 0.2)
    w_leader = float(config.get('industry_score_w_leader', 0.2) or 0.2)
    avg_cap = float(config.get('industry_avg_rsl_cap', 0.6) or 0.6)

    for industry, group in grouped:
        rsl_values = sorted(group['rsl'].tolist(), reverse=True)
        count_total = len(rsl_values)

        threshold_rank = 0
        threshold_rsl_out = ""
        if count_total > 0:
            threshold_rank = max(1, int(count_total * top_percent_threshold))
            threshold_rsl = rsl_values[threshold_rank - 1]
            threshold_rsl_out = f"{threshold_rsl:.4f}".replace(".", ",")

        count_strong = int((group['rsl'] > 1.1).sum())
        breadth = count_strong / count_total
        count_very_strong = int((group['rsl'] > 1.2).sum())
        strong_breadth_ratio = count_very_strong / count_total
        leader_count = int((group['rsl'] > 1.3).sum())
        leader_ratio = leader_count / count_total
        top_rsl = group['rsl'].max()
        avg_rsl = group['rsl'].mean()
        median_rsl = group['rsl'].median()

        avg_rsl_adj = min(avg_rsl - 1.0, avg_cap)
        median_rsl_adj = median_rsl - 1.0
        
        score = (w_breadth * breadth + w_avg * avg_rsl_adj + w_median * median_rsl_adj + w_leader * leader_ratio)

        representative_sector = group['sector'].mode().iloc[0] if not group['sector'].mode().empty else 'Unbekannt'
        
        row = {
            'Branche': industry,
            'Sektor': representative_sector,
            'Aktien': count_total,
            'Breadth > 1.1': count_strong,
            'Breadth Ratio': breadth,
            'Strong Breadth > 1.2': count_very_strong,
            'Strong Breadth Ratio': strong_breadth_ratio,
            'Leader > 1.3': leader_count,
            'Leader Ratio': leader_ratio,
            'Avg RSL': avg_rsl,
            'Median RSL': median_rsl,
            'Top RSL': top_rsl,
            'Score': score,
            'Top-% Schwelle': f"{int(top_percent_threshold * 100)}%",
            'Grenzrang Top-%': threshold_rank,
            'RSL am Grenzrang': threshold_rsl_out,
        }

        if trend_enabled:
            past_vals = group['rsl_past'].dropna()
            past_vals = past_vals[past_vals > 0]
            if len(past_vals) > 0:
                past_count = len(past_vals)
                past_breadth = float((past_vals > 1.1).sum()) / past_count
                past_leader_ratio = float((past_vals > 1.3).sum()) / past_count
                past_avg = float(past_vals.mean())
                past_median = float(past_vals.median())
                past_avg_adj = min(past_avg - 1.0, avg_cap)
                past_median_adj = past_median - 1.0
                past_score = (w_breadth * past_breadth + w_avg * past_avg_adj + w_median * past_median_adj + w_leader * past_leader_ratio)
                row[trend_past_label] = past_score
                row[trend_score_label] = score - past_score
            else:
                row[trend_past_label] = None
                row[trend_score_label] = None
        summary_list.append(row)
       
    summary_df = pd.DataFrame(summary_list)
    if not summary_df.empty:
        summary_df = summary_df.sort_values(by='Score', ascending=False)
        summary_df['Rank'] = range(1, len(summary_df) + 1)
    return summary_df

def build_momentum_cluster_summary(results: List[Any], config: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """Berechnet Momentum-Cluster und deren Zusammenfassung."""
    if not results or not bool(config.get('cluster_enabled', True)):
        return pd.DataFrame(), {}

    def _bucket(value: Any, strong_threshold: float, neutral_threshold: float = 0.0) -> str:
        try:
            val = float(value) if value is not None else 0.0
        except: val = 0.0
        if val > strong_threshold: return "2"
        if val > neutral_threshold: return "1"
        return "0"

    # Gewichte extrahieren (Summe 1.0)
    w12 = float(config.get('cluster_score_w_mom12', 0.5))
    w6 = float(config.get('cluster_score_w_mom6', 0.3))
    wa = float(config.get('cluster_score_w_accel', 0.2))

    rows = []
    cluster_map: Dict[str, str] = {}
    total = max(1, len(results))
    for stock in results:
        cluster_id = "".join([
            _bucket(getattr(stock, 'mom_12m', None), 0.20),
            _bucket(getattr(stock, 'mom_6m', None), 0.10),
            _bucket(getattr(stock, 'mom_3m', None), 0.03),
            _bucket(getattr(stock, 'mom_accel', None), 0.03, -0.02),
        ])
        symbol = str(getattr(stock, 'yahoo_symbol', '') or '').strip().upper()
        if symbol:
            cluster_map[symbol] = cluster_id
        rows.append({
            'Cluster': cluster_id,
            'mom_12m': float(getattr(stock, 'mom_12m', 0.0) or 0.0),
            'mom_6m': float(getattr(stock, 'mom_6m', 0.0) or 0.0),
            'mom_accel': float(getattr(stock, 'mom_accel', 0.0) or 0.0),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(), cluster_map

    summary = (
        df.groupby('Cluster', dropna=False)
        .agg(
            Anzahl=('Cluster', 'size'),
            Avg_Mom_12M=('mom_12m', 'mean'),
            Avg_Mom_6M=('mom_6m', 'mean'),
            Avg_Accel=('mom_accel', 'mean'),
        )
        .reset_index()
    )
    summary['Cluster Anteil %'] = summary['Anzahl'] / float(total)
    summary['Score'] = (
        summary['Avg_Mom_12M'] * w12 + summary['Avg_Mom_6M'] * w6 + summary['Avg_Accel'] * wa
    )
    summary = summary.sort_values(['Score', 'Anzahl'], ascending=[False, False]).reset_index(drop=True)
    summary['Rank'] = range(1, len(summary) + 1)
    return summary, cluster_map
