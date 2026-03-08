import pandas as pd
import numpy as np
import math
from datetime import timedelta, date
from collections import defaultdict
import plotly.graph_objects as go
import plotly.io as pio

from django.shortcuts import render
from django.db.models import Sum
from .models import EODOptionSnapshot, OptionContract, TradeSuggestion
from .strategies import calculate_pnl


# ====================== UTILS ======================
def get_moneyness_zone(strike, underlying, iv, dte):
    iv_clean = float(iv) if (iv and not math.isnan(iv) and iv > 0) else 0.20
    dte_clean = max(dte, 1)
    sd_1 = underlying * iv_clean * math.sqrt(dte_clean / 365.25)
    distance = abs(float(strike) - underlying)
    if distance <= (0.5 * sd_1): return "01_ATM"
    if distance >= (2.0 * sd_1): return "03_DEEP_OTM"
    return "02_OTM"


def calculate_expiry_footprint(snapshot, anchor_date, target_expiry):
    anchor_snap = EODOptionSnapshot.objects.filter(product=snapshot.product, date=anchor_date).first()
    if not anchor_snap: return pd.DataFrame()

    curr_qs = snapshot.contracts.filter(expiration__date=target_expiry).values('strike', 'option_type', 'settlement',
                                                                               'open_interest')
    prev_qs = anchor_snap.contracts.filter(expiration__date=target_expiry).values('strike', 'option_type', 'settlement',
                                                                                  'open_interest')

    curr_df, prev_df = pd.DataFrame(list(curr_qs)), pd.DataFrame(list(prev_qs))
    if curr_df.empty or prev_df.empty: return pd.DataFrame()

    for df in [curr_df, prev_df]:
        df['strike'] = df['strike'].astype(float)
        df['settlement'] = df['settlement'].astype(float)
        df['open_interest'] = df['open_interest'].astype(float)
        df['notional'] = df['strike'] * df['settlement'] * df['open_interest'] * 50

    merged = pd.merge(curr_df, prev_df, on=['strike', 'option_type'], suffixes=('_curr', '_prev'))
    merged['notional_change'] = merged['notional_curr'] - merged['notional_prev']
    return merged


# ====================== VIEWS ======================

def footprint_view(request):
    snapshot = EODOptionSnapshot.objects.filter(product='ES').order_by('-date').first()
    if not snapshot: return render(request, 'options/no_data.html')

    underlying = float(snapshot.underlying_settlement)

    anchor_date = snapshot.date - timedelta(days=1)
    if anchor_date.weekday() == 6: anchor_date -= timedelta(days=2)
    anchor_snap = EODOptionSnapshot.objects.filter(product='ES', date=anchor_date).first()

    all_contracts = snapshot.contracts.all()
    expiry_oi = all_contracts.values('expiration').annotate(total_oi=Sum('open_interest')).order_by('-total_oi')
    if not expiry_oi.exists(): return render(request, 'options/no_data.html')

    df_exp = pd.DataFrame(list(expiry_oi))
    df_exp['expiration'] = pd.to_datetime(df_exp['expiration']).dt.date
    df_exp['dte'] = (df_exp['expiration'] - snapshot.date).apply(lambda x: x.days)

    # 🎯 Mutually Exclusive Time Horizons
    # Weekly: Tactical flow (< 30 DTE)
    front_df = df_exp[df_exp['dte'] < 30].sort_values('total_oi', ascending=False)
    front_target = front_df.iloc[0]['expiration'] if not front_df.empty else df_exp.iloc[0]['expiration']

    # Monthly: Intermediate structure (30 to 89 DTE)
    mid_df = df_exp[(df_exp['dte'] >= 30) & (df_exp['dte'] < 90)].sort_values('total_oi', ascending=False)
    structural_target = mid_df.iloc[0]['expiration'] if not mid_df.empty else front_target

    # Quarterly: Explicit macro floor (June 2026)
    june_q_date = next((d for d in df_exp['expiration'] if d.month == 6 and d.year == 2026), None)

    targets = [('weekly', front_target), ('monthly', structural_target), ('quarterly', june_q_date)]
    pillars = {}

    for key, target_expiry in targets:
        if not target_expiry: continue

        qs_curr = all_contracts.filter(expiration__date=target_expiry).values('strike', 'option_type', 'open_interest',
                                                                              'settlement')
        df = pd.DataFrame(list(qs_curr))
        if df.empty: continue

        # Strikes are already normalized by Databento in DB
        df['strike'] = df['strike'].astype(float)
        df['notional_curr'] = df['strike'] * df['settlement'].astype(float) * df['open_interest'].astype(float) * 50

        if anchor_snap:
            qs_prev = anchor_snap.contracts.filter(expiration__date=target_expiry).values('strike', 'option_type',
                                                                                          'open_interest', 'settlement')
            df_prev = pd.DataFrame(list(qs_prev))
            if not df_prev.empty:
                df_prev['strike'] = df_prev['strike'].astype(float)
                df_prev['notional_prev'] = df_prev['strike'] * df_prev['settlement'].astype(float) * df_prev[
                    'open_interest'].astype(float) * 50

                df = pd.merge(df, df_prev[['strike', 'option_type', 'notional_prev']], on=['strike', 'option_type'],
                              how='left').fillna(0)
                df['notional_change'] = df['notional_curr'] - df['notional_prev']
                df['growth'] = df.apply(
                    lambda x: ((x['notional_curr'] - x['notional_prev']) / abs(x['notional_prev']) * 100) if x[
                                                                                                                 'notional_prev'] != 0 else 0,
                    axis=1)
            else:
                df['notional_change'] = df['notional_curr']
                df['growth'] = 0
        else:
            df['notional_change'] = df['notional_curr']
            df['growth'] = 0

        current_dte = (target_expiry - snapshot.date).days
        if key == 'weekly':
            df['bin'] = ((df['strike'] // 25) * 25).astype(int).astype(str) + "S"
        else:
            df['bin'] = df.apply(lambda r: get_moneyness_zone(r['strike'], underlying, 0.20, current_dte), axis=1)

        summary = df.groupby(['bin', 'option_type']).agg({
            'notional_change': 'sum',
            'growth': 'mean'
        }).reset_index()

        summary['notional_m'] = summary['notional_change'] / 1_000_000

        # Isolate true institutional flow via Absolute Magnitude sorting
        summary['abs_notional'] = summary['notional_m'].abs()
        summary = summary.sort_values('abs_notional', ascending=False).drop(columns=['abs_notional']).head(25)

        pillars[key] = summary.rename(columns={'option_type': 'type'}).to_dict('records')

    return render(request, 'options/footprint.html',
                  {'pillars': pillars, 'underlying': underlying, 'snapshot': snapshot})
def option_chain(request):
    snapshot = EODOptionSnapshot.objects.filter(product='ES').order_by('-date').first()
    if not snapshot: return render(request, 'options/no_data.html')

    limit_date = snapshot.date + timedelta(days=120)
    qs = snapshot.contracts.filter(expiration__date__lte=limit_date).values('strike', 'option_type', 'settlement',
                                                                            'delta', 'expiration', 'open_interest')

    exp_data = defaultdict(dict)
    for c in qs:
        exp = c['expiration'].date() if hasattr(c['expiration'], 'date') else c['expiration']
        strike = float(c['strike'])
        if strike not in exp_data[exp]: exp_data[exp][strike] = {'call': None, 'put': None}
        if c['option_type'] == 'C':
            exp_data[exp][strike]['call'] = c
        else:
            exp_data[exp][strike]['put'] = c

    chain_data = {exp: [{'strike': s, 'call': exp_data[exp][s]['call'], 'put': exp_data[exp][s]['put']} for s in
                        sorted(exp_data[exp].keys())] for exp in sorted(exp_data.keys())}
    return render(request, 'options/chain.html',
                  {'snapshot': snapshot, 'chain_data': chain_data, 'underlying': snapshot.underlying_settlement})


def outcome_view(request):
    snapshot = EODOptionSnapshot.objects.filter(product='ES').order_by('-date').first()
    if not snapshot:
        return render(request, 'options/no_data.html')

    suggestions = list(TradeSuggestion.objects.filter(snapshot=snapshot).values())

    for sug in suggestions:
        strikes = sug['strikes'].replace('P', '').split('/')
        short_k, long_k = float(strikes[0]), float(strikes[1])

        x_range = np.linspace(short_k - 150, short_k + 150, 100)

        y_vals = calculate_pnl(
            sug['strategy_type'],
            x_range,
            {'short_k': short_k, 'long_k': long_k},
            float(sug['credit_debit'])
        )

        match y_vals:
            case _ if len(y_vals) > 0:
                fig = go.Figure(go.Scatter(
                    x=x_range,
                    y=y_vals,
                    fill='tozeroy',
                    fillcolor='rgba(74, 222, 128, 0.15)',
                    line=dict(color='#4ade80', width=3)
                ))
                fig.update_layout(
                    template="plotly_dark",
                    height=130,
                    margin=dict(l=0, r=0, t=0, b=0),
                    xaxis_visible=True,
                    yaxis_visible=False,
                    paper_bgcolor='rgba(0,0,0,0)',  # Transparent to map to card bg
                    plot_bgcolor='rgba(0,0,0,0)'
                )
                sug['plot'] = pio.to_html(fig, full_html=False, include_plotlyjs='cdn')
            case _:
                sug['plot'] = None

    valid_plots = sum(1 for s in suggestions if s.get('plot'))
    print(f"🧬 OUTCOME PROBE: {len(suggestions)} suggestions extracted. {valid_plots} valid Plotly divs generated.")

    return render(request, 'options/outcome.html',
                  {'suggestions': suggestions, 'underlying': float(snapshot.underlying_settlement)})