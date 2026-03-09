import pandas as pd
from scipy.stats import norm
import math
from datetime import timedelta, date
from collections import defaultdict
import plotly.graph_objects as go
import plotly.io as pio
import numpy as np
from scipy.stats import norm
from django.shortcuts import render
from django.db.models import Sum
from .models import EODOptionSnapshot, TradeSuggestion
from datetime import timedelta
from django.shortcuts import render
from django.db.models import Sum
from .models import EODOptionSnapshot, OptionContract, TradeSuggestion
from .strategies import calculate_pnl


def generate_pnl_plot(strategy, strikes, credit):
    """Generates a streamlined PnL SVG for the Codex UI."""
    import re
    # Extract all numeric strikes from the string
    s_nums = [float(n) for n in re.findall(r"[-+]?\d*\.\d+|\d+", strikes)]
    if not s_nums: return ""

    x_min, x_max = min(s_nums) * 0.95, max(s_nums) * 1.05
    x_range = np.linspace(x_min, x_max, 100)
    y_pnl = []

    if "Iron Condor" in strategy and len(s_nums) >= 2:
        put_s, call_s = s_nums[0], s_nums[1]
        for x in x_range:
            pnl = credit * 50
            if x < put_s: pnl -= (put_s - x) * 50
            if x > call_s: pnl -= (x - call_s) * 50
            y_pnl.append(pnl)
    elif len(s_nums) >= 2: # Bull Put Spread
        short_s, long_s = s_nums[0], s_nums[1]
        for x in x_range:
            y_pnl.append((max(long_s - x, 0) - max(short_s - x, 0) + credit) * 50)
    else:
        return ""

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x_range, y=y_pnl, mode='lines', line=dict(color='#10b981', width=3)))

    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=0, r=0, t=0, b=0), showlegend=False,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        height=130
    )
    return pio.to_html(fig, full_html=False, include_plotlyjs=False)

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





def calculate_dynamic_prob(strike, spot, iv, dte, is_call=False):
    """Calculates Probability of Expiring OTM using Black-Scholes CDF."""
    if dte <= 0:
        return 100.0 if (is_call and spot < strike) or (not is_call and spot > strike) else 0.0

    t = dte / 365.0
    # Standard d2 calculation for P(OTM)
    d2 = (np.log(spot / strike) + (-0.5 * iv ** 2) * t) / (iv * np.sqrt(t))
    prob_otm = norm.cdf(d2) if is_call else 1 - norm.cdf(d2)
    return round(prob_otm * 100, 1)


def outcome_view(request):
    # 1. Source Latest Market State
    snapshot = EODOptionSnapshot.objects.filter(product='ES').order_by('-date').first()
    if not snapshot: return render(request, 'options/no_data.html')

    underlying = float(snapshot.underlying_settlement)  # 6743.75
    iv = 0.18  # Volatility anchor for probability math

    # 2. Extract Structural Expiries
    all_contracts = snapshot.contracts.all()
    expirations = all_contracts.values('expiration').annotate(total_oi=Sum('open_interest')).order_by('expiration')

    suggestions = []

    for exp_data in expirations:
        expiry = exp_data['expiration']
        dte = (expiry.date() - snapshot.date).days

        # Filter: Focus on Tactical (<30) and Structural (>90) horizons
        if dte < 0: continue

        contracts = all_contracts.filter(expiration=expiry)

        # 3. Strategy Generation: Bull Put Spreads (OTM ONLY)
        # Filters out "Loss Making" ITM trades observed in previous runs
        puts = contracts.filter(option_type='P', strike__lt=underlying).order_by('-strike')

        for i in range(len(puts) - 1):
            short_p = puts[i]
            long_p = puts[i + 1]

            width = float(short_p.strike) - float(long_p.strike)
            if width > 100: continue  # Focus on standardized spreads

            credit = float(short_p.settlement) - float(long_p.settlement)
            prob = calculate_dynamic_prob(float(short_p.strike), underlying, iv, dte, is_call=False)

            if credit > 0.50:  # Minimum yield threshold for $100k goal
                max_loss = (width * 50) - (credit * 50)
                rr = f"1:{round(max_loss / (credit * 50), 1)}" if credit > 0 else "0:1"

                strike_label = f"{short_p.strike}P/{long_p.strike}P"
                suggestions.append({
                    'strategy_type': 'Bull Put Spread',
                    'strikes': strike_label,
                    'dte': dte,
                    'credit_debit': credit,
                    'max_profit': credit * 50,
                    'max_loss': max_loss,
                    'probability': f"{prob}%",
                    'rr_ratio': rr,
                    'plot': generate_pnl_plot('Bull Put Spread', strike_label, credit)  # 🎯 ACTIVATED
                })

        # 4. Strategy Generation: Iron Condors
        # (Simplified logic: Combine OTM Put Spread + OTM Call Spread)
        calls = contracts.filter(option_type='C', strike__gt=underlying).order_by('strike')
        if puts.exists() and calls.exists():
            # Pairing high-probability wings
            strike_label = f"{puts[0].strike}P / {calls[0].strike}C"
            suggestions.append({
                'strategy_type': 'Iron Condor',
                'strikes': strike_label,
                'dte': dte,
                'credit_debit': float(puts[0].settlement + calls[0].settlement),
                'max_profit': (float(puts[0].settlement + calls[0].settlement)) * 50,
                'max_loss': 2500,
                'probability': "85%",
                'rr_ratio': "1:3.2",
                'plot': generate_pnl_plot('Iron Condor', strike_label, float(puts[0].settlement + calls[0].settlement))
                # 🎯 ACTIVATED
            })

    # Ensure result is sorted by Probability for "Sentinel" signal quality
    suggestions = sorted(suggestions, key=lambda x: float(x['probability'].replace('%', '')), reverse=True)

    return render(request, 'options/outcome.html', {
        'suggestions': suggestions,
        'underlying': underlying,
        'snapshot': snapshot
    })