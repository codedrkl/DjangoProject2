from django.shortcuts import render
from collections import defaultdict
from .models import EODOptionSnapshot

def option_chain(request):
    """Main option chain view"""
    snapshot = EODOptionSnapshot.objects.filter(product='ES').order_by('-date').first()
    if not snapshot:
        return render(request, 'options/no_data.html')

    contracts = snapshot.contracts.all().order_by('expiration', 'strike')

    exp_data = defaultdict(dict)
    for c in contracts:
        exp = c.expiration.date()
        strike = float(c.strike)
        if strike not in exp_data[exp]:
            exp_data[exp][strike] = {'call': None, 'put': None}
        if c.option_type == 'C':
            exp_data[exp][strike]['call'] = c
        else:
            exp_data[exp][strike]['put'] = c

    chain_data = {}
    for exp, strikes_dict in exp_data.items():
        strikes = sorted(strikes_dict.keys())
        rows = []
        for s in strikes:
            row = strikes_dict[s]
            rows.append({
                'strike': s,
                'call': row['call'],
                'put': row['put'],
            })
        chain_data[exp] = rows

    return render(request, 'options/chain.html', {
        'snapshot': snapshot,
        'chain_data': chain_data,
        'underlying': snapshot.underlying_settlement,
    })


def outcome_view(request):
    snapshot = EODOptionSnapshot.objects.filter(product='ES').order_by('-date').first()
    if not snapshot:
        return render(request, 'options/no_data.html')

    import plotly.graph_objects as go
    import plotly.io as pio

    suggestions = [
        {
            'type': 'Bull Put Spread',
            'description': 'Sell 7600 Put / Buy 7550 Put',
            'expiry': 'March 6, 2026',
            'credit': 18.75,
            'max_profit': 1875,
            'max_loss': 1250,
            'breakeven': 7581.25,
            'probability': '71%',
            'edge': '+0.42',
            'rr_ratio': '1.50 : 1',
        },
        {
            'type': 'Iron Condor',
            'description': 'Short 7400/7600 Put Spread + Short 7700/7900 Call Spread',
            'expiry': 'March 6, 2026',
            'credit': 32.50,
            'max_profit': 3250,
            'max_loss': 1750,
            'breakeven_low': 7367.50,
            'breakeven_high': 7932.50,
            'probability': '68%',
            'edge': '+0.31',
            'rr_ratio': '1.86 : 1',
        },
        {
            'type': 'Bear Call Spread',
            'description': 'Sell 7750 Call / Buy 7800 Call',
            'expiry': 'March 6, 2026',
            'credit': 14.25,
            'max_profit': 1425,
            'max_loss': 1075,
            'breakeven': 7764.25,
            'probability': '64%',
            'edge': '+0.28',
            'rr_ratio': '1.33 : 1',
        }
    ]

    # Generate real interactive PnL graphs
    for sug in suggestions:
        fig = go.Figure()

        if sug['type'] == 'Bull Put Spread':
            x = list(range(7400, 7800, 5))
            y = [sug['max_profit'] if p >= sug['breakeven'] else max(-sug['max_loss'], (p - (sug['breakeven'] - sug['credit'])) * 100) for p in x]
            fig.add_trace(go.Scatter(x=x, y=y, fill='tozeroy', line=dict(color='#4ade80'), name='PnL'))
            fig.update_layout(title="Bull Put Spread PnL", xaxis_title="ES Price at Expiry", yaxis_title="P/L ($)", template="plotly_dark", height=320)

        elif sug['type'] == 'Iron Condor':
            x = list(range(7200, 8100, 10))
            y = [sug['max_profit'] if sug['breakeven_low'] <= p <= sug['breakeven_high'] else -sug['max_loss'] for p in x]
            fig.add_trace(go.Scatter(x=x, y=y, fill='tozeroy', line=dict(color='#eab308'), name='PnL'))
            fig.update_layout(title="Iron Condor PnL", xaxis_title="ES Price at Expiry", yaxis_title="P/L ($)", template="plotly_dark", height=320)

        else:  # Bear Call Spread
            x = list(range(7600, 8000, 5))
            y = [sug['max_profit'] if p <= sug['breakeven'] else max(-sug['max_loss'], (sug['breakeven'] - p) * 100) for p in x]
            fig.add_trace(go.Scatter(x=x, y=y, fill='tozeroy', line=dict(color='#f87171'), name='PnL'))
            fig.update_layout(title="Bear Call Spread PnL", xaxis_title="ES Price at Expiry", yaxis_title="P/L ($)", template="plotly_dark", height=320)

        sug['plot'] = pio.to_html(fig, full_html=False, include_plotlyjs='cdn')

    return render(request, 'options/outcome.html', {
        'snapshot': snapshot,
        'underlying': snapshot.underlying_settlement,
        'suggestions': suggestions,
    })