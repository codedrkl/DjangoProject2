import json
import plotly
import plotly.graph_objects as go
from collections import defaultdict
from django.shortcuts import render
from django.db.models import Case, When, Value, IntegerField
from .models import OptionChainSnapshot, TradeSuggestion


def option_chain(request):
    snapshot = OptionChainSnapshot.objects.order_by('-timestamp').first()
    if not snapshot:
        return render(request, 'options/chain.html', {'chain_data': {}, 'underlying': 0.0})

    underlying = float(snapshot.underlying_price or 0.0)
    raw_matrix = defaultdict(lambda: defaultdict(lambda: {'dte': 0, 'call': {}, 'put': {}}))
    contracts = snapshot.contracts.all()

    for c in contracts:
        exp = c.expiration.date()
        strike = float(c.strike)
        raw_matrix[exp][strike]['dte'] = c.dte
        payload = {'settlement': float(c.settlement), 'delta': float(c.delta), 'open_interest': int(c.open_interest)}
        if c.option_type == 'C':
            raw_matrix[exp][strike]['call'] = payload
        elif c.option_type == 'P':
            raw_matrix[exp][strike]['put'] = payload

    chain_data = {}
    for exp in sorted(raw_matrix.keys()):
        strikes_list = []
        for strike in sorted(raw_matrix[exp].keys()):
            row = raw_matrix[exp][strike]
            strikes_list.append({'strike': strike, 'dte': row['dte'], 'call': row['call'], 'put': row['put']})
        chain_data[exp] = strikes_list

    return render(request, 'options/chain.html',
                  {'snapshot': snapshot, 'underlying': underlying, 'chain_data': chain_data})


def outcome_view(request):
    snapshot = OptionChainSnapshot.objects.order_by('-timestamp').first()
    if not snapshot:
        return render(request, 'options/outcome.html', {'suggestions': [], 'snapshot': None})

    underlying = float(snapshot.underlying_price)
    suggestions = snapshot.suggestions.all().order_by('-max_profit')

    # Grid: ±12% move for Ratio Spread "Cliffs" and Tail Risk
    x_axis = [underlying * (0.88 + i / 833) for i in range(200)]
    enhanced_suggestions = []

    for sug in suggestions:
        y_axis = []
        prem = float(sug.credit_debit)
        strat = sug.strategy_type
        strikes = sug.strikes

        for x in x_axis:
            payoff = 0
            try:
                if "Ratio" in strat:
                    # Logic for "1x 6520.0P / -2x 6380.0P"
                    parts = strikes.split(' / ')
                    k_long = float(parts[0].split()[1].replace('P', '').replace('C', ''))
                    k_short = float(parts[1].split()[1].replace('P', '').replace('C', ''))
                    if 'P' in strikes:
                        payoff = prem + (max(k_long - x, 0) - 2 * max(k_short - x, 0))
                    else:
                        payoff = prem + (max(x - k_long, 0) - 2 * max(x - k_short, 0))

                elif "Butterfly" in strat:
                    # Logic for "6675.0/6625.0x2/6525.0 P"
                    pts = strikes.split()[0].replace('x2', '').split('/')
                    ki, km, ko = map(float, pts)
                    # Standard Butterfly Tent
                    payoff = prem + (max(ki - x, 0) - 2 * max(km - x, 0) + max(ko - x, 0))

                elif "Straddle" in strat:
                    ka = float(strikes.split()[0])
                    payoff = prem - abs(x - ka)

                elif "Vertical" in strat:
                    ks, kl = map(float, strikes.split()[0].split('/'))
                    if 'P' in strikes:
                        payoff = prem - (max(ks - x, 0) - max(kl - x, 0))
                    else:
                        payoff = prem - (max(x - ks, 0) - max(x - kl, 0))

                y_axis.append(payoff * 50)  # ES Multiplier
            except:
                y_axis.append(0)

        # High-visibility Emerald line for profits, Amber for neutral/cost
        line_color = '#10b981' if y_axis[100] > 0 else '#fbbf24'

        fig = go.Figure(data=go.Scatter(
            x=x_axis, y=y_axis, fill='tozeroy',
            fillcolor='rgba(16, 185, 129, 0.05)',
            line=dict(color=line_color, width=2)
        ))

        fig.update_layout(
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            margin=dict(l=0, r=0, t=10, b=0), height=200,
            xaxis=dict(visible=False),
            yaxis=dict(visible=False, zeroline=True, zerolinecolor='#ffffff', zerolinewidth=1),
            showlegend=False
        )

        enhanced_suggestions.append({
            'data': sug,
            'graph': json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        })

    return render(request, 'options/outcome.html', {'snapshot': snapshot, 'suggestions': enhanced_suggestions})


def footprint_view(request):
    snapshot = OptionChainSnapshot.objects.filter(bins__isnull=False).distinct().order_by('-timestamp').first()
    if not snapshot:
        return render(request, 'options/footprint.html', {'snapshot': None, 'bins': []})

    bins = snapshot.bins.select_related('ref_snapshot').annotate(
        sort_order=Case(When(bin_type='WEEKLY', then=Value(1)), When(bin_type='MONTHLY', then=Value(2)),
                        When(bin_type='QUARTERLY', then=Value(3)), output_field=IntegerField())
    ).order_by('sort_order', 'zone')

    return render(request, 'options/footprint.html', {'snapshot': snapshot, 'bins': bins})


def pnl_test_view(request):
    """
    Isolated PnL Test: Renders a single trade to verify Plotly/CDN/Base integrity.
    """
    from .models import TradeSuggestion
    import json
    import plotly.graph_objects as go

    # 1. Grab 1 suggestion
    sug = TradeSuggestion.objects.first()
    if not sug:
        return render(request, 'options/test_pnl.html', {'error': 'No trade suggestions found in DB.'})

    underlying = 6636.0  # Placeholder or fetch from snapshot

    # 2. Simple manual curve (V-shape for Straddle or Tent for Fly)
    x_axis = [underlying * (0.95 + i / 1000) for i in range(100)]
    y_axis = [abs(x - underlying) * 50 - 500 for x in x_axis]  # Generic V-shape

    fig = go.Figure(data=go.Scatter(
        x=x_axis, y=y_axis, fill='tozeroy',
        fillcolor='rgba(251, 191, 36, 0.2)',
        line=dict(color='#fbbf24', width=3)
    ))

    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        height=400,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis=dict(gridcolor='#222', zerolinecolor='#444'),
        yaxis=dict(gridcolor='#222', zerolinecolor='#444')
    )

    graph_json = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    return render(request, 'options/test_pnl.html', {
        'sug': sug,
        'graph_json': graph_json
    })