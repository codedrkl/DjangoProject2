import numpy as np

def calculate_pnl(stype, x, p, credit, multiplier=50):
    """Broadcasting PnL engine using numpy.where."""
    x = np.array(x)
    if stype == 'Straddle':
        return (credit - np.abs(p['strike'] - x)) * multiplier
    if stype == 'Bull Put Spread':
        w = p['short_k'] - p['long_k']
        max_l = w - credit
        y = np.where(x >= p['short_k'], credit,
               np.where(x <= p['long_k'], -max_l, (x - p['long_k']) - max_l))
        return y * multiplier
    return np.zeros_like(x)

def apply_metrics(sug, bankroll=100000):
    """Expected Value and Kelly sizing logic."""
    p = float(str(sug.get('probability', '50')).replace('%', '')) / 100
    w, l = float(sug['max_profit']), float(sug['max_loss'])
    ev = (p * w) - ((1 - p) * l)
    sug['ev'] = round(ev, 2)
    if l > 0 and ev > 0:
        b = w / l
        f = ((b * p - (1 - p)) / b) * 0.5
        sug['kelly_pct'] = round(f * 100, 2)
        sug['contracts'] = int((bankroll * f) / l)
    else:
        sug['kelly_pct'], sug['contracts'] = 0, 0
    return sug