from collections import defaultdict
from django.shortcuts import render
from .models import OptionChainSnapshot, TradeSuggestion
from django.db.models import Case, When, Value, IntegerField

def option_chain(request):
    """
    Pivots flat PostgreSQL option records into a nested TOS-style matrix.
    Complexity: O(N) where N is number of contracts in the active snapshot.
    """
    snapshot = OptionChainSnapshot.objects.order_by('-timestamp').first()

    if not snapshot:
        return render(request, 'options/chain.html', {'chain_data': {}, 'underlying': 0.0})

    underlying = float(snapshot.underlying_price or 0.0)

    # Hash Map Allocation for O(1) lookups during pivot
    raw_matrix = defaultdict(lambda: defaultdict(lambda: {'dte': 0, 'call': {}, 'put': {}}))

    contracts = snapshot.contracts.all()
    for c in contracts:
        exp = c.expiration.date()
        strike = float(c.strike)

        raw_matrix[exp][strike]['dte'] = c.dte

        payload = {
            'settlement': float(c.settlement),
            'delta': float(c.delta),
            'open_interest': int(c.open_interest)
        }

        if c.option_type == 'C':
            raw_matrix[exp][strike]['call'] = payload
        elif c.option_type == 'P':
            raw_matrix[exp][strike]['put'] = payload

    # Serialization to Sorted Array for Template Rendering
    chain_data = {}
    for exp in sorted(raw_matrix.keys()):
        strikes_list = []
        for strike in sorted(raw_matrix[exp].keys()):
            row = raw_matrix[exp][strike]
            strikes_list.append({
                'strike': strike,
                'dte': row['dte'],
                'call': row['call'],
                'put': row['put']
            })
        chain_data[exp] = strikes_list

    context = {
        'snapshot': snapshot,
        'underlying': underlying,
        'chain_data': chain_data,
    }

    return render(request, 'options/chain.html', context)


def outcome_view(request):
    """
    Sentinel Scanner Dashboard
    Extracts the highest probability volatility surface edges.
    """
    snapshot = OptionChainSnapshot.objects.order_by('-timestamp').first()

    if not snapshot:
        return render(request, 'options/outcome.html', {'suggestions': [], 'snapshot': None})

    suggestions = snapshot.suggestions.all().order_by('-max_profit', 'credit_debit')

    context = {
        'snapshot': snapshot,
        'suggestions': suggestions,
    }

    return render(request, 'options/outcome.html', context)


def footprint_view(request):
    """
    Renders the temporal institutional footprint bins.
    Corrected: select_related moved to the Bin level.
    """
    # 1. Get the latest snapshot that has bins
    snapshot = OptionChainSnapshot.objects.filter(bins__isnull=False)\
        .distinct()\
        .order_by('-timestamp')\
        .first()

    if not snapshot:
        return render(request, 'options/footprint.html', {'snapshot': None, 'bins': []})

    # 2. Optimization: Fetch bins AND their reference snapshots in one hit
    bins = snapshot.bins.select_related('ref_snapshot').annotate(
        sort_order=Case(
            When(bin_type='WEEKLY', then=Value(1)),
            When(bin_type='MONTHLY', then=Value(2)),
            When(bin_type='QUARTERLY', then=Value(3)),
            output_field=IntegerField(),
        )
    ).order_by('sort_order', 'zone')

    return render(request, 'options/footprint.html', {
        'snapshot': snapshot,
        'bins': bins,
    })