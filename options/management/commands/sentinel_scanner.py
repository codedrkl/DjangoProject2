import math
from django.core.management.base import BaseCommand
from django.utils import timezone
from options.models import OptionChainSnapshot, TradeSuggestion, OptionContract, IntradayOptionContract


class Command(BaseCommand):
    help = 'Regime Aware Matrix - Isolated Intraday Support'

    def add_arguments(self, parser):
        parser.add_argument('--intraday', action='store_true', help='Use live IntradayOptionContract table')

    def handle(self, *args, **options):
        if options['intraday']:
            try:
                # 1. Get the latest intraday pulse
                latest_ts = IntradayOptionContract.objects.latest('timestamp').timestamp
                contracts = IntradayOptionContract.objects.filter(timestamp=latest_ts)
                spot = float(contracts.first().underlying_price)

                # 2. Create a "Virtual Snapshot" so TradeSuggestions have a parent for the UI
                # We use a unique label for this specific intraday timestamp
                label = f"LIVE_{latest_ts.strftime('%H%M')}"
                snapshot, _ = OptionChainSnapshot.objects.update_or_create(
                    product="ES",
                    date=latest_ts.date(),
                    label=label,
                    defaults={
                        'underlying_price': spot,
                        'state_signature': 'INTRADAY_SQUEEZE_ACTIVE',
                        'timestamp': latest_ts
                    }
                )

                self.stdout.write(self.style.NOTICE(f"🔍 Scanning LIVE Intraday: {latest_ts} | F: {spot}"))
                self.run_scanner_logic(snapshot, contracts, spot)

            except IntradayOptionContract.DoesNotExist:
                self.stdout.write(self.style.ERROR("Intraday table empty. Run daemon first."))
        else:
            # Standard EOD Logic
            snapshot = OptionChainSnapshot.objects.filter(product="ES").order_by('-date', '-timestamp').first()
            if not snapshot:
                self.stdout.write(self.style.ERROR("Database empty."))
                return

            spot = float(snapshot.underlying_price)
            contracts = snapshot.contracts.all()
            self.run_scanner_logic(snapshot, contracts, spot)

    def is_monthly_expiry(self, expiry_date):
        if expiry_date.weekday() != 4: return False
        return 15 <= expiry_date.day <= 21

    def run_scanner_logic(self, snapshot, contracts, F):
        suggestions_to_save = []
        sig = snapshot.state_signature or "COMPRESSION_NEUTRAL"
        is_expansion = "EXPANSION" in sig or "SQUEEZE" in sig

        widths = [10, 25, 50, 100]

        # Build the Matrix from the QuerySet (Works for both EOD and Intraday lists)
        matrix = {}
        for c in contracts:
            if c.option_type not in ['P', 'C']: continue
            dte = c.dte
            if dte not in matrix:
                # Support both models: .expiration (EOD) vs .expiration (Intraday is date)
                exp_date = c.expiration if isinstance(c.expiration, date) else c.expiration.date()
                matrix[dte] = {'P': {}, 'C': {}, 'exp': exp_date}

            matrix[dte][c.option_type][float(c.strike)] = {
                'price': float(c.settlement),
                'delta': float(c.delta or 0.0)
            }

        dtes = sorted(matrix.keys())

        def get_strike_by_delta(dte, opt_type, target):
            strikes = matrix[dte][opt_type]
            if not strikes: return None
            return min(strikes.keys(), key=lambda k: abs(abs(strikes[k]['delta']) - target))

        for dte in dtes:
            chain = matrix[dte]
            is_monthly = self.is_monthly_expiry(chain['exp'])

            for W in widths:
                # 1. VERTICALS
                for opt_type in ['P', 'C']:
                    ks = get_strike_by_delta(dte, opt_type, 0.35)
                    if not ks: continue
                    kl = ks - W if opt_type == 'P' else ks + W

                    if kl in chain[opt_type]:
                        px_s, px_l = chain[opt_type][ks]['price'], chain[opt_type][kl]['price']
                        credit = abs(px_s - px_l)
                        if credit > 0:
                            m_prof, m_risk = credit * 50, (W - credit) * 50
                            suggestions_to_save.append(TradeSuggestion(
                                snapshot=snapshot, strategy_type="Vertical Spread", dte=dte,
                                strikes=f"{ks}/{kl} {opt_type}", width=W, credit_debit=credit,
                                max_profit=m_prof, max_loss=m_risk, rr_ratio=round(m_prof / max(m_risk, 1), 2),
                                probability=65.0, edge=f"Regime {sig}", is_monthly=is_monthly
                            ))

                # 2. IRON CONDORS
                if not is_expansion:
                    kp_s, kc_s = get_strike_by_delta(dte, 'P', 0.16), get_strike_by_delta(dte, 'C', 0.16)
                    if kp_s and kc_s:
                        kp_l, kc_l = kp_s - W, kc_s + W
                        if kp_l in chain['P'] and kc_l in chain['C']:
                            ic_cr = (chain['P'][kp_s]['price'] - chain['P'][kp_l]['price']) + \
                                    (chain['C'][kc_s]['price'] - chain['C'][kc_l]['price'])
                            if ic_cr > 0:
                                m_prof, m_risk = ic_cr * 50, (W - ic_cr) * 50
                                suggestions_to_save.append(TradeSuggestion(
                                    snapshot=snapshot, strategy_type="Iron Condor", dte=dte,
                                    strikes=f"{kp_l}/{kp_s}P - {kc_s}/{kc_l}C", width=W,
                                    credit_debit=ic_cr, max_profit=m_prof, max_loss=m_risk,
                                    rr_ratio=round(m_prof / max(m_risk, 1), 2), probability=72.0, is_monthly=is_monthly
                                ))

                # 3. RATIO SPREADS (Aggressive Squeeze Logic)
                if is_expansion:
                    kl, ks = get_strike_by_delta(dte, 'P', 0.30), get_strike_by_delta(dte, 'P', 0.15)
                    if kl and ks:
                        rv = chain['P'][kl]['price'] - (2 * chain['P'][ks]['price'])
                        suggestions_to_save.append(TradeSuggestion(
                            snapshot=snapshot, strategy_type="Expansion Ratio", dte=dte,
                            strikes=f"1x {kl}P / -2x {ks}P", width=abs(kl - ks), credit_debit=rv,
                            max_profit=abs(kl - ks) * 50, max_loss=9999, rr_ratio=0.0,
                            probability=78.0, is_monthly=is_monthly
                        ))

        # Atomic Clean/Save for this specific snapshot
        TradeSuggestion.objects.filter(snapshot=snapshot).delete()
        TradeSuggestion.objects.bulk_create(suggestions_to_save)
        self.stdout.write(self.style.SUCCESS(f"🎯 Saved {len(suggestions_to_save)} trades for {sig}."))