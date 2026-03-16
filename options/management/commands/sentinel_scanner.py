import math
from django.core.management.base import BaseCommand
from options.models import OptionChainSnapshot, TradeSuggestion


class Command(BaseCommand):
    help = 'Institutional High-Density Matrix Scanner v0.3.7 - Full R:R Integration'

    def is_monthly_expiry(self, expiry_date):
        """CME ES Monthlys are the 3rd Friday of the month."""
        if expiry_date.weekday() != 4: return False
        return 15 <= expiry_date.day <= 21

    def handle(self, *args, **options):
        self.stdout.write("🔍 Initializing Multi-Width Surface Scan...")
        snapshot = OptionChainSnapshot.objects.order_by('-timestamp').first()
        if not snapshot:
            self.stdout.write(self.style.ERROR("❌ Database empty."))
            return

        self.run_scanner_logic(snapshot)

    def run_scanner_logic(self, snapshot):
        suggestions_to_save = []
        F = float(snapshot.underlying_price or 5120.0)
        widths = [5, 10, 20, 25, 50, 100]

        # Load and group contracts locally for O(1) strategy building
        raw_contracts = snapshot.contracts.all()
        matrix = {}
        for c in raw_contracts:
            if c.option_type not in ['P', 'C']: continue
            dte = c.dte
            if dte not in matrix:
                matrix[dte] = {'P': {}, 'C': {}, 'exp': c.expiration.date()}
            matrix[dte][c.option_type][float(c.strike)] = {
                'price': float(c.settlement),
                'delta': float(c.delta or 0.0)
            }

        dtes = sorted(matrix.keys())

        def get_strike_by_delta(dte, opt_type, target_delta):
            strikes = matrix[dte][opt_type]
            if not strikes: return None
            return min(strikes.keys(), key=lambda k: abs(abs(strikes[k]['delta']) - target_delta))

        def get_closest_strike(dte, opt_type, target_k):
            strikes = list(matrix[dte][opt_type].keys())
            if not strikes: return None
            return min(strikes, key=lambda x: abs(x - target_k))

        for idx, dte in enumerate(dtes):
            chain = matrix[dte]
            is_monthly = self.is_monthly_expiry(chain['exp'])

            for W in widths:
                # --- 1. VERTICAL SPREADS ---
                for opt_type in ['P', 'C']:
                    ks = get_strike_by_delta(dte, opt_type, 0.30)
                    if ks:
                        kl = ks - W if opt_type == 'P' else ks + W
                        if kl in chain[opt_type]:
                            credit = abs(chain[opt_type][ks]['price'] - chain[opt_type][kl]['price'])
                            if credit > 0:
                                m_prof = credit * 50
                                m_risk = (W - credit) * 50
                                raw_rr = round(m_prof / max(m_risk, 0.5), 2)
                                suggestions_to_save.append(TradeSuggestion(
                                    snapshot=snapshot, strategy_type="Vertical Spread", dte=dte,
                                    strikes=f"{ks}/{kl} {opt_type}", width=W, credit_debit=credit,
                                    max_profit=m_prof, max_loss=m_risk, rr_ratio=raw_rr,
                                    probability=70.0, edge=f"{W}pt {opt_type}", is_monthly=is_monthly
                                ))

                # --- 2. IRON CONDORS ---
                kp_s = get_strike_by_delta(dte, 'P', 0.16)
                kc_s = get_strike_by_delta(dte, 'C', 0.16)
                if kp_s and kc_s:
                    kp_l, kc_l = kp_s - W, kc_s + W
                    if kp_l in chain['P'] and kc_l in chain['C']:
                        ic_cr = (chain['P'][kp_s]['price'] - chain['P'][kp_l]['price']) + \
                                (chain['C'][kc_s]['price'] - chain['C'][kc_l]['price'])
                        if ic_cr > 0:
                            m_prof = ic_cr * 50
                            m_risk = (W - ic_cr) * 50
                            raw_rr = round(m_prof / max(m_risk, 0.5), 2)
                            suggestions_to_save.append(TradeSuggestion(
                                snapshot=snapshot, strategy_type="Iron Condor", dte=dte,
                                strikes=f"{kp_l}/{kp_s}P - {kc_s}/{kc_l}C", width=W,
                                credit_debit=ic_cr, max_profit=m_prof, max_loss=m_risk,
                                rr_ratio=raw_rr, probability=68.0, is_monthly=is_monthly
                            ))

                # --- 3. BUTTERFLY & BWB ---
                km = get_closest_strike(dte, 'P', F)
                ki, ko, kb = km + W, km - W, km - (W * 2)
                if all(k in chain['P'] for k in [ki, km, ko]):
                    fly_cost = chain['P'][ki]['price'] - (2 * chain['P'][km]['price']) + chain['P'][ko]['price']
                    if fly_cost > 0:
                        m_prof = (W - fly_cost) * 50
                        m_risk = fly_cost * 50
                        raw_rr = round(m_prof / max(m_risk, 0.5), 2)
                        suggestions_to_save.append(TradeSuggestion(
                            snapshot=snapshot, strategy_type="Butterfly", dte=dte,
                            strikes=f"{ki}/{km}x2/{ko} P", width=W, credit_debit=-fly_cost,
                            max_profit=m_prof, max_loss=m_risk, rr_ratio=raw_rr,
                            probability=25.0, is_monthly=is_monthly
                        ))
                    if kb in chain['P']:
                        bwb_c = chain['P'][ki]['price'] - (2 * chain['P'][km]['price']) + chain['P'][kb]['price']
                        if bwb_c > 0:
                            m_prof = W * 50
                            m_risk = bwb_c * 50
                            raw_rr = round(m_prof / max(m_risk, 0.5), 2)
                            suggestions_to_save.append(TradeSuggestion(
                                snapshot=snapshot, strategy_type="Broken Wing Butterfly", dte=dte,
                                strikes=f"{ki}/{km}x2/{kb} P", width=W, credit_debit=-bwb_c,
                                max_profit=m_prof, max_loss=m_risk, rr_ratio=raw_rr,
                                probability=65.0, is_monthly=is_monthly
                            ))

            # --- 4. RATIO SPREAD ---
            kl, ks = get_strike_by_delta(dte, 'P', 0.30), get_strike_by_delta(dte, 'P', 0.15)
            if kl and ks:
                rv = chain['P'][kl]['price'] - (2 * chain['P'][ks]['price'])
                suggestions_to_save.append(TradeSuggestion(
                    snapshot=snapshot, strategy_type="Ratio Spread", dte=dte,
                    strikes=f"1x {kl}P / -2x {ks}P", width=abs(kl - ks), credit_debit=rv,
                    max_profit=abs(kl - ks) * 50, max_loss=9999, rr_ratio=0.0,
                    probability=75.0, is_monthly=is_monthly
                ))

            # --- 5. STRADDLE ---
            ka = get_closest_strike(dte, 'C', F)
            sc = chain['C'][ka]['price'] + chain['P'][ka]['price']
            suggestions_to_save.append(TradeSuggestion(
                snapshot=snapshot, strategy_type="Straddle", dte=dte,
                strikes=f"{ka} ATM C+P", width=0, credit_debit=sc,
                max_profit=sc * 50, max_loss=99999, rr_ratio=0.0,
                probability=50.0, is_monthly=is_monthly
            ))

        TradeSuggestion.objects.filter(snapshot=snapshot).delete()
        TradeSuggestion.objects.bulk_create(suggestions_to_save)
        self.stdout.write(self.style.SUCCESS(f"🎯 Saved {len(suggestions_to_save)} trades with Float R:R integration."))