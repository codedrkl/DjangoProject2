import math
from django.core.management.base import BaseCommand
from options.models import OptionChainSnapshot, TradeSuggestion


class Command(BaseCommand):
    help = 'Institutional High-Density Matrix Scanner - All Strategies'

    def is_monthly_expiry(self, expiry_date):
        """CME ES Monthlys are the 3rd Friday of the month."""
        if expiry_date.weekday() != 4: return False
        return 15 <= expiry_date.day <= 21

    def handle(self, *args, **options):
        self.stdout.write("🔍 Initializing Full Surface Matrix Scan...")
        target_snapshot = OptionChainSnapshot.objects.order_by('-timestamp').first()
        if not target_snapshot:
            self.stdout.write(self.style.ERROR("❌ Database empty."))
            return
        self.run_scanner_logic(target_snapshot)

    def run_scanner_logic(self, snapshot):
        suggestions_to_save = []
        F = float(snapshot.underlying_price or 5120.0)

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
            W = 50.0
            chain = matrix[dte]
            is_monthly = self.is_monthly_expiry(chain['exp'])

            # --- 1. VERTICAL SPREADS (30Δ) ---
            for opt_type in ['P', 'C']:
                ks = get_strike_by_delta(dte, opt_type, 0.30)
                kl = ks - W if opt_type == 'P' else ks + W
                if ks and kl in chain[opt_type]:
                    credit = abs(chain[opt_type][ks]['price'] - chain[opt_type][kl]['price'])
                    if credit > 0:
                        suggestions_to_save.append(TradeSuggestion(
                            snapshot=snapshot, strategy_type="Vertical Spread", dte=dte,
                            strikes=f"{ks}/{kl} {opt_type}", credit_debit=credit,
                            max_profit=credit * 50, max_loss=(W - credit) * 50,
                            probability="70%", edge="30Δ Target", is_monthly=is_monthly
                        ))

            # --- 2. IRON CONDORS (16Δ) ---
            kp_s = get_strike_by_delta(dte, 'P', 0.16)
            kc_s = get_strike_by_delta(dte, 'C', 0.16)
            if kp_s and kc_s:
                kp_l, kc_l = kp_s - W, kc_s + W
                if kp_l in chain['P'] and kc_l in chain['C']:
                    ic_cr = (chain['P'][kp_s]['price'] - chain['P'][kp_l]['price']) + \
                            (chain['C'][kc_s]['price'] - chain['C'][kc_l]['price'])
                    if ic_cr > 0:
                        suggestions_to_save.append(TradeSuggestion(
                            snapshot=snapshot, strategy_type="Iron Condor", dte=dte,
                            strikes=f"{kp_l}/{kp_s}P - {kc_s}/{kc_l}C",
                            credit_debit=ic_cr, max_profit=ic_cr * 50, max_loss=(W - ic_cr) * 50,
                            probability="68%", edge="1SD Wing", is_monthly=is_monthly
                        ))

            # --- 3. BUTTERFLY & BWB (ATM) ---
            km = get_closest_strike(dte, 'P', F)
            ki, ko, kb = km + W, km - W, km - (W * 2)
            if all(k in chain['P'] for k in [ki, km, ko]):
                fly_cost = chain['P'][ki]['price'] - (2 * chain['P'][km]['price']) + chain['P'][ko]['price']
                suggestions_to_save.append(TradeSuggestion(
                    snapshot=snapshot, strategy_type="Butterfly", dte=dte,
                    strikes=f"{ki}/{km}x2/{ko} P", credit_debit=fly_cost,
                    max_profit=(W - abs(fly_cost)) * 50, max_loss=abs(fly_cost) * 50,
                    probability="15%", edge="Fly", is_monthly=is_monthly
                ))
                if kb in chain['P']:
                    bwb_c = chain['P'][ki]['price'] - (2 * chain['P'][km]['price']) + chain['P'][kb]['price']
                    suggestions_to_save.append(TradeSuggestion(
                        snapshot=snapshot, strategy_type="Broken Wing Butterfly", dte=dte,
                        strikes=f"{ki}/{km}x2/{kb} P", credit_debit=bwb_c,
                        max_profit=W * 50, max_loss=abs(bwb_c) * 50, probability="65%", is_monthly=is_monthly
                    ))

            # --- 4. RATIO SPREAD ---
            kl, ks = get_strike_by_delta(dte, 'P', 0.30), get_strike_by_delta(dte, 'P', 0.15)
            if kl and ks:
                rv = chain['P'][kl]['price'] - (2 * chain['P'][ks]['price'])
                suggestions_to_save.append(TradeSuggestion(
                    snapshot=snapshot, strategy_type="Ratio Spread", dte=dte,
                    strikes=f"1x {kl}P / -2x {ks}P", credit_debit=rv,
                    max_profit=abs(kl - ks) * 50, max_loss=9999, probability="75%", is_monthly=is_monthly
                ))

            # --- 5. CALENDAR SPREAD ---
            if idx + 1 < len(dtes):
                nd = dtes[idx + 1]
                ka = get_closest_strike(dte, 'C', F)
                if ka in matrix[nd]['C']:
                    cc = matrix[nd]['C'][ka]['price'] - chain['C'][ka]['price']
                    suggestions_to_save.append(TradeSuggestion(
                        snapshot=snapshot, strategy_type="Calendar Spread", dte=dte,
                        strikes=f"{ka}C {dte}d/{nd}d", credit_debit=cc,
                        max_profit=999, max_loss=cc * 50, probability="40%", is_monthly=is_monthly
                    ))

            # --- 6. STRADDLE ---
            ka = get_closest_strike(dte, 'C', F)
            sc = chain['C'][ka]['price'] + chain['P'][ka]['price']
            suggestions_to_save.append(TradeSuggestion(
                snapshot=snapshot, strategy_type="Straddle", dte=dte,
                strikes=f"{ka} ATM C+P", credit_debit=sc,
                max_profit=99999, max_loss=sc * 50, probability="N/A", is_monthly=is_monthly
            ))

        TradeSuggestion.objects.filter(snapshot=snapshot).delete()
        TradeSuggestion.objects.bulk_create(suggestions_to_save)
        self.stdout.write(self.style.SUCCESS(f"🎯 Saved {len(suggestions_to_save)} Trades."))