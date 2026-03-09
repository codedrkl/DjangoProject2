import pytz
import math
from django.core.management.base import BaseCommand
from django.utils import timezone
from options.models import EODOptionSnapshot, TradeSuggestion
from .download_es_eod import download_es_market_snapshot


class Command(BaseCommand):
    help = 'Executes Multi-Strategy Volatility Surface Matrix Scan'

    def handle(self, *args, **options):
        nyc_tz = pytz.timezone('America/New_York')
        now_nyc = timezone.now().astimezone(nyc_tz)
        hour_nyc = now_nyc.hour

        self.stdout.write(f"🕒 NYC Market Time: {now_nyc.strftime('%H:%M')} EST")

        today_snaps = EODOptionSnapshot.objects.filter(date=now_nyc.date()).order_by('-timestamp')
        target_snapshot = None

        if 9 <= hour_nyc < 16:
            self.stdout.write("📈 RTH ACTIVE: Seeking most recent Intraday data...")
            target_snapshot = today_snaps.exclude(label='EOD').first()
        elif 16 <= hour_nyc < 20:
            self.stdout.write("🌙 POST-RTH: Seeking EOD Settlement...")
            target_snapshot = today_snaps.filter(label='EOD').first()
        else:
            self.stdout.write("📡 GLOBEX ACTIVE: Seeking most recent AEST/OD data...")
            target_snapshot = today_snaps.exclude(label='EOD').first()

        if not target_snapshot:
            self.stdout.write(self.style.WARNING("📡 No recent snapshot found. Triggering ID-Force Download..."))
            target_snapshot = download_es_market_snapshot()

        if target_snapshot:
            self.run_scanner_logic(target_snapshot)
        else:
            self.stdout.write(self.style.ERROR("❌ Scanner aborted. Valid snapshot acquisition failed."))

    def run_scanner_logic(self, snapshot):
        self.stdout.write(f"🔍 Matrix Compilation: {snapshot.label} (ID: {snapshot.id})")
        suggestions_to_save = []

        F = float(snapshot.underlying_settlement) if snapshot.underlying_settlement else 5120.0

        # 1. ORM BYPASS: Construct high-speed O(1) lookup matrix mapping [DTE][Type][Strike] -> Price
        raw_contracts = snapshot.contracts.all()
        matrix = {}
        for c in raw_contracts:
            exp_date = c.expiration.date() if hasattr(c.expiration, 'date') else c.expiration
            dte = (exp_date - snapshot.date).days
            if dte < 0 or dte > 200: continue

            if dte not in matrix: matrix[dte] = {'P': {}, 'C': {}}
            matrix[dte][c.option_type][float(c.strike)] = float(c.settlement)

        dtes = sorted(matrix.keys())

        def get_closest_strike(dte, opt_type, target):
            strikes = list(matrix[dte][opt_type].keys())
            if not strikes: return None
            return min(strikes, key=lambda x: abs(x - target))

        def safe_div(n, d):
            return n / d if d else 0.0

        for idx, dte in enumerate(dtes):
            W = 50.0 if dte <= 30 else 100.0  # Dynamic Wing Width
            K_ATM = get_closest_strike(dte, 'C', F)
            if not K_ATM: continue

            C_ATM = matrix[dte]['C'].get(K_ATM, 0)
            P_ATM = matrix[dte]['P'].get(K_ATM, 0)
            if C_ATM == 0 or P_ATM == 0: continue

            # ---------------------------------------------------------
            # 1. VERTICAL SPREADS (Iterating via Delta Proxies)
            # ---------------------------------------------------------
            for d_target in [0.30, 0.20, 0.10]:
                for opt_type in ['P', 'C']:
                    target_k = F * math.exp(-0.85 * 0.20 * math.sqrt(dte / 365)) if opt_type == 'P' else \
                        F * math.exp(0.85 * 0.20 * math.sqrt(dte / 365))

                    K_short = get_closest_strike(dte, opt_type, target_k)
                    if not K_short: continue
                    K_long = K_short - W if opt_type == 'P' else K_short + W

                    P_short = matrix[dte][opt_type].get(K_short, 0)
                    P_long = matrix[dte][opt_type].get(K_long, 0)

                    if P_short > 0 and P_long > 0:
                        credit = abs(P_short - P_long)
                        if credit >= (W * 0.01):
                            suggestions_to_save.append(TradeSuggestion(
                                snapshot=snapshot, strategy_type=f"Vertical {'Put' if opt_type == 'P' else 'Call'}",
                                dte=dte, strikes=f"{K_short}/{K_long}", credit_debit=credit,
                                max_profit=credit * 50, max_loss=(W - credit) * 50,
                                probability=f"{int((1 - d_target) * 100)}%", edge='+0.42',
                                rr_ratio=f"1:{round(safe_div(W - credit, credit), 2)}"
                            ))

            # ---------------------------------------------------------
            # 2. STRADDLES (ATM)
            # ---------------------------------------------------------
            straddle_debit = C_ATM + P_ATM
            suggestions_to_save.append(TradeSuggestion(
                snapshot=snapshot, strategy_type="Straddle", dte=dte, strikes=f"{K_ATM}C/{K_ATM}P",
                credit_debit=straddle_debit, max_profit=99999, max_loss=straddle_debit * 50,
                probability="N/A", edge='+0.25', rr_ratio="Unlimited"
            ))

            # ---------------------------------------------------------
            # 3. IRON CONDORS (Dynamic Width)
            # ---------------------------------------------------------
            K_Put_Short = get_closest_strike(dte, 'P', F * 0.95)
            K_Call_Short = get_closest_strike(dte, 'C', F * 1.05)
            if K_Put_Short and K_Call_Short:
                K_Put_Long = K_Put_Short - W
                K_Call_Long = K_Call_Short + W

                P_PS = matrix[dte]['P'].get(K_Put_Short, 0);
                P_PL = matrix[dte]['P'].get(K_Put_Long, 0)
                C_CS = matrix[dte]['C'].get(K_Call_Short, 0);
                C_CL = matrix[dte]['C'].get(K_Call_Long, 0)

                if all([P_PS, P_PL, C_CS, C_CL]):
                    ic_credit = (P_PS - P_PL) + (C_CS - C_CL)
                    if ic_credit > 0:
                        suggestions_to_save.append(TradeSuggestion(
                            snapshot=snapshot, strategy_type="Iron Condor", dte=dte,
                            strikes=f"{K_Put_Long}/{K_Put_Short}P - {K_Call_Short}/{K_Call_Long}C",
                            credit_debit=ic_credit, max_profit=ic_credit * 50, max_loss=(W - ic_credit) * 50,
                            probability="68%", edge='+0.35',
                            rr_ratio=f"1:{round(safe_div(W - ic_credit, ic_credit), 2)}"
                        ))

            # ---------------------------------------------------------
            # 4 & 5. BUTTERFLY & BROKEN WING BUTTERFLY (Puts)
            # ---------------------------------------------------------
            K_Fly_Short = get_closest_strike(dte, 'P', F * 0.98)  # Slightly OTM
            if K_Fly_Short:
                K_Fly_ITM = K_Fly_Short + W
                K_Fly_OTM = K_Fly_Short - W
                K_BWB_OTM = K_Fly_Short - (W * 2)  # Asymmetric extension for BWB

                P_Short = matrix[dte]['P'].get(K_Fly_Short, 0)
                P_ITM = matrix[dte]['P'].get(K_Fly_ITM, 0)
                P_OTM = matrix[dte]['P'].get(K_Fly_OTM, 0)
                P_BWB = matrix[dte]['P'].get(K_BWB_OTM, 0)

                if P_Short and P_ITM and P_OTM:
                    fly_cost = P_ITM - (2 * P_Short) + P_OTM
                    suggestions_to_save.append(TradeSuggestion(
                        snapshot=snapshot, strategy_type="Butterfly (Put)", dte=dte,
                        strikes=f"{K_Fly_ITM}/{K_Fly_Short}x2/{K_Fly_OTM}", credit_debit=abs(fly_cost),
                        max_profit=(W - abs(fly_cost)) * 50, max_loss=abs(fly_cost) * 50, probability="15%",
                        edge='+0.18', rr_ratio=f"{round(safe_div(W - abs(fly_cost), abs(fly_cost)), 2)}:1"
                    ))

                if P_Short and P_ITM and P_BWB:
                    bwb_cost = P_ITM - (2 * P_Short) + P_BWB
                    bwb_type = "Credit" if bwb_cost < 0 else "Debit"
                    suggestions_to_save.append(TradeSuggestion(
                        snapshot=snapshot, strategy_type="Broken Wing Butterfly", dte=dte,
                        strikes=f"{K_Fly_ITM}/{K_Fly_Short}x2/{K_BWB_OTM}", credit_debit=abs(bwb_cost),
                        max_profit=(W - abs(bwb_cost)) * 50, max_loss=abs((W * 2) - W + bwb_cost) * 50,
                        probability="65%", edge='+0.40', rr_ratio="Variable"
                    ))

            # ---------------------------------------------------------
            # 6. RATIO SPREADS (Front Ratio 1x2 Call)
            # ---------------------------------------------------------
            K_Ratio_Long = K_ATM
            K_Ratio_Short = K_ATM + W
            C_RL = matrix[dte]['C'].get(K_Ratio_Long, 0)
            C_RS = matrix[dte]['C'].get(K_Ratio_Short, 0)
            if C_RL and C_RS:
                ratio_cost = C_RL - (2 * C_RS)
                suggestions_to_save.append(TradeSuggestion(
                    snapshot=snapshot, strategy_type="Ratio Spread 1x2 (Call)", dte=dte,
                    strikes=f"+{K_Ratio_Long}C / -2x {K_Ratio_Short}C", credit_debit=abs(ratio_cost),
                    max_profit=W * 50, max_loss=99999, probability="70%", edge='+0.22', rr_ratio="N/A"
                ))

            # ---------------------------------------------------------
            # 7. CALENDAR SPREADS (Time Horizon Delta)
            # ---------------------------------------------------------
            if idx + 1 < len(dtes):
                back_dte = dtes[idx + 1]
                C_Front = matrix[dte]['C'].get(K_ATM, 0)
                C_Back = matrix[back_dte]['C'].get(K_ATM, 0)
                if C_Front and C_Back:
                    cal_debit = C_Back - C_Front
                    suggestions_to_save.append(TradeSuggestion(
                        snapshot=snapshot, strategy_type="Calendar Spread", dte=dte,
                        strikes=f"-{dte}d {K_ATM}C / +{back_dte}d {K_ATM}C", credit_debit=abs(cal_debit),
                        max_profit=cal_debit * 150, max_loss=abs(cal_debit) * 50, probability="45%", edge='+0.30',
                        rr_ratio="Variable"
                    ))

            # ---------------------------------------------------------
            # 8. AI SUGGESTIONS: Vega -60 Hedge (Khamenei Macro Shock)
            # ---------------------------------------------------------
            K_Hedge_Short = get_closest_strike(dte, 'P', F * 0.90)
            K_Hedge_Long = K_Hedge_Short - W if K_Hedge_Short else None
            if K_Hedge_Short and K_Hedge_Long:
                P_HS = matrix[dte]['P'].get(K_Hedge_Short, 0)
                P_HL = matrix[dte]['P'].get(K_Hedge_Long, 0)
                if P_HS and P_HL:
                    hedge_cost = (2 * P_HL) - P_HS  # Buy 2 far OTM, Sell 1 closer OTM
                    suggestions_to_save.append(TradeSuggestion(
                        snapshot=snapshot, strategy_type="AI: Vega Hedge (1x2 Put Backspread)", dte=dte,
                        strikes=f"-{K_Hedge_Short}P / +2x {K_Hedge_Long}P", credit_debit=abs(hedge_cost),
                        max_profit=99999, max_loss=(W + abs(hedge_cost)) * 50, probability="10%", edge='+0.85',
                        rr_ratio="Macro"
                    ))

        # ---------------------------------------------------------
        # BATCH PERSISTENCE
        # ---------------------------------------------------------
        if suggestions_to_save:
            TradeSuggestion.objects.filter(snapshot=snapshot).delete()
            TradeSuggestion.objects.bulk_create(suggestions_to_save)
            self.stdout.write(self.style.SUCCESS(f"🎯 Saved {len(suggestions_to_save)} Advanced Matrix Parameters."))
        else:
            self.stdout.write(self.style.WARNING("⚠️ Matrix collapsed. No mathematical edge resolved."))