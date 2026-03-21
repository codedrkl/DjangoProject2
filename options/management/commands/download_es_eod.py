import warnings
import pytz
import databento as db
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from django.core.management.base import BaseCommand
from django.utils import timezone
from django.conf import settings
from options.models import OptionChainSnapshot, OptionContract
from options.engines.black76 import Black76Engine   # ← NEW SHARED IMPORT

warnings.filterwarnings("ignore", module="databento")


class Command(BaseCommand):
    help = 'Institutional ES Downloader - Black-76 Synthesized Greeks with NaN Protection'

    def add_arguments(self, parser):
        parser.add_argument('--label', type=str, help='Snapshot label', default='EOD')
        parser.add_argument('--force-date', type=str, help='YYYY-MM-DD override')

    def resolve_target_date(self, force_date: str | None = None):
        if force_date: return datetime.strptime(force_date, '%Y-%m-%d').date()
        nyc_tz = pytz.timezone('America/New_York')
        now_nyc = datetime.now(nyc_tz)
        eod_available = now_nyc.hour >= 17
        if now_nyc.weekday() == 5:
            return (now_nyc - timedelta(days=1)).date()
        elif now_nyc.weekday() == 6:
            return (now_nyc - timedelta(days=2)).date()
        elif now_nyc.weekday() == 0:
            return now_nyc.date() if eod_available else (now_nyc - timedelta(days=3)).date()
        return now_nyc.date() if eod_available else (now_nyc - timedelta(days=1)).date()

    def handle(self, *args, **options):
        client = db.Historical(key=settings.DATABENTO_API_KEY)
        engine = Black76Engine(risk_free_rate=0.053)
        target_date = self.resolve_target_date(options['force_date'])
        label = options['label']
        dataset = getattr(settings, 'DATABENTO_DATASET', 'GLBX.MDP3')

        self.stdout.write(self.style.NOTICE(f"🚀 INGESTING: {target_date} | {label}"))
        start_search = pd.to_datetime(target_date).tz_localize('UTC')
        end_search = start_search + timedelta(days=1)

        try:
            def_df = client.timeseries.get_range(dataset=dataset, schema="definition", symbols="ALL_SYMBOLS",
                                                 start=start_search, end=end_search).to_df().reset_index()
            futures = def_df[(def_df.instrument_class == "F") & (def_df.asset == "ES")].sort_values('expiration')
            if futures.empty: return self.stdout.write(self.style.ERROR("No futures found."))

            lead_symbol = futures.iloc[0]['raw_symbol']
            all_future_symbols = list(futures.raw_symbol.unique())

            try:
                und_stats = client.timeseries.get_range(dataset=dataset, schema="statistics", symbols=[lead_symbol],
                                                        start=start_search, end=end_search).to_df()
                # Get the last settle price for the future
                raw_f_settle = und_stats[und_stats.stat_type == 3]['price'].iloc[-1] if not und_stats.empty else 5120.0
                F_settle = float(raw_f_settle) if not pd.isna(raw_f_settle) else 5120.0
            except:
                F_settle = 5120.0

            opts = def_df[
                (def_df.instrument_class.isin(["C", "P"])) & (def_df.underlying.isin(all_future_symbols))].copy()
            opts["expiration_dt"] = pd.to_datetime(opts["expiration"]).dt.tz_localize(None).dt.date
            opts["dte"] = (opts["expiration_dt"] - target_date).apply(lambda x: x.days)
            scope = opts[opts["dte"].between(0, 120)].copy()

            snapshot, _ = OptionChainSnapshot.objects.update_or_create(product="ES", date=target_date, label=label,
                                                                       defaults={'timestamp': timezone.now(),
                                                                                 'underlying_price': F_settle})
            records = []
            scope_dicts = scope.to_dict('records')

            for i in range(0, len(scope_dicts), 500):
                batch = scope_dicts[i:i + 500]
                batch_ids = [row["instrument_id"] for row in batch]
                pivot = pd.DataFrame()
                try:
                    stats = client.timeseries.get_range(dataset=dataset, schema="statistics", symbols=batch_ids,
                                                        stype_in="instrument_id", start=start_search,
                                                        end=end_search).to_df()
                    if not stats.empty:
                        pivot = stats.pivot_table(index='instrument_id', columns='stat_type',
                                                  values=['price', 'quantity'], aggfunc='last')
                except:
                    pass

                for row in batch:
                    sid = row["instrument_id"]
                    s_val = 0.0
                    oi_val = 0

                    if not pivot.empty and sid in pivot.index:
                        # Hardened extraction with NaN checks
                        p_val = pivot.loc[sid].get(('price', 3), np.nan)
                        q_val = pivot.loc[sid].get(('quantity', 9), np.nan)

                        s_val = float(p_val) if not pd.isna(p_val) else 0.0
                        oi_val = int(q_val) if not pd.isna(q_val) else 0

                    raw_strike = row.get("strike_price", 0)
                    strike_val = float(raw_strike) if not pd.isna(raw_strike) else 0.0

                    opt_type = row.get("instrument_class", "P")
                    dte = int(row["dte"])

                    calc_delta = 0.0
                    if strike_val > 0 and s_val > 0.0:
                        T = max(dte, 0.001) / 365.0
                        iv = engine.implied_volatility(s_val, F_settle, strike_val, T, opt_type)
                        calc_delta = engine.delta(F_settle, strike_val, T, iv, opt_type)

                    records.append(OptionContract(
                        snapshot=snapshot, instrument_id=sid, raw_symbol=row.get("raw_symbol", ""),
                        expiration=pd.to_datetime(row["expiration"]), strike=strike_val,
                        option_type=opt_type, settlement=s_val, open_interest=oi_val,
                        delta=calc_delta, dte=dte
                    ))

            OptionContract.objects.filter(snapshot=snapshot).delete()
            OptionContract.objects.bulk_create(records, batch_size=2000)
            self.stdout.write(self.style.SUCCESS(f"✅ Success: {len(records)} contracts written with stable Greeks."))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Fail: {e}"))