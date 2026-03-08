import databento as db
import pandas as pd
from django.core.management.base import BaseCommand
from django.utils import timezone
from django.conf import settings
from datetime import timedelta, date

from options.models import EODOptionSnapshot, OptionContract


class Command(BaseCommand):
    help = 'Institutional ES Downloader - Persistent ID-Forced Save'

    def add_arguments(self, parser):
        parser.add_argument('--date', type=str, help='YYYY-MM-DD')

    def handle(self, *args, **options):
        # 1. Date Resolution
        target_date = date.fromisoformat(options['date']) if options['date'] else (
                    timezone.now() - timedelta(days=1)).date()

        # 2. THE SMART GATE
        EODOptionSnapshot.objects.filter(product="ES", date=target_date).delete()

        self.stdout.write(self.style.NOTICE(f"🚀 INITIATING ID-FORCE SCAN: {target_date}"))
        client = db.Historical(key=settings.DATABENTO_API_KEY)

        # 3. UNIVERSAL PULL
        try:
            def_df = client.timeseries.get_range(
                dataset=settings.DATASET,
                schema="definition",
                symbols="ALL_SYMBOLS",
                start=target_date,
                end=target_date + timedelta(days=1)
            ).to_df()
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"API Error: {e}"))
            return

        # 4. Extract the Clean Standard Future Curve (ESH6, ESM6, ESU6)
        # Filtering by asset='ES' removes the daily micro variants (ES1H603)
        futures = def_df[
            (def_df.instrument_class == "F") &
            (def_df.asset == "ES")
            ].sort_values('expiration')

        if futures.empty:
            self.stdout.write(self.style.ERROR("No standard ES futures found."))
            return

        lead_symbol = futures.iloc[0]['raw_symbol']
        all_future_symbols = list(futures.raw_symbol.unique())
        self.stdout.write(f"🎯 Anchor: {lead_symbol} | Standard Curve: {all_future_symbols[:4]}")

        # 5. Underlying Price
        try:
            und_stats = client.timeseries.get_range(dataset=settings.DATASET, schema="statistics",
                                                    symbols=[lead_symbol], start=target_date,
                                                    end=target_date + timedelta(days=1)).to_df()
            underlying_price = float(
                und_stats[und_stats.stat_type == 3]['price'].iloc[-1]) if not und_stats.empty else None
        except Exception as e:
            self.stdout.write(f"Underlying price warning: {e}")
            underlying_price = None

        # 6. DENSITY & TIMELINE FILTER
        opts = def_df[
            (def_df.instrument_class.isin(["C", "P"])) &
            (def_df.underlying.isin(all_future_symbols))
            ].copy()

        opts["exp_dt"] = pd.to_datetime(opts["expiration"]).dt.tz_localize(None).dt.date
        opts["dte_calc"] = (opts["exp_dt"] - target_date).apply(lambda x: x.days)
        scope = opts[opts["dte_calc"].between(0, 120)].copy()

        self.stdout.write(f"📡 Validated Scope: {len(scope)} contracts.")

        # 7. Create Snapshot
        snapshot = EODOptionSnapshot.objects.create(product="ES", date=target_date,
                                                    underlying_settlement=underlying_price)

        # 8. BULLETPROOF BATCH SAVE VIA INSTRUMENT_ID
        records = []
        # Convert DataFrame to list of dicts for faster iteration
        scope_dicts = scope.to_dict('records')

        for i in range(0, len(scope_dicts), 500):
            batch = scope_dicts[i:i + 500]
            batch_ids = [row["instrument_id"] for row in batch]

            pivot = pd.DataFrame()
            try:
                # Querying by instrument_id bypasses all raw_symbol string format rejections
                stats = client.timeseries.get_range(
                    dataset=settings.DATASET,
                    schema="statistics",
                    symbols=batch_ids,
                    stype_in="instrument_id",  # 🎯 The bypass key
                    start=target_date,
                    end=target_date + timedelta(days=1)
                ).to_df()
                if not stats.empty:
                    pivot = stats.pivot_table(index='instrument_id', columns='stat_type', values=['price', 'quantity'],
                                              aggfunc='last')
            except Exception as e:
                self.stdout.write(self.style.WARNING(f"⚠️ Batch stats failed, defaulting to 0: {e}"))
                # Proceed with empty pivot so contracts are still saved structurally

            for row in batch:
                sid = row["instrument_id"]
                s_val = 0
                oi_val = 0
                if sid in pivot.index:
                    if ('price', 3) in pivot.columns:
                        s_val = pivot.loc[sid].get(('price', 3), 0)
                    if ('quantity', 9) in pivot.columns:
                        oi_val = pivot.loc[sid].get(('quantity', 9), 0)

                records.append(OptionContract(
                    snapshot=snapshot, raw_symbol=row["raw_symbol"], expiration=row["expiration"],
                    strike=row["strike_price"], option_type=row["instrument_class"],
                    settlement=float(s_val) if pd.notna(s_val) else 0,
                    open_interest=int(oi_val) if pd.notna(oi_val) else 0, dte=int(row["dte_calc"])
                ))

        if records:
            OptionContract.objects.bulk_create(records, batch_size=1000)
            self.stdout.write(
                self.style.SUCCESS(f"✅ FINAL PERSISTENCE COMPLETE: {len(records)} contracts written to DB."))
        else:
            self.stdout.write(self.style.ERROR("❌ FATAL: No records generated for DB insertion."))