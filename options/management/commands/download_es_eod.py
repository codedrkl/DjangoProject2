import os
import math
import databento as db
import pandas as pd
from datetime import timedelta, date
from django.core.management.base import BaseCommand
from django.utils import timezone
from django.conf import settings
from options.models import EODOptionSnapshot, OptionContract

def download_es_market_snapshot(label_override=None):
    client = db.Historical(key=settings.DATABENTO_API_KEY)
    now = timezone.now()
    aest_now = now + timedelta(hours=10)
    label = label_override or f"OD_{aest_now.strftime('%H%M_AEST')}"

    safe_end = now - timedelta(minutes=60)
    safe_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    try:
        data = client.timeseries.get_range(
            dataset="GLBX.MDP3", symbols="ES.OPT", schema='definition',
            stype_in='parent', start=safe_start, end=safe_end
        )
        df = data.to_df().reset_index()
        if df.empty: return None

        df.fillna(0, inplace=True)
        F_settle = float(df['trading_reference_price'].iloc[0]) if 'trading_reference_price' in df.columns else 5120.0

        snapshot = EODOptionSnapshot.objects.create(
            product="ES", date=now.date(), label=label, underlying_settlement=F_settle
        )

        df["exp_dt"] = pd.to_datetime(df["expiration"]).dt.tz_localize(None).dt.date
        df["dte"] = (df["exp_dt"] - now.date()).apply(lambda x: x.days)
        scope = df[df["dte"].between(0, 10)].copy()

        batch_ids = scope["instrument_id"].tolist()
        pivot = pd.DataFrame()
        if batch_ids:
            try:
                stats = client.timeseries.get_range(
                    dataset="GLBX.MDP3", schema="statistics", symbols=batch_ids,
                    stype_in="instrument_id", start=safe_start, end=safe_end
                ).to_df()
                if not stats.empty:
                    stats.fillna(0, inplace=True)
                    pivot = stats.pivot_table(index='instrument_id', columns='stat_type', values='price', aggfunc='last')
                    pivot.fillna(0, inplace=True)
            except Exception as e:
                print(f"⚠️ Stats Schema Error: {e}")

        contracts = []
        for _, row in scope.iterrows():
            sid = row["instrument_id"]
            live_price = float(pivot.loc[sid].get(3, 0)) if sid in pivot.index else float(row.get('trading_reference_price', 0))

            contracts.append(OptionContract(
                snapshot=snapshot,
                raw_symbol=row.get('raw_symbol', ''),
                strike=float(row.get('strike_price', 0)) / 1e9,
                expiration=pd.to_datetime(row.get('expiration')),
                option_type=row.get('instrument_class', 'P'),
                settlement=live_price,
                dte=int(row["dte"])
            ))

        OptionContract.objects.bulk_create(contracts, batch_size=1000)
        print(f"✅ {label} Saved: {len(contracts)} contracts (Live RTH Pricing Active).")
        return snapshot

    except Exception as e:
        print(f"❌ {label} Ingestion Error: {e}")
        return None


class Command(BaseCommand):
    help = 'Institutional ES Downloader - 10-DTE Intraday vs 200-DTE EOD'

    def add_arguments(self, parser):
        parser.add_argument('--date', type=str, help='YYYY-MM-DD')
        parser.add_argument('--label', type=str, help='MO, LC, PH, or EOD', default='EOD')

    def handle(self, *args, **options):
        target_date = date.fromisoformat(options['date']) if options['date'] else (timezone.now() - timedelta(days=1)).date()
        session_label = options['label']
        is_eod = (session_label == 'EOD')
        client = db.Historical(key=settings.DATABENTO_API_KEY)

        EODOptionSnapshot.objects.filter(product="ES", date=target_date, label=session_label).delete()
        self.stdout.write(self.style.NOTICE(f"🚀 RUNNING: {target_date} | {session_label}"))

        try:
            def_df = client.timeseries.get_range(
                dataset=settings.DATASET, schema="definition", symbols="ES.OPT",
                stype_in="parent", start=target_date, end=target_date + timedelta(days=1)
            ).to_df()
            def_df.fillna(0, inplace=True)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"API Error: {e}"))
            return

        def_df["exp_dt"] = pd.to_datetime(def_df["expiration"]).dt.tz_localize(None).dt.date
        def_df["dte_calc"] = (def_df["exp_dt"] - target_date).apply(lambda x: x.days)

        max_dte = 200 if is_eod else 10
        scope = def_df[def_df["dte_calc"].between(0, max_dte)].copy()

        self.stdout.write(f"📡 Filters: {session_label} | Scope: {len(scope)} contracts.")

        snapshot = EODOptionSnapshot.objects.create(
            product="ES", date=target_date, label=session_label, underlying_settlement=0
        )

        records = []
        scope_dicts = scope.to_dict('records')

        for i in range(0, len(scope_dicts), 500):
            batch = scope_dicts[i:i + 500]
            batch_ids = [row["instrument_id"] for row in batch]

            pivot = pd.DataFrame()
            try:
                stats = client.timeseries.get_range(
                    dataset=settings.DATASET, schema="statistics", symbols=batch_ids,
                    stype_in="instrument_id", start=target_date, end=target_date + timedelta(days=1)
                ).to_df()
                if not stats.empty:
                    stats.fillna(0, inplace=True)
                    pivot = stats.pivot_table(index='instrument_id', columns='stat_type', values='price', aggfunc='last')
                    pivot.fillna(0, inplace=True)
            except: pass

            for row in batch:
                sid = row["instrument_id"]
                s_val = pivot.loc[sid].get(3, 0) if sid in pivot.index else 0

                records.append(OptionContract(
                    snapshot=snapshot, raw_symbol=row["raw_symbol"],
                    expiration=row["expiration"], strike=float(row["strike_price"]) / 1e9,
                    option_type=row["instrument_class"], settlement=float(s_val),
                    dte=int(row["dte_calc"])
                ))

        if records:
            OptionContract.objects.bulk_create(records, batch_size=2000)
            self.stdout.write(self.style.SUCCESS(f"✅ Saved {len(records)} contracts to DB."))