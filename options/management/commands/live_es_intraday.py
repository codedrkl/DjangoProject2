# options/management/commands/download_es_10dte.py

import warnings
import pytz
import re
import math
import databento as db
import pandas as pd
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from django.conf import settings
from options.models import IntradayOptionContract
from options.engines.black76 import Black76Engine

warnings.filterwarnings("ignore", module="databento")


class Command(BaseCommand):
    help = 'Download ES option chain (0–9 DTE) and persist to IntradayOptionContract'

    def handle(self, *args, **options):
        engine = Black76Engine()
        nyc_tz = pytz.timezone('America/New_York')
        today_nyc = datetime.now(nyc_tz).date()

        client = db.Historical(key=settings.DATABENTO_API_KEY)

        self.stdout.write(self.style.NOTICE(f"Downloading ES option chain – date: {today_nyc}"))

        # ─── FIXED TIME WINDOW (avoids 422 data_start_too_precise_to_forward_fill) ───
        utc_now = datetime.now(pytz.UTC)
        start = utc_now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        end = None  # latest available

        # ==================== DOWNLOAD DEFINITIONS ====================
        try:
            df = client.timeseries.get_range(
                dataset="GLBX.MDP3",
                schema="definition",
                symbols=["ES.OPT", "EW.OPT", "EW1.OPT", "EW2.OPT", "EW3.OPT", "EW4.OPT"],
                stype_in="parent",
                start=start,
                end=end
            ).to_df()

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Definition download failed: {e}"))
            return

        if df.empty:
            self.stdout.write(self.style.ERROR("No option definitions returned."))
            return

        # ==================== CLEAN & FILTER 0–9 DTE ====================
        df = df[df['instrument_class'].isin(['C', 'P'])].copy()
        df['strike'] = pd.to_numeric(df['strike_price'], errors='coerce')
        df = df.dropna(subset=['strike'])

        df['expiration'] = pd.to_datetime(df['expiration']).dt.date
        df['dte'] = (df['expiration'] - today_nyc).apply(lambda x: x.days)

        # Keep only 0–9 DTE (10 days total)
        scope_df = df[(df['dte'] >= 0) & (df['dte'] <= 9)].copy()

        if scope_df.empty:
            self.stdout.write(self.style.WARNING("No contracts in 0–9 DTE range."))
            return

        self.stdout.write(self.style.SUCCESS(f"Found {len(scope_df)} contracts in 0–9 DTE"))

        # ==================== UNDERLYING PRICE (latest available) ====================
        try:
            fut_df = client.timeseries.get_range(
                dataset="GLBX.MDP3",
                schema="mbp-1",               # more reliable than trades
                symbols="ES.c.0",
                stype_in="continuous",
                start=start,
                end=end
            ).to_df()

            underlying_price = float(fut_df['close'].iloc[-1]) if not fut_df.empty else 0.0

        except Exception as e:
            self.stdout.write(self.style.WARNING(f"Underlying price fetch failed: {e}"))
            underlying_price = 0.0

        if underlying_price <= 0:
            self.stdout.write(self.style.ERROR("No valid underlying price found – aborting"))
            return

        self.stdout.write(self.style.SUCCESS(f"Underlying price (ES): {underlying_price:.2f}"))

        # ==================== LAST PRICES FOR OPTIONS ====================
        try:
            prices_df = client.timeseries.get_range(
                dataset="GLBX.MDP3",
                schema="mbp-1",
                symbols=scope_df['raw_symbol'].tolist(),
                stype_in="raw_symbol",
                start=start,
                end=end
            ).to_df()

            last_prices = prices_df.groupby('symbol')['close'].last().to_dict()

        except Exception as e:
            self.stdout.write(self.style.WARNING(f"Price download failed: {e}"))
            last_prices = {}

        # ==================== BUILD & PERSIST RECORDS ====================
        records = []
        timestamp = datetime.now(nyc_tz)

        for _, row in scope_df.iterrows():
            sid = int(row['instrument_id'])
            raw_sym = str(row['raw_symbol'])
            strike = float(row['strike'])
            opt_type = row['instrument_class']
            exp_date = row['expiration']
            dte = row['dte']

            px = last_prices.get(raw_sym, 0.0)
            if px <= 0:
                continue

            T = max(dte, 0.1) / 365.25

            try:
                iv = engine.implied_volatility(px, underlying_price, strike, T, opt_type)
                delta = engine.delta(underlying_price, strike, T, iv, opt_type)
            except Exception:
                continue

            records.append(IntradayOptionContract(
                timestamp=timestamp,
                underlying_price=underlying_price,
                instrument_id=sid,
                raw_symbol=raw_sym,
                expiration=exp_date,
                strike=strike,
                option_type=opt_type,
                settlement=px,
                open_interest=int(row.get('open_interest', 0)),
                implied_vol=iv,
                delta=delta,
                dte=dte
            ))

        if records:
            try:
                IntradayOptionContract.objects.bulk_create(records, batch_size=2000)
                self.stdout.write(
                    self.style.SUCCESS(
                        f"✅ Persisted {len(records)} contracts (0–9 DTE) at {timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    )
                )
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Database insert failed: {e}"))
        else:
            self.stdout.write(self.style.WARNING("No valid priced contracts to persist"))
