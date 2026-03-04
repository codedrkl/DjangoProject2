import databento as db
import pandas as pd
import math
import numpy as np
from scipy.optimize import brentq
from django.core.management.base import BaseCommand
from django.utils import timezone
from django.conf import settings
from datetime import timedelta
from options.models import EODOptionSnapshot, OptionContract

# ====================== BLACK-76 HELPERS ======================
def black76_call(F, K, T, sigma):
    if T <= 0 or sigma <= 0:
        return max(F - K, 0)
    d1 = (math.log(F / K) + (sigma ** 2 / 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return F * norm_cdf(d1) - K * norm_cdf(d2)

def norm_cdf(x):
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0

def norm_pdf(x):
    return math.exp(-x**2 / 2.0) / math.sqrt(2.0 * math.pi)

def implied_vol(F, K, T, price, option_type='C'):
    if T <= 0 or price <= 0:
        return None
    def objective(sigma):
        if option_type == 'C':
            return black76_call(F, K, T, sigma) - price
        else:
            return black76_call(F, K, T, sigma) - (F - K) - price
    try:
        return brentq(objective, 1e-5, 8.0, xtol=1e-8, maxiter=100)
    except (ValueError, RuntimeError):
        return None

def black76_delta(F, K, T, sigma, option_type='C'):
    if T <= 0 or sigma is None or sigma <= 0 or math.isnan(sigma):
        return 1.0 if (option_type == 'C' and F > K) else 0.0
    d1 = (math.log(F / K) + (sigma ** 2 / 2) * T) / (sigma * math.sqrt(T))
    if option_type == 'C':
        return norm_cdf(d1)
    return norm_cdf(d1) - 1

def black76_gamma(F, K, T, sigma):
    if T <= 0 or sigma is None or sigma <= 0 or math.isnan(sigma):
        return None
    d1 = (math.log(F / K) + (sigma ** 2 / 2) * T) / (sigma * math.sqrt(T))
    return norm_pdf(d1) / (F * sigma * math.sqrt(T))

def black76_theta(F, K, T, sigma, option_type='C'):
    if T <= 0 or sigma is None or sigma <= 0 or math.isnan(sigma):
        return None
    d1 = (math.log(F / K) + (sigma ** 2 / 2) * T) / (sigma * math.sqrt(T))
    theta_base = -(F * sigma * norm_pdf(d1)) / (2.0 * math.sqrt(T))
    return theta_base / 365.25

# ====================== MAIN COMMAND ======================
class Command(BaseCommand):
    help = 'Final ES downloader — 100% NaN-safe + Gemini-enhanced'

    def handle(self, *args, **options):
        client = db.Historical(key=settings.DATABENTO_API_KEY)
        target_date = (timezone.now() - timedelta(days=1)).date()

        self.stdout.write(f"🔍 Downloading for {target_date}")

        def_df = client.timeseries.get_range(
            dataset=settings.DATASET,
            schema="definition",
            symbols="ALL_SYMBOLS",
            start=target_date,
            end=target_date + timedelta(days=1),
        ).to_df()

        futures = def_df[(def_df.instrument_class == "F") & (def_df.asset == settings.PRODUCT)]["raw_symbol"].unique()

        underlying_price = None
        if len(futures) > 0:
            und_stats = client.timeseries.get_range(
                dataset=settings.DATASET,
                schema="statistics",
                symbols=[futures[0]],
                start=target_date,
                end=target_date + timedelta(days=1),
            ).to_df()
            sett = und_stats[und_stats.stat_type == 3]
            if not sett.empty:
                underlying_price = float(sett['price'].iloc[-1])

        if underlying_price:
            min_strike = max(5800, int(underlying_price - 1650))
            max_strike = int(underlying_price + 800)
        else:
            min_strike, max_strike = 5800, 8000

        opts = def_df[
            (def_df.underlying.isin(futures)) &
            (def_df.instrument_class.isin(["C", "P"])) &
            (def_df.strike_price.between(min_strike, max_strike))
        ].copy()

        target_datetime = pd.to_datetime(target_date).tz_localize('UTC')
        opts["exp_datetime"] = pd.to_datetime(opts["expiration"])
        opts["dte_exact"] = (opts["exp_datetime"] - target_datetime).dt.total_seconds() / 86400.0
        opts["T"] = opts["dte_exact"] / 365.25

        short_term = opts[opts["dte_exact"] <= 15]

        symbols = short_term["raw_symbol"].tolist()

        stats_list = []
        for i in range(0, len(symbols), 500):
            batch_stats = client.timeseries.get_range(
                dataset=settings.DATASET,
                schema="statistics",
                symbols=symbols[i:i+500],
                start=target_date,
                end=target_date + timedelta(days=1),
            ).to_df()
            stats_list.append(batch_stats)

        stats_df = pd.concat(stats_list) if stats_list else pd.DataFrame()
        stats_pivot = stats_df.pivot_table(
            index='instrument_id', columns='stat_type',
            values=['price', 'quantity'], aggfunc='last'
        ) if not stats_df.empty else pd.DataFrame()

        snapshot, _ = EODOptionSnapshot.objects.update_or_create(
            product=settings.PRODUCT,
            date=target_date,
            defaults={'underlying_settlement': underlying_price, 'created_at': timezone.now()}
        )
        snapshot.contracts.all().delete()

        records = []
        iv_count = 0
        for _, row in short_term.iterrows():
            instr_id = row["instrument_id"]
            settlement = None
            volume = 0
            oi = 0

            if instr_id in stats_pivot.index:
                p = stats_pivot.loc[instr_id]
                sett_val = p.get(('price', 3))
                if pd.notna(sett_val):
                    settlement = float(sett_val)
                oi_val = p.get(('quantity', 9))
                oi = int(oi_val) if pd.notna(oi_val) else 0
                vol_val = p.get(('quantity', 6))
                volume = int(vol_val) if pd.notna(vol_val) else 0

            iv = delta = gamma = theta = None
            if settlement and settlement > 0 and underlying_price and row["T"] > 0:
                T = float(row["T"])
                K = float(row["strike_price"])
                F = underlying_price
                iv = implied_vol(F, K, T, settlement, row["instrument_class"])
                if iv is not None and not math.isnan(iv) and iv > 0:
                    delta = black76_delta(F, K, T, iv, row["instrument_class"])
                    gamma = black76_gamma(F, K, T, iv)
                    theta = black76_theta(F, K, T, iv, row["instrument_class"])
                    iv_count += 1

            records.append(OptionContract(
                snapshot=snapshot,
                raw_symbol=row["raw_symbol"],
                underlying=row["underlying"],
                expiration=row["expiration"],
                strike=row["strike_price"],
                option_type=row["instrument_class"],
                settlement=settlement,
                volume=volume,
                open_interest=oi,
                dte=int(row["dte_exact"]),
                implied_vol=iv,
                delta=delta,
                gamma=gamma,
                theta=theta,
            ))

        OptionContract.objects.bulk_create(records)

        self.stdout.write(self.style.SUCCESS(
            f"🎉 SAVED {len(records)} contracts | {iv_count} with full Greeks"
        ))
        if underlying_price:
            self.stdout.write(f"   ES Future: {underlying_price:.2f} ✅")