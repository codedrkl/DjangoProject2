import os
import django
import pandas as pd
from datetime import date, timedelta

# 1. Setup Django Environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'es_option_chain.settings')
django.setup()

from options.models import EODOptionSnapshot, OptionContract, TradeSuggestion


def run_diagnostic():
    print("" + "=" * 50)
    print("🔍 INSTITUTIONAL DB INTEGRITY CHECK")
    print("=" * 50 + "\n")

    # A. Check Snapshots
    snaps = EODOptionSnapshot.objects.all().order_by('-date')
    if not snaps.exists():
        print("❌ ERROR: No snapshots found in EODOptionSnapshot.")
        return

    latest = snaps.first()
    print(f"✅ LATEST SNAPSHOT: {latest.date} (Price: {latest.underlying_settlement})")

    # B. Check 120-DTE Coverage
    limit_date = latest.date + timedelta(days=120)
    contracts = latest.contracts.all()

    total_count = contracts.count()
    long_dated = contracts.filter(expiration__date__gte=latest.date + timedelta(days=10))
    mar_20 = contracts.filter(expiration__date=date(2026, 3, 20))

    print(f"📊 TOTAL CONTRACTS: {total_count}")
    print(f"📅 LONG-DATED (10+ DTE): {long_dated.count()}")
    print(f"🎯 MARCH 20TH QUARTERLY: {mar_20.count()} contracts")

    if mar_20.exists():
        # Check Notional Density for $100k Profit Goal [cite: 2026-03-08]
        sample = mar_20.first()
        print(f"   - Sample Strike: {sample.strike} {sample.option_type}")
        print(f"   - Sample OI: {sample.open_interest}")
    else:
        print("⚠️ WARNING: March 20th Quarterly is MISSING. Download may have failed DTE filter.")

    # C. Check Footprint Baseline (March 5th)
    anchor_date = date(2026, 3, 5)
    anchor = EODOptionSnapshot.objects.filter(date=anchor_date).first()
    if anchor:
        print(f"✅ ANCHOR FOUND: {anchor_date} has {anchor.contracts.count()} contracts.")
    else:
        print(f"❌ ANCHOR MISSING: Cannot calculate 'Change' for footprint without March 5th.")

    # D. Check Scanner Cache
    suggestions = TradeSuggestion.objects.filter(snapshot=latest)
    print(f"🎯 CACHED SUGGESTIONS: {suggestions.count()} entries found for {latest.date}")

    if suggestions.exists():
        for sug in suggestions[:3]:
            print(f"   - Proposal: {sug.strikes} | EV: {sug.edge} | Prob: {sug.probability}")

    print("\n" + "=" * 50)
    if mar_20.count() > 0 and anchor:
        print("🚀 STATUS: GREEN. Data is ready for full View restoration.")
    else:
        print("🛠️ STATUS: RED. Run downloader for March 5th and 6th with 120-DTE scope.")
    print("=" * 50)


if __name__ == "__main__":
    run_diagnostic()