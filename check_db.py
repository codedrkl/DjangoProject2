import os
import django
import pandas as pd
from datetime import date, timedelta

# 1. Setup Django Environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'es_option_chain.settings')
django.setup()

from options.models import EODOptionSnapshot, OptionContract

def run_timeline_audit():
    print("\n" + "="*65)
    print("📅 TIMELINE AUDIT: DTE VERIFICATION")
    print("="*65 + "\n")

    snapshot = EODOptionSnapshot.objects.order_by('-date').first()
    if not snapshot:
        print("❌ No snapshots found.")
        return

    contracts = snapshot.contracts.all().order_by('expiration')
    if not contracts.exists():
        print("❌ No contracts found for this snapshot.")
        return

    # A. Calculate actual DTE Range
    first_exp = contracts.first().expiration.date()
    last_exp = contracts.last().expiration.date()
    max_dte = (last_exp - snapshot.date).days

    print(f"📡 Snapshot Date: {snapshot.date}")
    print(f"⏭️  Earliest Expiry: {first_exp}")
    print(f"🏁 Furthest Expiry: {last_exp}")
    print(f"📏 TOTAL DTE SPAN: {max_dte} days")

    # B. Count distinct expiration cycles
    exp_counts = contracts.values('expiration__date').distinct().count()
    print(f"🗓️  DISTINCT EXPIRATIONS: {exp_counts} unique dates")

    # C. Verification for $100k Goal
    if max_dte >= 115:
        print(f"\n✅ SUCCESS: Database spans {max_dte} days. The 120-DTE window is ACTIVE.")
    else:
        print(f"\n❌ FAILURE: Database only spans {max_dte} days. You are blind to the Quarterly cycle.")

    print("\n" + "="*65)

if __name__ == "__main__":
    run_timeline_audit()