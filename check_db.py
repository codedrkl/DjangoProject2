import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'es_option_chain.settings')
django.setup()

from options.models import OptionChainSnapshot

def run_timeline_audit():
    print("\n" + "="*75)
    print("📅 TIMELINE AUDIT: DTE + CYCLE VERIFICATION v2")
    print("="*75 + "\n")

    snapshot = OptionChainSnapshot.objects.order_by('-date', '-timestamp').first()
    if not snapshot:
        print("❌ No snapshots found. Run: python manage.py download_es_eod")
        return

    contracts = snapshot.contracts.all().order_by('expiration')
    if not contracts.exists():
        print("❌ No contracts in latest snapshot.")
        return

    first_exp = contracts.first().expiration.date()
    last_exp = contracts.last().expiration.date()
    max_dte = (last_exp - snapshot.date).days

    print(f"📡 Snapshot Date : {snapshot.date} | Label: {snapshot.label}")
    print(f"⏭️  First Expiry  : {first_exp} ({contracts.first().dte} DTE)")
    print(f"🏁  Last Expiry   : {last_exp} ({contracts.last().dte} DTE)")
    print(f"📏 MAX DTE SPAN   : {max_dte} days")

    exps = sorted({c.expiration.date() for c in contracts})
    weeklies = [d for d in exps if d.weekday() == 4 and 1 <= (d.day // 7) <= 3]
    monthlies = [d for d in exps if d.weekday() == 4 and 15 <= d.day <= 21]

    print(f"🗓️  UNIQUE EXPIRATIONS: {len(exps)}")
    print(f"   Weeklies detected : {len(weeklies)}")
    print(f"   Monthlies detected: {len(monthlies)}")

    if max_dte >= 115:
        print(f"\n✅ EXCELLENT: {max_dte}+ DTE span → Quarterly visibility ACTIVE")
    elif max_dte >= 45:
        print(f"\n⚠️  PARTIAL: {max_dte} DTE → Monthly ok, Quarterly missing")
    else:
        print(f"\n❌ INSUFFICIENT: Only {max_dte} DTE. Run EOD downloader again.")

    print("\n" + "="*75)

if __name__ == "__main__":
    run_timeline_audit()