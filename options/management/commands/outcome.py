import math
from django.core.management.base import BaseCommand
from options.models import OptionChainSnapshot, TradeOutcome


class Command(BaseCommand):
    help = 'JATS™ Outcome Engine - Isolated Strategy Generation'

    def handle(self, *args, **options):
        self.stdout.write("🛠️ Generating Isolated Trade Outcomes...")

        # Get the latest clean snapshot from the new downloader
        snapshot = OptionChainSnapshot.objects.order_by('-timestamp').first()
        if not snapshot:
            self.stdout.write(self.style.ERROR("❌ No snapshot found. Run downloader first."))
            return

        # 1. Clear ONLY the outcomes for this snapshot
        TradeOutcome.objects.filter(snapshot=snapshot).delete()

        # 2. Hard-coded "Institutional Guardrail" Trade (The 6670/6790 Iron Condor)
        # This ensures you always have your preferred trade visible regardless of scanner bugs
        TradeOutcome.objects.create(
            snapshot=snapshot,
            strategy_name="Institutional Iron Condor",
            structure="6670/6650P - 6790/6810C",
            bias="Neutral/Call Credit Preferred",
            credit_collected=7.25,
            max_risk=12.75,
            rr_ratio=0.57
        )

        self.stdout.write(
            self.style.SUCCESS(f"✅ Outcomes frozen for {snapshot.label} at Spot: {snapshot.underlying_price}"))