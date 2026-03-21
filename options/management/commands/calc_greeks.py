import math
from statistics import NormalDist
from django.core.management.base import BaseCommand
from options.models import OptionChainSnapshot, OptionContract
from options.engines.black76 import Black76Engine   # ← NEW
# ... rest of your command unchanged


class Command(BaseCommand):
    help = 'In-place Black-76 Greek synthesis for existing matrix data'

    def handle(self, *args, **options):
        self.stdout.write("⚙️ Initiating Local Black-76 Matrix Synthesis...")

        snapshot = OptionChainSnapshot.objects.order_by('-timestamp').first()
        if not snapshot:
            self.stdout.write(self.style.ERROR("Matrix empty. Aborting."))
            return

        engine = Black76Engine(risk_free_rate=0.053)
        F_settle = float(snapshot.underlying_price)

        contracts = list(snapshot.contracts.all())
        mutated_count = 0

        for c in contracts:
            if c.strike > 0 and c.settlement > 0.0:
                T = max(c.dte, 0.001) / 365.0
                iv = engine.implied_volatility(c.settlement, F_settle, c.strike, T, c.option_type)
                c.delta = engine.delta(F_settle, c.strike, T, iv, c.option_type)
                mutated_count += 1
            else:
                c.delta = 0.0

        OptionContract.objects.bulk_update(contracts, ['delta'], batch_size=2000)

        self.stdout.write(
            self.style.SUCCESS(f"✅ Synthesis Complete: {mutated_count} Deltas injected into local state."))