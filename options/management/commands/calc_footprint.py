import math
from decimal import Decimal
from django.core.management.base import BaseCommand
from django.db.models import F, Q, FloatField, Sum, ExpressionWrapper
from options.models import OptionChainSnapshot, FootprintBin, IntradayOptionContract


class Command(BaseCommand):
    help = 'JATS Footprint Engine v1.6 - NaN Guard'

    def to_safe_decimal(self, value):
        try:
            f_val = float(value)
            if not math.isfinite(f_val): return None
            return Decimal(str(round(f_val, 2)))
        except (ValueError, TypeError):
            return None

    def handle(self, *args, **options):
        timestamps = IntradayOptionContract.objects.values_list('timestamp', flat=True).distinct().order_by(
            '-timestamp')[:2]
        if len(timestamps) < 2:
            self.stdout.write("Need at least two intraday snapshots for delta-footprint.")
            return
        current_data = IntradayOptionContract.objects.filter(timestamp=timestamps[0])
        prior_data = IntradayOptionContract.objects.filter(timestamp=timestamps[1])

        self.stdout.write("👣 Initializing JATS Footprint lockdown...")
        snapshot = OptionChainSnapshot.objects.order_by('-date', '-timestamp').first()
        if not snapshot: return

        FootprintBin.objects.filter(snapshot=snapshot).delete()

        contracts = snapshot.contracts.annotate(
            gex_contribution=ExpressionWrapper(
                F('open_interest') * F('settlement') * 50.0,
                output_field=FloatField()
            )
        )

        strikes_qs = contracts.values('strike').annotate(
            total_oi=Sum('open_interest'),
            call_gex=Sum('gex_contribution', filter=Q(option_type='C')),
            put_gex=Sum('gex_contribution', filter=Q(option_type='P'))
        ).order_by('strike')

        bins_to_create = []
        for s in strikes_qs:
            clean_strike = self.to_safe_decimal(s['strike'])
            if clean_strike is None: continue

            c_gex = self.to_safe_decimal(s['call_gex']) or Decimal('0.00')
            p_gex = self.to_safe_decimal(s['put_gex']) or Decimal('0.00')

            bins_to_create.append(FootprintBin(
                snapshot=snapshot,
                strike_price=clean_strike,
                net_gamma_exposure=float(c_gex - p_gex),
                oi_density=s['total_oi'] or 0
            ))

        if bins_to_create:
            FootprintBin.objects.bulk_create(bins_to_create)
            self.stdout.write(self.style.SUCCESS(f"✅ Footprint Optimized: {len(bins_to_create)} bins created."))