from django.core.management.base import BaseCommand
from django.db.models import Sum, F, Q
from options.models import OptionChainSnapshot, FootprintBin


class Command(BaseCommand):
    help = 'Aggregates Institutional Footprint vs Reference Friday'

    def add_arguments(self, parser):
        parser.add_argument('--ref-date', type=str, help='YYYY-MM-DD of the anchor Friday', default='2026-03-06')

    def handle(self, *args, **options):
        ref_date = options['ref_date']
        curr = OptionChainSnapshot.objects.order_by('-timestamp').first()
        # Find the specific Friday anchor
        ref = OptionChainSnapshot.objects.filter(date=ref_date, label='EOD').first()

        if not curr or not ref:
            self.stdout.write(
                self.style.ERROR(f"❌ Missing data. Curr: {curr.date if curr else 'None'}, Ref: {ref_date}"))
            return

        self.stdout.write(f"📊 Analyzing Footprint: {curr.date} vs Anchor {ref.date}")
        FootprintBin.objects.filter(snapshot=curr).delete()

        bins = [
            ('WEEKLY', 0, 7),
            ('MONTHLY', 8, 45),
            ('QUARTERLY', 46, 120)
        ]

        INSTITUTIONAL_THRESHOLD = 500_000  # $500k Notional minimum
        ES_MULTIPLIER = 50

        for code, d_min, d_max in bins:
            # Aggregate current institutional flow
            # Using Absolute Delta to measure "Total Exposure Weight"
            curr_data = curr.contracts.filter(dte__range=(d_min, d_max)).annotate(
                notional=F('strike') * F('open_interest') * ES_MULTIPLIER
            ).filter(notional__gte=INSTITUTIONAL_THRESHOLD)

            # Split into ATM and OTM zones
            for zone in ['ATM', 'OTM']:
                spot = float(curr.underlying_price)
                zone_filter = Q(strike__range=(spot * 0.98, spot * 1.02)) if zone == 'ATM' else ~Q(
                    strike__range=(spot * 0.98, spot * 1.02))

                curr_zone = curr_data.filter(zone_filter).aggregate(
                    total_delta_weight=Sum(F('strike') * F('open_interest') * F('delta'))
                )['total_delta_weight'] or 0.0

                # Compare against Reference Friday
                ref_zone = ref.contracts.filter(dte__range=(d_min, d_max)).filter(zone_filter).annotate(
                    notional=F('strike') * F('open_interest') * ES_MULTIPLIER
                ).filter(notional__gte=INSTITUTIONAL_THRESHOLD).aggregate(
                    total_delta_weight=Sum(F('strike') * F('open_interest') * F('delta'))
                )['total_delta_weight'] or 0.0

                growth = curr_zone - ref_zone

                FootprintBin.objects.create(
                    snapshot=curr,
                    ref_snapshot=ref,
                    bin_type=code,
                    zone=zone,
                    notional_delta=curr_zone / 1_000_000,
                    growth=growth / 1_000_000,
                    volume_filter_met=True
                )

        self.stdout.write(self.style.SUCCESS("✅ Institutional Footprint Synced vs Anchor."))