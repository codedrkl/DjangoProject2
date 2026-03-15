from django.core.management.base import BaseCommand
from django.db.models import Sum, F, Q
from options.models import OptionChainSnapshot, FootprintBin


class Command(BaseCommand):
    help = 'Aggregates Institutional Footprint vs Reference Friday with Strike Granularity'

    def add_arguments(self, parser):
        parser.add_argument('--ref-date', type=str, help='YYYY-MM-DD of the anchor Friday', default='2026-03-06')

    def handle(self, *args, **options):
        ref_date = options['ref_date']
        curr = OptionChainSnapshot.objects.order_by('-timestamp').first()
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
            curr_data_all = curr.contracts.filter(dte__range=(d_min, d_max)).annotate(
                notional=F('strike') * F('open_interest') * ES_MULTIPLIER
            ).filter(notional__gte=INSTITUTIONAL_THRESHOLD)

            # Split into ATM and OTM zones
            for zone in ['ATM', 'OTM']:
                spot = float(curr.underlying_price)
                zone_filter = Q(strike__range=(spot * 0.98, spot * 1.02)) if zone == 'ATM' else ~Q(
                    strike__range=(spot * 0.98, spot * 1.02))

                curr_zone_qs = curr_data_all.filter(zone_filter)

                # 1. Calculate Aggregate Weights
                curr_zone_weight = curr_zone_qs.aggregate(
                    total_delta_weight=Sum(F('strike') * F('open_interest') * F('delta'))
                )['total_delta_weight'] or 0.0

                # 2. Extract Top 3 Granular Walls (Strikes)
                # We sort by absolute notional to find the biggest "Pins" or "Walls"
                top_strikes_qs = curr_zone_qs.order_by('-notional')[:3]
                walls_metadata = []
                for contract in top_strikes_qs:
                    walls_metadata.append({
                        'strike': int(contract.strike),
                        'type': contract.option_type,
                        'oi': contract.open_interest,
                        'notional_m': round((contract.strike * contract.open_interest * ES_MULTIPLIER) / 1_000_000, 1)
                    })

                # 3. Reference Friday Comparison
                ref_zone_weight = ref.contracts.filter(dte__range=(d_min, d_max)).filter(zone_filter).annotate(
                    notional=F('strike') * F('open_interest') * ES_MULTIPLIER
                ).filter(notional__gte=INSTITUTIONAL_THRESHOLD).aggregate(
                    total_delta_weight=Sum(F('strike') * F('open_interest') * F('delta'))
                )['total_delta_weight'] or 0.0

                growth = curr_zone_weight - ref_zone_weight

                FootprintBin.objects.create(
                    snapshot=curr,
                    ref_snapshot=ref,
                    bin_type=code,
                    zone=zone,
                    notional_delta=curr_zone_weight / 1_000_000,
                    growth=growth / 1_000_000,
                    volume_filter_met=True,
                    top_walls=walls_metadata  # Inject the granular finesse
                )

        self.stdout.write(self.style.SUCCESS("✅ Institutional Footprint + Granular Walls Synced."))