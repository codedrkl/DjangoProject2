import math
from statistics import NormalDist
from django.core.management.base import BaseCommand
from options.models import OptionChainSnapshot, OptionContract


class Black76Engine:
    """
    Standard Black-76 European Pricing Model for Futures Options.
    """

    def __init__(self, risk_free_rate=0.053):
        self.r = risk_free_rate
        self.norm = NormalDist(mu=0.0, sigma=1.0)
        self.N = self.norm.cdf

    def _n(self, x: float) -> float:
        return math.exp(-0.5 * x ** 2) / math.sqrt(2 * math.pi)

    def _d1_d2(self, F: float, K: float, T: float, sigma: float):
        d1 = (math.log(F / K) + (0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        return d1, d2

    def price(self, F: float, K: float, T: float, sigma: float, opt_type: str) -> float:
        if T <= 0 or sigma <= 0: return 0.0
        d1, d2 = self._d1_d2(F, K, T, sigma)
        discount = math.exp(-self.r * T)
        if opt_type == 'C':
            return discount * (F * self.N(d1) - K * self.N(d2))
        else:
            return discount * (K * self.N(-d2) - F * self.N(-d1))

    def vega(self, F: float, K: float, T: float, sigma: float) -> float:
        if T <= 0 or sigma <= 0: return 0.0
        d1, _ = self._d1_d2(F, K, T, sigma)
        return F * math.exp(-self.r * T) * self._n(d1) * math.sqrt(T)

    def implied_volatility(self, target_price: float, F: float, K: float, T: float, opt_type: str) -> float:
        intrinsic = max(0.0, F - K) if opt_type == 'C' else max(0.0, K - F)
        if target_price <= intrinsic + 0.05:
            return 0.001

        MAX_ITER = 50
        TOL = 1e-4
        sigma = 0.30
        MAX_SIGMA = 5.0  # 500% IV hard cap to prevent float overflow

        for _ in range(MAX_ITER):
            price_est = self.price(F, K, T, sigma, opt_type)
            diff = price_est - target_price

            if abs(diff) < TOL:
                return sigma

            v = self.vega(F, K, T, sigma)

            # Singularity bypass: If Vega is essentially zero, price is insensitive to IV.
            if v < 1e-6:
                break

            step = diff / v

            # Clamp the gradient step to prevent wild oscillatory divergence
            step = max(-0.5, min(0.5, step))

            sigma = sigma - step

            # Enforce absolute domain bounds
            if sigma <= 0.001:
                sigma = 0.001
            elif sigma > MAX_SIGMA:
                sigma = MAX_SIGMA

        return sigma

    def delta(self, F: float, K: float, T: float, sigma: float, opt_type: str) -> float:
        if T <= 0:
            if opt_type == 'C': return 1.0 if F > K else 0.0
            if opt_type == 'P': return -1.0 if F < K else 0.0

        if sigma <= 0.001:
            if opt_type == 'C': return 1.0 if F >= K else 0.0
            if opt_type == 'P': return -1.0 if F <= K else 0.0

        d1, _ = self._d1_d2(F, K, T, sigma)
        discount = math.exp(-self.r * T)

        if opt_type == 'C':
            return discount * self.N(d1)
        else:
            return discount * (self.N(d1) - 1.0)


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