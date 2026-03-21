import math
from statistics import NormalDist

class Black76Engine:
    """
    Centralized Black-76 Professional Engine (v2.1)
    Single source of truth for EOD + Intraday + calc_greeks.
    Robust NaN/edge-case protection + stable IV solver.
    """
    def __init__(self, risk_free_rate: float = 0.053):
        self.r = risk_free_rate
        self.norm = NormalDist(mu=0.0, sigma=1.0)
        self.N = self.norm.cdf

    def _n(self, x: float) -> float:
        return math.exp(-0.5 * x ** 2) / math.sqrt(2 * math.pi)

    def _d1_d2(self, F: float, K: float, T: float, sigma: float):
        T = max(T, 1e-8)
        sigma = max(sigma, 1e-6)
        d1 = (math.log(F / K) + (0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        return d1, d2

    def price(self, F: float, K: float, T: float, sigma: float, opt_type: str) -> float:
        if T <= 0 or sigma <= 0:
            return max(0.0, F - K) if opt_type.upper() == 'C' else max(0.0, K - F)
        d1, d2 = self._d1_d2(F, K, T, sigma)
        df = math.exp(-self.r * T)
        if opt_type.upper() == 'C':
            return df * (F * self.N(d1) - K * self.N(d2))
        return df * (K * self.N(-d2) - F * self.N(-d1))

    def vega(self, F: float, K: float, T: float, sigma: float) -> float:
        if T <= 0 or sigma <= 0: return 0.0
        d1, _ = self._d1_d2(F, K, T, sigma)
        return F * math.exp(-self.r * T) * self._n(d1) * math.sqrt(T)

    def implied_volatility(self, target_price: float, F: float, K: float, T: float, opt_type: str) -> float:
        intrinsic = max(0.0, F - K) if opt_type.upper() == 'C' else max(0.0, K - F)
        if target_price <= intrinsic + 0.05:
            return 0.15
        sigma = 0.25
        for _ in range(40):
            p_est = self.price(F, K, T, sigma, opt_type)
            diff = p_est - target_price
            if abs(diff) < 1e-4: return sigma
            v = self.vega(F, K, T, sigma)
            if v < 1e-7: break
            sigma = max(0.01, min(4.0, sigma - (diff / v)))
        return sigma

    def delta(self, F: float, K: float, T: float, sigma: float, opt_type: str) -> float:
        if T <= 0:
            return 1.0 if (F > K and opt_type.upper() == 'C') or (F < K and opt_type.upper() == 'P') else 0.0
        d1, _ = self._d1_d2(F, K, T, sigma)
        df = math.exp(-self.r * T)
        return df * self.N(d1) if opt_type.upper() == 'C' else df * (self.N(d1) - 1.0)