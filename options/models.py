from django.db import models


class OptionChainSnapshot(models.Model):
    product = models.CharField(max_length=20, default="ES")
    date = models.DateField()
    label = models.CharField(max_length=50)
    timestamp = models.DateTimeField(auto_now_add=True)
    underlying_price = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    state_signature = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        unique_together = ('date', 'label')
        ordering = ['-date', '-timestamp']


class OptionContract(models.Model):
    snapshot = models.ForeignKey(OptionChainSnapshot, related_name='contracts', on_delete=models.CASCADE)
    instrument_id = models.BigIntegerField()
    raw_symbol = models.CharField(max_length=100)
    expiration = models.DateTimeField()
    strike = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    option_type = models.CharField(max_length=1)
    settlement = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    open_interest = models.IntegerField(default=0)
    implied_vol = models.FloatField(null=True)
    delta = models.FloatField(null=True)
    theta = models.FloatField(null=True)
    dte = models.IntegerField()


class FootprintBin(models.Model):
    snapshot = models.ForeignKey(OptionChainSnapshot, related_name='footprints', on_delete=models.CASCADE)
    strike_price = models.DecimalField(max_digits=10, decimal_places=2)
    net_gamma_exposure = models.FloatField(default=0.0)
    oi_density = models.IntegerField(default=0)

    class Meta:
        ordering = ['strike_price']


class TradeOutcome(models.Model):
    """Isolated strategy table to prevent chain contamination."""
    snapshot = models.ForeignKey(OptionChainSnapshot, related_name='outcomes', on_delete=models.CASCADE)
    strategy_name = models.CharField(max_length=100)
    structure = models.CharField(max_length=255)
    bias = models.CharField(max_length=50)
    credit_collected = models.FloatField()
    max_risk = models.FloatField()
    rr_ratio = models.FloatField()


class TradeSuggestion(models.Model):
    snapshot = models.ForeignKey(OptionChainSnapshot, related_name='suggestions', on_delete=models.CASCADE)
    strategy_type = models.CharField(max_length=100)
    dte = models.IntegerField()
    strikes = models.CharField(max_length=200)
    width = models.FloatField()
    credit_debit = models.FloatField()
    max_profit = models.FloatField()
    max_loss = models.FloatField()
    rr_ratio = models.FloatField()
    probability = models.FloatField()
    edge = models.CharField(max_length=100, blank=True)
    is_monthly = models.BooleanField(default=False)

# options/models.py

class IntradayOptionContract(models.Model):
    """
    Physically isolated table for 24/7 Intraday Squeeze analysis.
    Limited to 0-9 DTE with 3-hour timestamped anchors.
    """
    timestamp = models.DateTimeField(db_index=True)
    underlying_price = models.FloatField()
    instrument_id = models.BigIntegerField()
    raw_symbol = models.CharField(max_length=100)
    expiration = models.DateField()
    strike = models.FloatField()
    option_type = models.CharField(max_length=1) # 'C' or 'P'
    settlement = models.FloatField()
    open_interest = models.IntegerField(default=0)
    implied_vol = models.FloatField(null=True)
    delta = models.FloatField(null=True)
    theta = models.FloatField(null=True)
    dte = models.IntegerField()

    class Meta:
        indexes = [
            models.Index(fields=['timestamp', 'dte']),
        ]
        ordering = ['-timestamp', 'strike']

    def __str__(self):
        return f"{self.timestamp} | {self.raw_symbol} | Spot: {self.underlying_price}"