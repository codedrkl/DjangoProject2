from django.db import models
from django.utils import timezone

class OptionChainSnapshot(models.Model):
    product = models.CharField(max_length=10, default="ES")
    date = models.DateField()
    label = models.CharField(max_length=50)
    underlying_price = models.FloatField(default=0.0)
    timestamp = models.DateTimeField(default=timezone.now)

    class Meta:
        unique_together = ('date', 'label', 'product')
        indexes = [
            models.Index(fields=['date', 'label']),
        ]

    def __str__(self):
        return f"{self.product} | {self.date} | {self.label} (Underlying: {self.underlying_price})"


class OptionContract(models.Model):
    OPTION_TYPES = (
        ('C', 'Call'),
        ('P', 'Put'),
    )

    snapshot = models.ForeignKey(OptionChainSnapshot, on_delete=models.CASCADE, related_name='contracts')
    instrument_id = models.BigIntegerField(default=0)
    raw_symbol = models.CharField(max_length=50)
    expiration = models.DateTimeField()
    strike = models.FloatField()
    option_type = models.CharField(max_length=1, choices=OPTION_TYPES)
    settlement = models.FloatField(default=0.0)
    open_interest = models.IntegerField(default=0)
    delta = models.FloatField(default=0.0)
    dte = models.IntegerField()

    class Meta:
        unique_together = ('snapshot', 'raw_symbol')
        indexes = [
            models.Index(fields=['snapshot', 'option_type', 'strike']),
            models.Index(fields=['instrument_id']),
            models.Index(fields=['dte']),
        ]

    def __str__(self):
        return f"{self.raw_symbol} | {self.option_type} | K: {self.strike} | Exp: {self.expiration.strftime('%Y-%m-%d')} | Settle: {self.settlement}"


class TradeSuggestion(models.Model):
    snapshot = models.ForeignKey(OptionChainSnapshot, on_delete=models.CASCADE, related_name='suggestions')
    strategy_type = models.CharField(max_length=100)
    dte = models.IntegerField()
    strikes = models.CharField(max_length=100)
    credit_debit = models.FloatField()
    max_profit = models.FloatField()
    max_loss = models.FloatField()
    probability = models.CharField(max_length=10)
    edge = models.CharField(max_length=20)
    rr_ratio = models.CharField(max_length=20, default="N/A")
    timestamp = models.DateTimeField(auto_now_add=True)
    is_monthly = models.BooleanField(default=False)

    class Meta:
        indexes = [
            models.Index(fields=['snapshot', 'strategy_type']),
        ]

    def __str__(self):
        return f"{self.strategy_type} | {self.strikes} | Edge: {self.edge}"


class FootprintNode(models.Model):
    """Strike-level institutional walls (UI Heatmap)"""
    snapshot = models.ForeignKey(OptionChainSnapshot, on_delete=models.CASCADE, related_name='footprint')
    strike = models.FloatField()
    call_oi = models.IntegerField(default=0)
    put_oi = models.IntegerField(default=0)
    total_oi = models.IntegerField(default=0)
    oi_pct = models.FloatField(default=0.0)

    class Meta:
        indexes = [models.Index(fields=['snapshot', 'strike'])]

    def __str__(self):
        return f"{self.strike} | C: {self.call_oi} | P: {self.put_oi}"


class FootprintBin(models.Model):
    """Temporal Aggregation Bins (Weekly/Monthly/Quarterly)"""
    BIN_CHOICES = (
        ('WEEKLY', '01_WEEKLY'),
        ('MONTHLY', '02_MONTHLY'),
        ('QUARTERLY', '03_QUARTERLY'),
    )
    snapshot = models.ForeignKey(OptionChainSnapshot, on_delete=models.CASCADE, related_name='bins')
    ref_snapshot = models.ForeignKey(OptionChainSnapshot, on_delete=models.SET_NULL, null=True, related_name='ref_bins')
    bin_type = models.CharField(max_length=20, choices=BIN_CHOICES)
    zone = models.CharField(max_length=50) # ATM vs OTM
    notional_delta = models.FloatField(default=0.0)
    growth = models.FloatField(default=0.0)
    volume_filter_met = models.BooleanField(default=False)

    class Meta:
        ordering = ['bin_type', 'zone']