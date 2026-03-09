from django.db import models

# options/models.py
from django.utils import timezone

class EODOptionSnapshot(models.Model):
    product = models.CharField(max_length=10, default='ES', db_index=True)
    date = models.DateField(db_index=True)
    label = models.CharField(max_length=50, default='EOD', db_index=True)
    # Adding null=True temporarily allows the migration to pass without a default
    timestamp = models.DateTimeField(auto_now_add=True, db_index=True, null=True)
    underlying_settlement = models.DecimalField(max_digits=12, decimal_places=4, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-date', '-timestamp']

    def __str__(self):
        return f"{self.date} | {self.label} | {self.product}"


class OptionContract(models.Model):
    snapshot = models.ForeignKey(EODOptionSnapshot, on_delete=models.CASCADE, related_name='contracts')
    raw_symbol = models.CharField(max_length=50, db_index=True)
    expiration = models.DateTimeField(db_index=True)
    strike = models.DecimalField(max_digits=10, decimal_places=2, db_index=True)
    option_type = models.CharField(max_length=1, choices=[('C', 'Call'), ('P', 'Put')])

    settlement = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    open_interest = models.BigIntegerField(default=0)

    # Greeks for Sentinel Alpha
    implied_vol = models.FloatField(null=True, blank=True)
    delta = models.FloatField(null=True, blank=True)
    theta = models.FloatField(null=True, blank=True)
    dte = models.IntegerField(db_index=True)

    class Meta:
        unique_together = ('snapshot', 'raw_symbol')
        indexes = [
            models.Index(fields=['snapshot', 'expiration', 'option_type']),
        ]


class TradeSuggestion(models.Model):
    snapshot = models.ForeignKey(EODOptionSnapshot, on_delete=models.CASCADE)
    strategy_type = models.CharField(max_length=50)
    dte = models.IntegerField()
    strikes = models.CharField(max_length=100)
    credit_debit = models.DecimalField(max_digits=12, decimal_places=2)
    max_profit = models.DecimalField(max_digits=12, decimal_places=2)
    max_loss = models.DecimalField(max_digits=12, decimal_places=2)
    probability = models.CharField(max_length=10)
    edge = models.CharField(max_length=10)
    rr_ratio = models.CharField(max_length=20)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']