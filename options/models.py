from django.db import models


class EODOptionSnapshot(models.Model):
    product = models.CharField(max_length=10, default='ES')
    date = models.DateField(db_index=True)
    underlying_settlement = models.DecimalField(max_digits=12, decimal_places=4, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('product', 'date')
        ordering = ['-date']


class OptionContract(models.Model):
    snapshot = models.ForeignKey(EODOptionSnapshot, on_delete=models.CASCADE, related_name='contracts')

    raw_symbol = models.CharField(max_length=32)
    underlying = models.CharField(max_length=20)
    expiration = models.DateTimeField(db_index=True)
    strike = models.DecimalField(max_digits=10, decimal_places=2, db_index=True)
    option_type = models.CharField(max_length=1, choices=[('C', 'Call'), ('P', 'Put')])

    settlement = models.DecimalField(max_digits=12, decimal_places=4, null=True)
    volume = models.BigIntegerField(default=0)
    open_interest = models.BigIntegerField(default=0)

    # Black-76 Greeks
    implied_vol = models.DecimalField(max_digits=8, decimal_places=4, null=True)
    delta = models.DecimalField(max_digits=6, decimal_places=4, null=True)
    gamma = models.DecimalField(max_digits=8, decimal_places=6, null=True)
    theta = models.DecimalField(max_digits=10, decimal_places=6, null=True)  # daily theta

    dte = models.IntegerField()

    class Meta:
        unique_together = ('snapshot', 'raw_symbol')
        ordering = ['expiration', 'strike']