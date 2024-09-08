from django.db import models

# Create your models here.
from django.db import models

# Create your models here.
        
# Don't remove this mark
### ### Below code is Generated ### ###

class Sales(models.Model):
	ID = models.AutoField(primary_key=True)
	PurchaseID = models.TextField(blank=True, null=True)
	ItemName = models.TextField(blank=True, null=True)
	BuyerName = models.TextField(blank=True, null=True)
	PurchaseEmail = models.TextField(blank=True, null=True)
	BuyerEmail = models.TextField(blank=True, null=True)
	Donotcontact = models.IntegerField(blank=True, null=True)
	PurchaseDate = models.TextField(blank=True, null=True)
	PurchaseTimeUTCtimezone = models.TextField(blank=True, null=True)
	SubtotalUSD = models.FloatField(blank=True, null=True)
	TaxesUSD = models.FloatField(blank=True, null=True)
	ShippingUSD = models.FloatField(blank=True, null=True)
	SalePriceUSD = models.FloatField(blank=True, null=True)
	GumroadFeeUSD = models.FloatField(blank=True, null=True)
	NetTotalUSD = models.FloatField(blank=True, null=True)
	TaxIncludedinPrice = models.FloatField(blank=True, null=True)
	StreetAddress = models.FloatField(blank=True, null=True)
	City = models.FloatField(blank=True, null=True)
	ZipCode = models.TextField(blank=True, null=True)
	State = models.TextField(blank=True, null=True)
	Country = models.TextField(blank=True, null=True)
	Referrer = models.TextField(blank=True, null=True)
	Refunded = models.IntegerField(blank=True, null=True)
	PartialRefundUSD = models.FloatField(blank=True, null=True)
	FullyRefunded = models.IntegerField(blank=True, null=True)
	Disputed = models.IntegerField(blank=True, null=True)
	DisputeWon = models.IntegerField(blank=True, null=True)
	Variants = models.FloatField(blank=True, null=True)
	DiscountCode = models.FloatField(blank=True, null=True)
	RecurringCharge = models.IntegerField(blank=True, null=True)
	Preorderauthorization = models.IntegerField(blank=True, null=True)
	ProductID = models.TextField(blank=True, null=True)
	OrderNumber = models.IntegerField(blank=True, null=True)
	PreorderauthorizationtimeUTCtimezone = models.FloatField(blank=True, null=True)
	CustomFields = models.TextField(blank=True, null=True)
	ItemPriceUSD = models.FloatField(blank=True, null=True)
	VariantsPriceUSD = models.FloatField(blank=True, null=True)
	ImportedCustomer = models.IntegerField(blank=True, null=True)
	GifteeEmail = models.TextField(blank=True, null=True)
	SKUID = models.FloatField(blank=True, null=True)
	Quantity = models.IntegerField(blank=True, null=True)
	Recurrence = models.FloatField(blank=True, null=True)
	Affiliate = models.FloatField(blank=True, null=True)
	AffiliatecommissionUSD = models.FloatField(blank=True, null=True)
	Discover = models.IntegerField(blank=True, null=True)
	SubscriptionEndedDate = models.FloatField(blank=True, null=True)
	Rating = models.FloatField(blank=True, null=True)
	LicenseKey = models.FloatField(blank=True, null=True)
	PaymentType = models.FloatField(blank=True, null=True)
	PayPalTransactionID = models.FloatField(blank=True, null=True)
