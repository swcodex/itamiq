from django.db import models

class Table(models.Model):
    name = models.CharField(max_length=255, unique=True)
    schema = models.CharField(max_length=255, default='public')

    def __str__(self):
        return f"{self.schema}.{self.name}"

class Column(models.Model):
    name = models.CharField(max_length=255)
    table = models.ForeignKey(Table, on_delete=models.CASCADE, related_name='columns')
    data_type = models.CharField(max_length=50)

    def __str__(self):
        return f"{self.table.name}.{self.name}"

class Relationship(models.Model):
    from_table = models.ForeignKey(Table, on_delete=models.CASCADE, related_name='outgoing_relationships')
    from_column = models.ForeignKey(Column, on_delete=models.CASCADE, related_name='outgoing_relationships')
    to_table = models.ForeignKey(Table, on_delete=models.CASCADE, related_name='incoming_relationships')
    to_column = models.ForeignKey(Column, on_delete=models.CASCADE, related_name='incoming_relationships')

    def __str__(self):
        return f"{self.from_table.name}.{self.from_column.name} -> {self.to_table.name}.{self.to_column.name}"