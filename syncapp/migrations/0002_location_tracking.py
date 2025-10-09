from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("syncapp", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="styliaproduct",
            name="location_assigned",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="styliaproduct",
            name="location_last_error",
            field=models.TextField(blank=True),
        ),
    ]
