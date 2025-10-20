from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("syncapp", "0002_location_tracking"),
    ]

    operations = [
        migrations.AddField(
            model_name="styliaproduct",
            name="push_last_error",
            field=models.TextField(blank=True),
        ),
        migrations.AddField(
            model_name="styliaproduct",
            name="push_error_count",
            field=models.IntegerField(default=0),
        ),
    ]
