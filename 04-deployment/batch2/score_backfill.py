from datetime import datetime

import score
from dateutil.relativedelta import relativedelta
from prefect import flow


@flow
def ride_duration_prediction_backfill():
    start_date = datetime(year=2021, month=3, day=1)
    end_date = datetime(year=2022, month=3, day=1)

    d = start_date

    while d <= end_date:
        score.ride_duration_prediction("green", "fcd3850afce44e29a7e50a4945a1fda5", d)
        d = d + relativedelta(months=1)


if __name__ == "__main__":
    ride_duration_prediction_backfill()
