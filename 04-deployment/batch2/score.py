#!/usr/bin/env python
# coding: utf-8

import sys
import uuid
from datetime import datetime
from typing import Optional

import mlflow
import pandas as pd
from dateutil.relativedelta import relativedelta
from prefect import flow, task
from prefect.context import get_run_context
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_extraction import DictVectorizer
from sklearn.metrics import mean_squared_error
from sklearn.pipeline import make_pipeline


def generate_uuids(n):
    uuids = []
    for i in range(n):
        uuids.append(str(uuid.uuid4()))
    return uuids


def read_dataframe(filename: str):
    df = pd.read_parquet(filename)

    df["duration"] = df.lpep_dropoff_datetime - df.lpep_pickup_datetime
    df.duration = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)]
    df["ride_id"] = generate_uuids(len(df))

    return df


def prepare_dictionaries(df: pd.DataFrame):
    categorical = ["PULocationID", "DOLocationID"]
    df[categorical] = df[categorical].astype(str)

    df["PU_DO"] = df["PULocationID"] + "_" + df["DOLocationID"]

    categorical = ["PU_DO"]
    numerical = ["trip_distance"]

    dicts = df[categorical + numerical].to_dict(orient="records")
    return dicts


def load_model(run_id):
    logged_model = f"/Users/michaelaltork/Documents/Coding Stuff/mlops-zoomcamp/04-deployment/web-service2/mlruns/1/{run_id}/artifacts/model"
    model = mlflow.pyfunc.load_model(logged_model)
    return model


def apply_model(input_file, run_id, output_file):
    print(f"reading data from {input_file}....")
    df = read_dataframe(input_file)
    dicts = prepare_dictionaries(df)

    print(f"loading the model with RUN_ID: {run_id}....")
    model = load_model(run_id)

    print("applying the model...")
    y_pred = model.predict(dicts)

    print(f"saving the results to {output_file}")
    df_result = pd.DataFrame()
    df_result["ride_id"] = df["ride_id"]
    df_result["lpep_pickup_datetime"] = df["lpep_pickup_datetime"]
    df_result["PULocationID"] = df["PULocationID"]
    df_result["DOLocationID"] = df["DOLocationID"]
    df_result["actual_duration"] = df["duration"]
    df_result["predicted_duration"] = y_pred
    df_result["diff"] = df_result["actual_duration"] - df_result["predicted_duration"]
    df_result["model_version"] = run_id

    df_result.to_parquet(output_file, index=False)


@flow
def ride_duration_prediction(
    taxi_type: str, run_id: str, run_date: Optional[datetime] = None
):
    if run_date is None:
        ctx = get_run_context()
        run_date = ctx.flow_run.expected_start_time

    print(run_date)
    prev_month = run_date - relativedelta(months=1)
    print(prev_month, prev_month.month, prev_month.year)
    year = prev_month.year
    month = prev_month.month

    output_file = f"output/{taxi_type}/tripdata_{year:04d}-{month:02d}.parquet"
    input_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"

    apply_model(input_file, run_id, output_file)


def run():
    taxi_type = sys.argv[1]  # "green"
    year = int(sys.argv[2])  # 2021
    month = int(sys.argv[3])  # 3
    run_id = sys.argv[4]  # "fcd3850afce44e29a7e50a4945a1fda5"

    ride_duration_prediction(
        taxi_type=taxi_type,
        run_id=run_id,
        run_date=datetime(year=year, month=month, day=18),
    )


if __name__ == "__main__":
    run()
