import os

import mlflow
from flask import Flask, jsonify, request

RUN_ID = os.getenv("RUN_ID")
# RUN_ID = "fcd3850afce44e29a7e50a4945a1fda5"
# TRACKING_URI = "sqlite:///mlflow.db"

# mlflow.set_tracking_uri(TRACKING_URI)
# mlflow.set_experiment("green-taxi-duration")

logged_model = f"/Users/michaelaltork/Documents/Coding Stuff/mlops-zoomcamp/04-deployment/web-service2/mlruns/1/{RUN_ID}/artifacts/model"

# Load model as a PyFuncModel.
model = mlflow.pyfunc.load_model(logged_model)


def prepare_features(ride):
    features = {}
    features["PU_DO"] = "%s_%s" % (ride["PULocationID"], ride["DOLocationID"])
    features["trip_distance"] = ride["trip_distance"]
    return features


def predict(features):
    # X = dv.transform(features) not needed as model is a pipeline of cv & rf
    preds = model.predict(features)
    return float(preds[0])


app = Flask("duration-prediction")


@app.route("/predict", methods=["POST"])
def predict_endpoint():
    ride = request.get_json()
    features = prepare_features(ride)
    pred = predict(features)

    result = {"duration": pred, "model_version": RUN_ID}

    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=9696)
