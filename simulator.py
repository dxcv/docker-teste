import sys
import json
import os
from importlib import import_module
import boto3 as aws
from datetime import datetime
import pickle
import list_imports
import os
from backtesting import MarketData, evaluateMult, evaluateTick

s3 = aws.client("s3")


def get_file_dependencies():
    os.system('find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf')
    models = [f"./models/{model}" for model in os.listdir(
        "./models") if model.split(".")[1] == "py"]
    strategies = [f"./strategies/{strategy}" for strategy in os.listdir(
        "./strategies") if strategy.split(".")[1] == "py"]
    files = models + strategies

    dependencies = []
    for file in files:
        dependencies.extend(list_imports.get(file))

    local_modules = ["order", "backtesting", "riskSignal", "event", "strategy"]
    filtered_deps = []
    for dependency in dependencies:
        if dependency not in local_modules and dependency not in filtered_deps:
            filtered_deps.append(dependency.split(".")[0])

    for dependency in list(set(filtered_deps)):
        try:
            os.system(f"pip install {dependency}")
        except:
            print(f"Dep Install Fail: {dependency}")


def list_bucket_files(bucket_name):
    try:
        pages = s3.get_paginator("list_objects").paginate(
            Bucket=bucket_name)
        files = [file["Key"]
                 for page in pages for file in page["Contents"]]
        return files
    except Exception as e:
        print("Error listing bucket:", e)


def get_interval_data(bucket_files, tickers, types, date_limit):
    files_to_look = []
    for ticker in tickers:
        for file_type in types:
            if file_type == "cov_matrix":
                files_to_look.append(file_type)
            else:
                files_to_look.append(f"{file_type}_{ticker}")

    files_to_filter = []
    for file in bucket_files:
        if file.split("/")[3].split(".")[0] in files_to_look:
            files_to_filter.append(file)

    init_date = datetime.strptime(date_limit[0], "%Y-%m-%d")
    final_date = datetime.strptime(date_limit[1], "%Y-%m-%d")

    files_to_download = []
    for file in files_to_filter:
        d = file.split("/")[: 3]
        file_date = datetime.strptime(f"{d[0]}-{d[1]}-{d[2]}", "%Y-%m-%d")
        if file_date >= init_date and file_date <= final_date:
            files_to_download.append(file)

    for file in files_to_download[: 14]:
        print(file)
        file_date = "".join(file.split("/")[: 3])
        file_type = file.split("/")[3].split(".")[0].split("_")[0]

        if file_type == "cov":
            file_path = f"data/{file_date}_COV_MATRIX.parquet"
        else:
            file_type = file.split("/")[3].split(".")[0]
            file_path = f"data/{file_date}_{file_type}.parquet"
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'wb') as f:
            s3.download_fileobj("dell-filtred-data", file, f)


def get_artifacts(bucket_files, model_name, model_run, strategy_name):
    return_files = []
    for file in bucket_files:
        artifact_type = file.split("/")[0]
        if artifact_type == "models":
            model = file.split("/")[1]
            if model == model_name:
                if file.split("/")[2] == f"{model}.py":
                    return_files.append(
                        (file, "/".join(file.split("/")[0::2])))
                if file.split("/")[2] == f"{model.lower()}-{model_run}.pkl":
                    return_files.append(
                        (file, "/".join(file.split("/")[0::2])))
        elif artifact_type == "strategies":
            if file.split("/")[1] == f"{strategy_name}.py":
                return_files.append((file, file))

    for file in return_files:
        os.makedirs(os.path.dirname(file[1]), exist_ok=True)
        with open(file[1], 'wb') as f:
            s3.download_fileobj("dell-artifacts", file[0], f)


filtred_files = list_bucket_files("dell-filtred-data")
bucket_files = list_bucket_files("dell-artifacts")


def load_settings():
    with open("simulator_settings.json", "r") as file:
        settings = json.load(file)
    return settings


relations_list = []
instruments_list = []
for run in load_settings()["settings"]:
    print(run)
    get_artifacts(bucket_files, run["model_class"],
                  run["model"], run["strategy"])

    get_interval_data(filtred_files, run["instruments"], [
        "NEG", "OFER_CPA", "OFER_VDA", "cov_matrix"], run["date_interval"])

    relations_list.append([run["model_class"], run["strategy"], run["model"]])
    instruments_list.append(run["instruments"])

get_file_dependencies()

strategy_list = []
for run in relations_list:
    exec(f"from strategies.{run[1]} import {run[1]}")
    exec(f"strategy_list.append({run[1]}('{run[0]}', '{run[2]}'))")


response = evaluateMult(strategy_list, instruments_list, 10000)

for summary in response:
    print("Summary")
    print(summary)
    # pip install requests
    # import requests
    # URL = "http://maps.googleapis.com/maps/api/geocode/json"
    # PARAMS = {'address':location}
    # r = requests.get(url = URL, params = PARAMS)
