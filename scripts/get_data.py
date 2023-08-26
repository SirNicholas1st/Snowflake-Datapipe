import os
import configparser
import requests
import json
import time
import csv
import boto3
import logging
import shutil
from datetime import datetime


global current_dir
current_dir = os.path.dirname(os.path.realpath(__file__))

# actual config path to be used in the future.
# this path would be actual target path, but for testing purposes playground used.
global config_path
config_path = f"{current_dir}\\config\\config.ini"


def get_config_data(config_path: str):
    parser = configparser.ConfigParser()
    parser.read(config_path)

    global latitudes
    latitudes = parser["Location-info"]["latitudes"].split(",")
    global longitudes
    longitudes = parser["Location-info"]["longitudes"].split(",")
    global location_names
    location_names = parser["Location-info"]["names"].split(",")
    global customer_id
    customer_id = parser["Customer-info"]["customer_id"]
    global dump_path
    dump_path = parser["Structure-info"]["dump_folder"]
    global data_path
    data_path = parser["Structure-info"]["data_folder"]
    global log_path
    log_path = parser["Structure-info"]["log_folder"]
    
    return None


def get_json_data(latitude, longitude, location_name):
    latitude, longitude, location_name = latitude.strip(), longitude.strip(), location_name.strip()
    url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
    json_data = requests.get(url).json()

    save_json(data_path, json_data, location_name)
    
    return None

def save_json(path: str, json_data: json, location_name: str):

    
    timestamp = str(time.time()).replace(".", "-")
    filename = f"{timestamp}_{customer_id}_{location_name}.json"
    path = f"{path}\\{filename}"
    with open(path, "w+") as file_out:
        json.dump(json_data, file_out)
    
    upload_json_to_s3(path, filename)
    return None
    

def upload_json_to_s3(path: str, filename: str):
    fulltxt = open("open-meteo-customer_accessKeys.csv").read()
    secret_key = fulltxt.split(",")[-1].strip()
    access_key = fulltxt.split(",")[-2].split("\n")[-1]
    
    s3_client = boto3.client("s3",
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        region_name = "eu-north-1")
    
    
    with open(path, "r") as f:
        try:
            s3_client.upload_file(f.name, Bucket = "open-meteo-source-bucket", Key = filename)
            logging.info(f"{datetime.now()}: {f.name} file upload succeeded, deleting file.")
            f.close()
            os.remove(f.name)
            logging.info(f"{datetime.now()}: {f.name} file deleted from temp.")
        except Exception as e:
            logging.info(f"{datetime.now()}: {f.name} file upload failed, moving to dump. Error: {e}")
            f.close()
            shutil.move(f.name, dump_path)
            logging.info(f"{datetime.now()}: {f.name} file moved to dump.")
    

get_config_data(config_path)

# Setting up logging
log_file_path = log_path + r"\logs.log"

logging.basicConfig(filename =  log_file_path, level = logging.INFO)


for i in range(len(latitudes)):
    get_json_data(latitude=latitudes[i], longitude=longitudes[i], location_name=location_names[i])
    

