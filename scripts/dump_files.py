import logging
import configparser
import boto3
import os
from datetime import datetime


test_config_path = r"C:\Users\Tino\Desktop\playground\config"

real_config_path = os.path.dirname(os.path.realpath(__file__)) + r"\config"


def get_config_info(config_path: str):

    parser = configparser.ConfigParser()
    parser.read(config_path + r"\config.ini")

    global dump_folder_path
    dump_folder_path = parser["Structure-info"]["dump_folder"]

    global log_path
    log_path = parser["Structure-info"]["log_folder"]
    
    return None


def send_files_to_s3(dump_path: str):

    logging.info(f"{datetime.now()}: Starting dump to s3.")

    fulltxt = open("open-meteo-customer_accessKeys.csv").read()
    secret_key = fulltxt.split(",")[-1].strip()
    access_key = fulltxt.split(",")[-2].split("\n")[-1]
    
    s3_client = boto3.client("s3",
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        region_name = "eu-north-1")
    f_count = 0
    for filename in os.listdir(dump_path):
        filepath = os.path.join(dump_path, filename)
    
        with open(filepath, "r") as f:
            try:
                s3_client.upload_file(f.name, Bucket = "open-meteo-source-bucket", Key = filename)
                logging.info(f"{datetime.now()}: {f.name} file upload succeeded, deleting file.")
                f.close()
                os.remove(f.name)
                logging.info(f"{datetime.now()}: {f.name} file deleted from dump.")
            except Exception as e:
                logging.info(f"{datetime.now()}: {f.name} file upload failed, trying again during next run. Error: {e}")
                f.close()
        f_count += 1
    if f_count == 0:
        logging.info(f"{datetime.now()}: No files to dump.")

get_config_info(test_config_path)

# Setting up logging
log_file_path = log_path + r"\logs.log"
logging.basicConfig(filename =  log_file_path, level = logging.INFO)


send_files_to_s3(dump_folder_path)