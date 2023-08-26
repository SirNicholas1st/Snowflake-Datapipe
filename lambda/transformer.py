import json
import boto3
from datetime import datetime

# AWS lambda function to add information from the file name to the json data.

def lambda_handler(event, context):
    
    s3_client = boto3.client("s3")
    source_bucket = "open-meteo-source-bucket"
    target_bucket = "open-meteo-target-bucket"
    source_key = event["Records"][0]["s3"]["object"]["key"]
    target_key = "modified_" + source_key
    
    
    response_object = s3_client.get_object(Bucket = source_bucket, Key = source_key)
    json_content = response_object["Body"].read().decode("utf-8")
    json_data = json.loads(json_content)
    
    list_of_key_info = source_key.split("_")
    file_timestamp = list_of_key_info[0].replace("-", ".")
    modified_timestamp = datetime.fromtimestamp(float(file_timestamp)).strftime("%Y-%m-%d %H:%M:%S")
    customer_id = list_of_key_info[1]
    location_name = list_of_key_info[-1].replace(".json", "")
    
    json_data["Timestamp"] = modified_timestamp
    json_data["customer_id"] = customer_id
    json_data["Location_name"] = location_name
    
    modified_json_content = json.dumps(json_data)
    
    s3_client.put_object(Bucket = target_bucket, Key = target_key, Body = modified_json_content)