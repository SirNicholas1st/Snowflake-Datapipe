import os
import configparser

global current_dir
current_dir = os.path.dirname(os.path.realpath(__file__))

def create_data_folders(parent_dir: str):
    dump_folder_name = "dump"
    temp_folder_name = "temp"
    config_folder_name = "config"
    log_folder_name = "logs"
    parent_dir = parent_dir

    global dump_folder_path
    dump_folder_path = os.path.join(parent_dir, dump_folder_name)
    global temp_folder_path
    temp_folder_path = os.path.join(parent_dir, temp_folder_name)
    global config_folder_path
    config_folder_path = os.path.join(parent_dir, config_folder_name)
    global log_folder_path
    log_folder_path = os.path.join(parent_dir, log_folder_name)

    # dump folder is intended to store files, if the s3 connection is down
    if not os.path.exists(dump_folder_path):
        os.mkdir(dump_folder_path)
    # temp is ment to store files temporaly before sending them to s3, after sending they are deleted
    if not os.path.exists(temp_folder_path):
        os.mkdir(temp_folder_path)
    if not os.path.exists(config_folder_path):
        os.mkdir(config_folder_path)
    if not os.path.exists(log_folder_path):
        os.mkdir(log_folder_path)


    return None

def add_config_file():
    parser = configparser.RawConfigParser()
    config_file_path = current_dir + r"\config.ini"
    parser.read(config_file_path)
    
    changes_for_config = [
        ("dump_folder", dump_folder_path),
        ("data_folder", temp_folder_path),
        ("log_folder", log_folder_path)
    ]

    for key, new_value in changes_for_config:
        parser.set("Structure-info", key, new_value)
    
    with open(config_folder_path + r"\config.ini", "w") as config_file:
        parser.write(config_file)
        
        config_file.close()

    return None

def add_log_file():
    f = open(log_folder_path + r"\logs.log", "w+")
    f.close()

    return None



create_data_folders(parent_dir = current_dir)
add_config_file()
add_log_file()
