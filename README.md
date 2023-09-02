# Snowflake-Datapipe

The goal is to build an example of what a simple data collection pipeline which collects data from multiple customers and uploads them to Snowflake. The OpenMeteo Api acts as the "customer system" in this case.

# Project overview

Lets imagine a case where a company for example sells various data analysis services, this could include a set of softwares that allows the customer to analyze their own data and/or services where the company gives recommendations to the customer based on data collected.

The company has multiple customers with on-premises servers which run other services provided by the company. One of these services is a tool that collects data from customer systems and stores them locally on the on-premises servers.

To enable the datadriven services the company wants to collect the data from multiple customers but at the same time keep the pipeline simple and easily maintainable. This project aims to explore a one possibility of creating a solution which runs on those on-premises servers and collects the data and uploads it to Snowflake.

The solution which ingests data from straight of the customer system should be easily maintainable and it should take into account that in the future there may rise a need to add other solution(s) that ingest data from other sources. For example the company could have a case where the customer wants to send data via email to be analyzed by the company.

To take into account the possibility of expansion of solutions I would like to set up a system where the needed transformations to the data are done before ingesting the data to Snowflake. In other words I would like for the data to arrive in the same format to Snowflake regardless of the used collection method. This makes the pipeline more manageable as we dont have to update the Snowflake logic everytime we add a new data source.

# The on-premises collection logic overview

## Requirements
The current version of the collecting logic requires a Python installation on the on-premises server.

## Installation 
To install the data collection tool on the customer on-premises server the company represantive either goes to on-site or establishes a remote connection to the server with for example RDP.

Then the person creates a new folder for the installation and pastes the following files to the folder: ```config.ini```, ```init_app.py```, ```get_data.py```, ```dump_files.py``` & the customer specific s3 bucket access keys. These files can be found from under the ```Scripts``` directory.

After the files have been uploaded then the ```config.ini``` is to be filled with the customer specific info. After the config has been filled the application can be initialized by running ```python init_app.py``` via CMD in the directory where the files are.

Schedule the following Python Scripts using Windows Task Scheduler: ```get_data.py``` & ```dump_files.py```. This article is a good example on how to do this https://www.jcchouinard.com/python-automation-using-task-scheduler/

### What do the files do?

```config.ini``` is the ini-file that is meant to be filled before initializing the "application". It contains the datapoints we want to collect from the customer and the customerid which will be used to identify which datapoints belong to which customer in Snowflake. 

```init_app.py``` script is ran after filling the ```config.ini```. The purpose of the script is to create the needed directories for the data, logs and customer specific config. The customer specific config contains the same data as the one filled by hand but also the filepaths which will be used by the ```get_data.py``` & ```dump_data.py```.

```get_data.py``` is the script responsible for collecting the data. In this demo case it collects data from OpenMeteo api. In short it does the following:

1. Requests data from the api.
2. Saves the data as json file to the temp directory.
3. Tries to upload the file to s3, if the upload is successfull the file is deleted from temp. If not the file is moved to the dump directory.

```dump_files.py``` this script is responsible for trying to dump the files from the dump directory to s3. The script tries to upload to s3 from the dump folder, if the file upload succeeds the file is deleted, if not the file is kept for a later try.

NOTE: S3 accesskeys not in the repo.

# Pipeline description

WIP
