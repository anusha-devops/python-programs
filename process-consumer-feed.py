#!/usr/local/fmrlib/python/bin/python
import os
import sys
import cx_Oracle
import logging
import re
import json
import getpass
import shlex
import subprocess
from typing import Optional
import boto3
import shutil
from botocore.config import Config

def setup_logger_local(mname):
        level = logging.INFO
        hdlr = logging.FileHandler(mname)
        logging.captureWarnings(capture=True)
        logger = logging.getLogger()
        FORMAT = logging.Formatter("%(asctime)-1s : %(process)d : %(levelname)-2s : %(message)s")
        hdlr.setFormatter(FORMAT)
        logger.addHandler(hdlr)
        logger.setLevel(level)
        return logger

proxy_definitions = {
    "http": "http://http.proxy.aws.fmrcloud.com:8000",
    "https": "http://http.proxy.aws.fmrcloud.com:8000",
}
sftp_config = Config(proxies=proxy_definitions)

def filter_none_values(kwargs: dict) -> dict:
    """Returns a new dictionary excluding items where value was None"""
    return {k: v for k, v in kwargs.items() if v is not None}

def assume_session(
    role_session_name: str,
    role_arn: str,
    duration_seconds: Optional[int] = None,
    region_name: Optional[str] = None,
) -> boto3.Session:
    assume_role_kwargs = filter_none_values(
        {
            "RoleSessionName": role_session_name,
            "RoleArn": role_arn,
            "DurationSeconds": duration_seconds,
        }
    )

    credentials = boto3.client("sts", config=sftp_config).assume_role(
        **assume_role_kwargs
    )["Credentials"]

    create_session_kwargs = filter_none_values(
        {
            "aws_access_key_id": credentials["AccessKeyId"],
            "aws_secret_access_key": credentials["SecretAccessKey"],
            "aws_session_token": credentials["SessionToken"],
            "region_name": region_name,
        }
    )
    return boto3.Session(**create_session_kwargs)

def write_object(
    local_file_path_with_file_name=None,
    bucket=None,
    remote_file_path_with_file_name=None,
):
    try:
        session = assume_session(
            "MyCustomSessionName",
            DZ_IAM_ROLE_ARN_EAST_1,
            region_name="us-east-1",
        )
        # Upload the file to S3
        logger.info("uploading file")
        s3 = session.client("s3")
        s3.upload_file(
            local_file_path_with_file_name,
            bucket,
            remote_file_path_with_file_name,
            ExtraArgs={
                "ServerSideEncryption": "aws:kms",
                "SSEKMSKeyId": DZ_KMS_KEY_EAST_1,
            },
        )
    except Exception as err:
        logger.error(f"{err}")
        raise err

def fetch_db_details():
    FMTWRP_CONNECTION_PRM=FMTWRP_GATEWAY_ORAUSERID+'/'+FMTWRP_GATEWAY_ORAPASSWD+'@'+TWO_TASK

    logger.info("DB Table Name - DZ_CONSUMER_NOTIFICATION_MAPPING")

    sql_statement="""
               SELECT AS_APP_ID_CONSUMER ||'|'||AS_CONSUMER_NM ||'|'||AS_NOTIFICATION_TYPE ||'|'||AS_RESOURCE_ARN ||'|'||AS_ROLE_ARN ||'|'||AS_OUTBOUND_PATH ||'|'||AS_REGION FROM DZ_CONSUMER_NOTIFICATION_MAPPING where as_status='20'
                  """

    try:
        FMTWRP_CONNECTION=cx_Oracle.connect(FMTWRP_CONNECTION_PRM)
        FMTWRP_CURSOR=FMTWRP_CONNECTION.cursor()
        FMTWRP_CURSOR.execute(sql_statement)
        db_details=FMTWRP_CURSOR.fetchall()
        if db_details:
            with open(feed_file_name, "w+") as feeds_file:
                for row in db_details:
                    for col in row:
                        feeds_file.write(str(col))
        else:
            logger.info("All the status are 0 so no records to update")
            logger.info("Insert records into DB DZ_CONSUMER_NOTIFICATION_MAPPING table and run this api to get reflected")
            sys.exit(0)
        FMTWRP_CURSOR.close()
        FMTWRP_CONNECTION.close()
        logger.info("fetched onboarding details from mapping database table")
    except cx_Oracle.DatabaseError as db_err_msg:
        logger.info("Error from database is - {}".format(db_err_msg))
        job_msg="select query to get consumer notification mapping table is failed"
        logger.info("Error   - {}".format(job_msg))
        FMTWRP_CURSOR.close()
        FMTWRP_CONNECTION.close()
        sys.exit(1)

def download_artifactory():

    json_file_name,cksum_previous=fetch_chksum_details()
    logger.info("Downloading the consumer-notification-mapping.json from artifactory")

    try:
            # pragma: allowlist secret
        subprocess.run(['/usr/bin/wget', '-P', FMTWRP_TMP_DIR, '--user='+FMTWRP_AUTH_USER, '--password='+FMTWRP_ARTIFACT_PASSWD, FMTWRP_DZ_ARTIFACTORY_URL+'/Drop_Zone/'+FMTWRP_ENV_LGFRM+"/"+json_file_name]) # pragma: allowlist secret
        # pragma: allowlist secret
    except Exception as err:
        logger.error(f"{err}")
        raise err
    return cksum_previous

def fetch_chksum_details():

    FMTWRP_CONNECTION_PRM=FMTWRP_GATEWAY_ORAUSERID+'/'+FMTWRP_GATEWAY_ORAPASSWD+'@'+TWO_TASK

    logger.info("DB Table Name - DZ_CONSUMER_NOTIFICATION_VERSION")
    sql_statement="""
                     select as_cksum_current from DZ_CONSUMER_NOTIFICATION_VERSION where as_status='Y'
                  """

    try:
        FMTWRP_CONNECTION=cx_Oracle.connect(FMTWRP_CONNECTION_PRM)
        FMTWRP_CURSOR=FMTWRP_CONNECTION.cursor()
        FMTWRP_CURSOR.execute(sql_statement)
        cksum_current=FMTWRP_CURSOR.fetchall()
        cksum_current=str(cksum_current[0][0])

        json_file_name="consumer-notification-mapping.json_"+FMTWRP_ENV_LGFRM+cksum_current
        FMTWRP_CURSOR.close()
        FMTWRP_CONNECTION.close()
        logger.info("fetched checksum details from DZ_CONSUMER_NOTIFICATION_VERSION table")
        return json_file_name,cksum_current
    except cx_Oracle.DatabaseError as db_err_msg:
        logger.info("Error from database is - {}".format(db_err_msg))
        job_msg="select query to get DZ_CONSUMER_NOTIFICATION_VERSION table is failed"
        logger.info("Error   - {}".format(job_msg))
        FMTWRP_CURSOR.close()
        FMTWRP_CONNECTION.close()
        sys.exit(1)

def process_log_file():
    local_directory=FMTWRP_TMP_DIR
    fmtwrp_remote_file_nm="consumer-notification-mapping.json"
    feed_file_with_path=os.path.join(local_directory,feed_file_name)
    logger.info("Reading {} feed details file".format(feed_file_with_path))
    num_of_lines = sum(1 for l in open(json_new_file_name)) - 1
    line_count=0
    try:
        with open(feed_file_with_path,'r') as feeds_file_read:
            lines=feeds_file_read.readlines()
        for line in lines:
            feed_line=line.split('|')
            consumer_app_id=feed_line[0]
            consumer_name=feed_line[1]
            notification_type=feed_line[2]
            resource_arn=feed_line[3]
            role_arn=feed_line[4]
            region_name=feed_line[6].strip()
            outbound_path=feed_line[5]

            new_data='[{"feedname":"'+outbound_path+'","notifications": [{"consumer_name":"'+consumer_name+'","notification_type":"'+notification_type+'","resource_arn":"'+resource_arn+'","role_arn":"'+role_arn+'","region":"'+region_name+'"}]}'
            line_count=line_count+1
            line_number=line_count+num_of_lines
            with open(json_new_file_name,'r+') as jsonfile:
                subprocess.run(['sed', '-i', '$ i' + new_data , json_new_file_name])
                subprocess.run(['sed', '-i', str(line_number)+'!s/"}]}$/"}]},/', json_new_file_name])

        command='cksum '+ json_new_file_name
        process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE)
        output = process.stdout.readline()
        if output:
            op1=output.decode('utf-8')
            cksum_new=op1.split(" ")[0].strip()
            cksum_file_name=FMTWRP_TMP_DIR+"/"+"consumer-notification-mapping.json_"+FMTWRP_ENV_LGFRM+cksum_new

        shutil.copy(json_new_file_name, cksum_file_name)
        subprocess.run(['curl', '-u', FMTWRP_AUTH_USER+":"+FMTWRP_ARTIFACT_PASSWD, '-X', 'PUT', '-T', cksum_file_name,FMTWRP_DZ_ARTIFACTORY_URL+'/Drop_Zone/'+FMTWRP_ENV_LGFRM+"/"+"consumer-notification-mapping.json_"+FMTWRP_ENV_LGFRM+cksum_new])
        logger.info("Uploaded the New Consumer Notification mapping Json file to artifactory")
        write_object(
            cksum_file_name,
            fmtwrp_bucket_name,
            fmtwrp_remote_file_nm,
             )

        logger.info("Uploaded the New Consumer Notification mapping json file to S3")
    except Exception as err:
        logger.error(f"{err}")
        raise err
    return(cksum_new)

def update_chksum_details():

    FMTWRP_CONNECTION_PRM=FMTWRP_GATEWAY_ORAUSERID+'/'+FMTWRP_GATEWAY_ORAPASSWD+'@'+TWO_TASK

    logger.info("DB Table Name - DZ_CONSUMER_NOTIFICATION_VERSION")
    sql_update_stmt="UPDATE DZ_CONSUMER_NOTIFICATION_VERSION SET AS_STATUS='N' where AS_STATUS='Y' and AS_CKSUM_CURRENT="+cksum_previous
    sql_insert_stmt="INSERT INTO DZ_CONSUMER_NOTIFICATION_VERSION (AS_CKSUM_CURRENT, AS_CKSUM_PREVIOUS,AS_STATUS) values("+cksum_new+","+cksum_previous+",'Y')"
    sql_update_map_stmt="UPDATE DZ_CONSUMER_NOTIFICATION_MAPPING SET AS_STATUS=0 where AS_STATUS=20"

    try:
        FMTWRP_CONNECTION=cx_Oracle.connect(FMTWRP_CONNECTION_PRM)
        FMTWRP_CURSOR=FMTWRP_CONNECTION.cursor()
        FMTWRP_CURSOR.execute(sql_update_stmt)
        FMTWRP_CURSOR.execute(sql_insert_stmt)
        FMTWRP_CURSOR.execute(sql_update_map_stmt)
        FMTWRP_CURSOR.execute('commit')
        FMTWRP_CURSOR.close()
        FMTWRP_CONNECTION.close()
        logger.info("Updated table DZ_CONSUMER_NOTIFICATION_VERSION with new json file cksum value")
        logger.info("Inserted new checksum and previous checksum values into DZ_CONSUMER_NOTIFICATION_VERSION with updated status as Y")
    except cx_Oracle.DatabaseError as db_err_msg:
        logger.info("Error from database is - {}".format(db_err_msg))
        job_msg="update and insert query to DZ_CONSUMER_NOTIFICATION_VERSION table is failed"
        logger.info("Error   - {}".format(job_msg))
        FMTWRP_CURSOR.close()
        FMTWRP_CONNECTION.close()
        sys.exit(1)

def main():
    global FMTWRP_GATEWAY_ORAUSERID
    global FMTWRP_GATEWAY_ORAPASSWD
    global TWO_TASK
    global feed_file_name
    global json_file_name
    global FMTWRP_ENV_LGFRM
    global consumer_location
    global FMTWRP_AUTH_USER
    global FMTWRP_ARTIFACT_PASSWD
    global fmtwrp_bucket_name
    global FMTWRP_TMP_DIR
    global FMTWRP_DZ_ARTIFACTORY_URL
    global cksum_previous
    global json_new_file_name
    global cksum_new
    global DZ_IAM_ROLE_ARN_EAST_1
    global DZ_KMS_KEY_EAST_1

    fmtwrp_bucket_name=os.environ['DZ_BUCKET_NAME_EAST_1']
    FMTWRP_LOG_FILE_NM=os.environ['FMTWRP_LOG_FILE_NM']
    FMTWRP_GATEWAY_ORAUSERID=os.environ['FMTWRP_GATEWAY_ORAUSERID']
    FMTWRP_GATEWAY_ORAPASSWD=os.environ['FMTWRP_GATEWAY_ORAPASSWD']
    TWO_TASK=os.environ['TWO_TASK']
    FMTWRP_TMP_DIR=os.environ['FMTWRP_TMP_DIR']
    FMTWRP_DZ_ARTIFACTORY_URL=os.environ['FMTWRP_DZ_ARTIFACTORY_URL']
    FMTWRP_ENV_LGFRM=os.environ['FMTWRP_ENV_LGFRM']
    FMTWRP_AUTH_USER=os.environ['FMTWRP_AUTH_USER']
    FMTWRP_ARTIFACT_PASSWD=os.environ['FMTWRP_ARTIFACT_PASSWD']
    DZ_KMS_KEY_EAST_1=os.environ['DZ_KMS_KEY_EAST_1']
    DZ_IAM_ROLE_ARN_EAST_1=os.environ['DZ_IAM_ROLE_ARN_EAST_1']
    feed_file_name=FMTWRP_TMP_DIR+"/feeds_details.txt"

    global logger
    global cksum_new
    logger=setup_logger_local(FMTWRP_LOG_FILE_NM)

    try:
        fetch_db_details()
        cksum_previous=download_artifactory()
        json_new_file_name=FMTWRP_TMP_DIR+'/consumer-notification-mapping.json_'+FMTWRP_ENV_LGFRM+cksum_previous
        cksum_new=process_log_file()
        update_chksum_details()
        os.remove(feed_file_name)
        os.remove(json_new_file_name)
        os.remove(FMTWRP_TMP_DIR+'/consumer-notification-mapping.json_'+FMTWRP_ENV_LGFRM+cksum_new)

    except OSError as e:  ## if failed, report it back to the user ##
        logger.info("Error: %s - %s." % (e.filename, e.strerror))
    except Exception as err:
        logger.error(f"{err}")
        raise err


if __name__ == "__main__":
    main()
                                                                                                               

