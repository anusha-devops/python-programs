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

    logger.info("DB Table Name - DZ_PRODUCER_CONSUMER_MAPPING")

    sql_statement="""
               SELECT as_feed_name ||'|'||as_feed_name_convention ||'|'||as_consumer_loc ||'|'||as_extension_file ||'|'||as_app_id_producer ||'|'||as_producer_nm ||'|'||as_datetimestamp_req ||'|'||as_append_timestamp_req ||'|'||as_event_trigger_req FROM DZ_PRODUCER_CONSUMER_MAPPING where as_status='20'
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
            logger.info("Insert records into DB DZ_PRODUCER_CONSUMER_MAPPING table and run this api to get reflected")
            sys.exit(0)
        FMTWRP_CURSOR.close()
        FMTWRP_CONNECTION.close()
        logger.info("fetched onboarding details from mapping database table")
    except cx_Oracle.DatabaseError as db_err_msg:
        logger.info("Error from database is - {}".format(db_err_msg))
        job_msg="select query to get producer consumer mapping table is failed"
        logger.info("Error   - {}".format(job_msg))
        FMTWRP_CURSOR.close()
        FMTWRP_CONNECTION.close()
        sys.exit(1)

def download_artifactory():

    json_file_name,cksum_previous=fetch_chksum_details()
    logger.info("Downloading the producer-consumer-mapping.json from artifactory")

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

    logger.info("DB Table Name - DZ_PRODUCER_CONSUMER_VERSION")
    sql_statement="""
                     select as_cksum_current from dz_producer_consumer_version where as_status='Y'
                  """

    try:
        FMTWRP_CONNECTION=cx_Oracle.connect(FMTWRP_CONNECTION_PRM)
        FMTWRP_CURSOR=FMTWRP_CONNECTION.cursor()
        FMTWRP_CURSOR.execute(sql_statement)
        cksum_current=FMTWRP_CURSOR.fetchall()
        cksum_current=str(cksum_current[0][0])

        json_file_name="producer-consumer-mapping.json_"+FMTWRP_ENV_LGFRM+cksum_current
        FMTWRP_CURSOR.close()
        FMTWRP_CONNECTION.close()
        logger.info("fetched checksum details from DZ_PRODUCER_CONSUMER_VERSION table")
        return json_file_name,cksum_current
    except cx_Oracle.DatabaseError as db_err_msg:
        logger.info("Error from database is - {}".format(db_err_msg))
        job_msg="select query to get DZ_PRODUCER_CONSUMER_VERSION table is failed"
        logger.info("Error   - {}".format(job_msg))
        FMTWRP_CURSOR.close()
        FMTWRP_CONNECTION.close()
        sys.exit(1)

def process_log_file():
    local_directory=FMTWRP_TMP_DIR
    fmtwrp_remote_file_nm="producer-consumer-mapping.json"
    feed_file_with_path=os.path.join(local_directory,feed_file_name)
    logger.info("Reading {} feed details file".format(feed_file_with_path))
    num_of_lines = sum(1 for l in open(json_new_file_name)) - 1
    line_count=0
    with open(feed_file_with_path,'r') as feeds_file_read:
        lines=feeds_file_read.readlines()
    for line in lines:
        feed_line=line.split('|')
        feed_name=feed_line[0]
        name_conv=feed_line[1]
        consumer_loc=feed_line[2]
        file_ext=feed_line[3]
        producer_app_id=feed_line[4]
        producer_name=feed_line[5]
        date_time_stamp=feed_line[6].strip()
        append_timestamp_req=feed_line[7]
        event_trigger_req=feed_line[8].strip()

        if feed_name==name_conv:
            seperator='NULL'
            file_pattern='NULL'
            file_pattern_length=len(file_pattern)
            final_feed_name=create_feed_name(feed_name,file_pattern,file_pattern_length,file_ext,seperator,date_time_stamp)
        else:

            field_sep=name_conv.split(feed_name)[1][0]

            if re.match('^[a-zA-Z0-9]', field_sep):
                seperator='NULL'
                file_pattern=name_conv.split(feed_name)[1]
                file_pattern_length=len(file_pattern)
                final_feed_name=create_feed_name(feed_name,file_pattern,file_pattern_length,file_ext,seperator,date_time_stamp)
            else:
                file_pattern=name_conv.split(feed_name)[1][1:]
                file_pattern_length=len(file_pattern)
                final_feed_name=create_feed_name(feed_name,file_pattern,file_pattern_length,file_ext,field_sep,date_time_stamp)
        if event_trigger_req.lower() == 'yes':
            last_field=consumer_loc.lstrip("/")
            consumer_location="outbound/event/"+producer_app_id+"/"+producer_name+"/"+last_field
        elif event_trigger_req.lower() == 'no':
            last_field=consumer_loc.lstrip("/")
            consumer_location="outbound/"+producer_app_id+"/"+producer_name+"/"+last_field
        new_data='{"feed_name":"'+final_feed_name+'","consumer_location":"'+consumer_location+'","append_timestamp":"'+append_timestamp_req+'"}'
        line_count=line_count+1
        line_number=line_count+num_of_lines

        with open(json_new_file_name,'r+') as jsonfile:
            subprocess.run(['sed', '-i', '$ i' + new_data , json_new_file_name])
            subprocess.run(['sed', '-i', str(line_number)+'!s/"}$/"},/', json_new_file_name])

    command='cksum '+ json_new_file_name
    process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE)
    output = process.stdout.readline()
    if output:
        op1=output.decode('utf-8')
        cksum_new=op1.split(" ")[0].strip()
        cksum_file_name=FMTWRP_TMP_DIR+"/"+"producer-consumer-mapping.json_"+FMTWRP_ENV_LGFRM+cksum_new

    shutil.copy(json_new_file_name, cksum_file_name)
    subprocess.run(['curl', '-u', FMTWRP_AUTH_USER+":"+FMTWRP_ARTIFACT_PASSWD, '-X', 'PUT', '-T', cksum_file_name,FMTWRP_DZ_ARTIFACTORY_URL+'/Drop_Zone/'+FMTWRP_ENV_LGFRM+"/"+"producer-consumer-mapping.json_"+FMTWRP_ENV_LGFRM+cksum_new])
    logger.info("Uploaded the New Producer consumer mapping Json file to artifactory")
    write_object(
       cksum_file_name,
       fmtwrp_bucket_name,
       fmtwrp_remote_file_nm,
     )

    logger.info("Uploaded the New Producer consumer mapping json file to S3")
    return(cksum_new)

def update_chksum_details():

    FMTWRP_CONNECTION_PRM=FMTWRP_GATEWAY_ORAUSERID+'/'+FMTWRP_GATEWAY_ORAPASSWD+'@'+TWO_TASK

    logger.info("DB Table Name - DZ_PRODUCER_CONSUMER_VERSION")
    sql_update_stmt="UPDATE dz_producer_consumer_version SET AS_STATUS='N' where AS_STATUS='Y' and AS_CKSUM_CURRENT="+cksum_previous
    sql_insert_stmt="INSERT INTO dz_producer_consumer_version (AS_CKSUM_CURRENT, AS_CKSUM_PREVIOUS,AS_STATUS) values("+cksum_new+","+cksum_previous+",'Y')"
    sql_update_map_stmt="UPDATE DZ_PRODUCER_CONSUMER_MAPPING SET AS_STATUS=0 where AS_STATUS=20"

    try:
        FMTWRP_CONNECTION=cx_Oracle.connect(FMTWRP_CONNECTION_PRM)
        FMTWRP_CURSOR=FMTWRP_CONNECTION.cursor()
        FMTWRP_CURSOR.execute(sql_update_stmt)
        FMTWRP_CURSOR.execute(sql_insert_stmt)
        FMTWRP_CURSOR.execute(sql_update_map_stmt)
        FMTWRP_CURSOR.execute('commit')
        FMTWRP_CURSOR.close()
        FMTWRP_CONNECTION.close()
        logger.info("Updated table DZ_PRODUCER_CONSUMER_VERSION with new json file cksum value")
        logger.info("Inserted new checksum and previous checksum values into DZ_PRODUCER_CONSUMER_VERSION with updated status as Y")
    except cx_Oracle.DatabaseError as db_err_msg:
        logger.info("Error from database is - {}".format(db_err_msg))
        job_msg="update and insert query to DZ_PRODUCER_CONSUMER_VERSION table is failed"
        logger.info("Error   - {}".format(job_msg))
        FMTWRP_CURSOR.close()
        FMTWRP_CONNECTION.close()
        sys.exit(1)

def create_feed_name(feed_name,file_pattern,file_pattern_length,file_ext,field_sep,date_time_stamp):
        if date_time_stamp.lower() == 'yes':
                if '_' in file_pattern or '-' in file_pattern or '.' in file_pattern:
                        replaced_pattern="\\\\\\\\w.{1,"+str(file_pattern_length+1)+"}"
                else:
                        replaced_pattern="\\\\\\\\d{"+str(file_pattern_length+1)+"}"
                if field_sep == 'NULL':
                        final_feed_name=feed_name+replaced_pattern
                else:
                        final_feed_name=feed_name+field_sep+replaced_pattern

                if file_ext.lower() == 'no':
                         final_feed_name=final_feed_name
                else:
                        final_feed_name=final_feed_name+'.'+file_ext
        elif date_time_stamp.lower() == "no":
                if file_ext.lower() == 'no':
                        final_feed_name=feed_name
                else:
                        final_feed_name=feed_name+'.'+file_ext
        return(final_feed_name)


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
        json_new_file_name=FMTWRP_TMP_DIR+'/producer-consumer-mapping.json_'+FMTWRP_ENV_LGFRM+cksum_previous
        cksum_new=process_log_file()
        update_chksum_details()
        os.remove(feed_file_name)
        os.remove(json_new_file_name)
        os.remove(FMTWRP_TMP_DIR+'/producer-consumer-mapping.json_'+FMTWRP_ENV_LGFRM+cksum_new)

    except OSError as e:  ## if failed, report it back to the user ##
        logger.info("Error: %s - %s." % (e.filename, e.strerror))
    except Exception as err:
        logger.error(f"{err}")
        raise err


if __name__ == "__main__":
    main()
        
        
        
