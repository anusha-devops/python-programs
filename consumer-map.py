#!/usr/local/fmrlib/python/bin/python
import os
import sys
import cx_Oracle
import logging

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

def process_input_file(FMTWRP_GATEWAY_ORAUSERID,FMTWRP_CONN_POOL):
        logger.info("Database Table Name : dz_consumer_notification_mapping")
        FMTWRP_FILE=os.path.join(FMTWRP_INPUT_FILE_LOC,FMTWRP_FL_NM)
        logger.info("Reading {} input file".format(FMTWRP_FILE))
        with open(FMTWRP_FILE,'r') as feed_details:
                lines=feed_details.readlines()
        for line in lines:
                input_line=line.split('|')
                consumer_app_id=input_line[0]
                producer_name=input_line[1]
                consumer_name=input_line[2]
                consumer_loc=input_line[3]
                notification_type=input_line[4]
                resource_arn=input_line[5]
                role_arn=input_line[6]
                region=input_line[7]
                feed_status="20"

                last_field=consumer_loc.rstrip('/')
                outbound_path="outbound/event/"+consumer_app_id+"/"+consumer_name+"/"+last_field
                sql_statement="""
            insert into DZ_CONSUMER_NOTIFICATION_MAPPING (AS_APP_ID_CONSUMER,AS_PRODUCER_NM,AS_CONSUMER_NM,AS_OUTBOUND_PATH,AS_NOTIFICATION_TYPE,AS_RESOURCE_ARN,AS_ROLE_ARN,AS_REGION,AS_INS_ID,AS_STATUS) values (:consumer_app_id,:producer_name,:consumer_name,:outbound_path,:notification_type,:resource_arn,:role_arn,:region,:FMTWRP_GATEWAY_ORAUSERID,:feed_status)
"""
                try:
                        FMTWRP_CONNECTION=FMTWRP_CONN_POOL.acquire()
                        FMTWRP_CURSOR=FMTWRP_CONNECTION.cursor()
                        FMTWRP_CURSOR.execute(sql_statement,[consumer_app_id,producer_name,consumer_name,outbound_path,notification_type,resource_arn,role_arn,region,FMTWRP_GATEWAY_ORAUSERID,feed_status])
                        FMTWRP_CURSOR.execute('commit')
                        FMTWRP_CURSOR.close()
                        logger.info("Inserted in consumer notification mapping information in db table")
                except cx_Oracle.DatabaseError as db_err_msg:
                        logger.info("Error from database is - {}".format(db_err_msg))
                        job_msg="insert in consumer notification mapping table is failed"
                        logger.info("Error   - {}".format(job_msg))
                        FMTWRP_CURSOR.close()

        FMTWRP_CONN_POOL.release(FMTWRP_CONNECTION)

def main():
        global logger
        global FMTWRP_INPUT_FILE_LOC
        global FMTWRP_FL_NM
        FMTWRP_FL_NM=os.environ['FMTWRP_FL_NM']
        FMTWRP_INPUT_FILE_LOC=os.environ['FMTWRP_INPUT_FILE_LOC']
        TWO_TASK=os.environ['TWO_TASK']
        FMTWRP_LOG_FILE_NM=os.environ['FMTWRP_LOG_FILE_NM']
        FMTWRP_GATEWAY_ORAUSERID=os.environ['FMTWRP_GATEWAY_ORAUSERID']
        FMTWRP_GATEWAY_ORAPASSWD=os.environ['FMTWRP_GATEWAY_ORAPASSWD']
        logger=setup_logger_local(FMTWRP_LOG_FILE_NM)

        FMTWRP_CONN_POOL=cx_Oracle.SessionPool(FMTWRP_GATEWAY_ORAUSERID,FMTWRP_GATEWAY_ORAPASSWD,TWO_TASK, min=1, max=2,increment=1, encoding="UTF-8") #pragma: no cover
        process_input_file(FMTWRP_GATEWAY_ORAUSERID,FMTWRP_CONN_POOL)

if __name__ == "__main__":
        main()
        
