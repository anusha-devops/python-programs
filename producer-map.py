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
        logger.info("Database Table Name : dz_producer_consumer_mapping")
        FMTWRP_FILE=os.path.join(FMTWRP_INPUT_FILE_LOC,FMTWRP_FL_NM)
        logger.info("Reading {} input file".format(FMTWRP_FILE))
        with open(FMTWRP_FILE,'r') as feed_details:
                lines=feed_details.readlines()
        for line in lines:
                input_line=line.split('|')
                feed_name=input_line[0]
                feed_name_convention=input_line[1]
                consumer_location=input_line[2]
                file_extension=input_line[3]
                producer_app_id=input_line[4]
                producer_name=input_line[5]
                datetimestamp_required=input_line[6]
                append_timestamp_required=input_line[7]
                event_trigger_required=input_line[8]
                feed_status="20"

                sql_insert="insert into DZ_PRODUCER_CONSUMER_MAPPING (AS_FEED_NAME,AS_FEED_NAME_CONVENTION,AS_CONSUMER_LOC,AS_EXTENSION_FILE,AS_APP_ID_PRODUCER,AS_PRODUCER_NM,AS_DATETIMESTAMP_REQ,AS_APPEND_TIMESTAMP_REQ,AS_EVENT_TRIGGER_REQ) values ("+"'"+feed_name+"',"+"'"+feed_name_convention+"',"+"'"+consumer_location+"',"+"'"+file_extension+"',"+"'"+producer_app_id+"',"+"'"+FMTWRP_GATEWAY_ORAUSERID+"',"+"'"+producer_name+"',"+"'"+datetimestamp_required+"',"+"'"+append_timestamp_required+"',"+"'"+event_trigger_required+"',"+"'"+feed_status+"')"

                logger.info(sql_insert)

                sql_statement="""
            insert into DZ_PRODUCER_CONSUMER_MAPPING (AS_FEED_NAME,AS_FEED_NAME_CONVENTION,AS_CONSUMER_LOC,AS_EXTENSION_FILE,AS_APP_ID_PRODUCER,AS_INS_ID,AS_PRODUCER_NM,AS_DATETIMESTAMP_REQ,AS_APPEND_TIMESTAMP_REQ,AS_EVENT_TRIGGER_REQ,AS_STATUS) values (:feed_name,:feed_name_convention,:consumer_location,:file_extension,:producer_app_id,:FMTWRP_GATEWAY_ORAUSERID,:producer_name,:datetimestamp_required,:append_timestamp_required,:event_trigger_required,:feed_status)
"""
                try:
                        FMTWRP_CONNECTION=FMTWRP_CONN_POOL.acquire()
                        FMTWRP_CURSOR=FMTWRP_CONNECTION.cursor()
                        FMTWRP_CURSOR.execute(sql_statement,[feed_name,feed_name_convention,consumer_location,file_extension,producer_app_id,FMTWRP_GATEWAY_ORAUSERID,producer_name,datetimestamp_required,append_timestamp_required,event_trigger_required,feed_status])
                        FMTWRP_CURSOR.execute('commit')
                        FMTWRP_CURSOR.close()
                        logger.info("Inserted Producer consumer mapping information in db table")
                except cx_Oracle.DatabaseError as db_err_msg:
                        logger.info("Error from database is - {}".format(db_err_msg))
                        job_msg="insert in producer consumer mapping table is failed"
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
