#!/usr/bin/python3

from mysql.connector import connection
import logging
import time
import sys

logger = logging.getLogger("Sql-logger")
fh = logging.FileHandler("sql-connector.log")
fh.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

class SqlConnector:

    def __init__(self):

        self.host ="localhost"
        self.user ="root"
        self.password ="password"
        self.database = "test"
        self.table = "job_schedules"
        if(self.getDBConnection() == -1):
            sys.exit(1)
        if(self.createTableIfNotPresent() == -1):
            sys.exit(1)

    def getDBConnection(self):
        connection_info = {
            'user': self.user,
            'host': self.host,
            'database': self.database,
            'passwd': self.password,
        }
        # print (connection_info)
        try:
            self.db_connection = connection.MySQLConnection(**connection_info)
            self.db_connection.ping()
        except connection.Error as error:
            logger.error("Unable to connect to the Database: {}".format(error))
            return -1
        logger.info("Successfully connected to the database : {database}".format(database=self.database))
        return 0

    def closeDBConnection(self):
        self.db_connection.close()

    def createTableIfNotPresent(self):
        try:
            if self.table not in self.getTables() :
                table_schema_query = """
                    CREATE TABLE {table} (
                        JOB_ID INT AUTO_INCREMENT PRIMARY KEY,
                        JOB_NAME VARCHAR(20),
                        SCRIPT_PATH VARCHAR(256),
                        TIMESTAMP INT
                    )""".format(table = self.table)
                cursorObj = self.db_connection.cursor()
                cursorObj.execute(table_schema_query)
                logger.info("Successfully created the table : {table}".format(table=self.table))
        except connection.Error as error:
            logger.fatal("Database error: Failed to Create table: {}".format(error))
            return -1
        return 0
                

    def getTables(self):
        query = "SHOW TABLES"
        try:
            cursorObj = self.db_connection.cursor()
            cursorObj.execute(query)
        except connection.Error as error:
            logger.error("Database error: Unable to execute the Query {query} : {error}".format(query = query, error = error))
            return -1
        logger.info("Query executed successfully : {query}".format(query=query))
        results = cursorObj.fetchall()
        if len(results) == 0:
            return None
        tables=[]
        for i in results:
            table, = i
            tables.append(table)
        return tables

    def insertJob(self, jobname, script_path, timestamp):
        query = "INSERT INTO {table} (JOB_NAME, SCRIPT_PATH, TIMESTAMP) VALUES (%s, %s, %s) ".format(table=self.table)
        values = (jobname, script_path, timestamp)
        try:
            cursorObj = self.db_connection.cursor()
            cursorObj.execute(query, values)
            self.db_connection.commit()
        except connection.Error as error:
            logger.error("Database Error: Unable to execute the Query {query} : {error}".format(query = query, error = error))
            return -1
        logger.info("Insert successful : {values}".format(values=values))
        return cursorObj.lastrowid
    
    def getJobById(self, jobid):
        query = "SELECT * FROM {table} WHERE JOB_ID=%s".format(table=self.table)
        values=(jobid,)
        try:
            cursorObj = self.db_connection.cursor()
            cursorObj.execute(query, values)
        except connection.Error as error:
            logger.error("Database error: Unable to execute the Query {query} : {error}".format(query = query, error = error))
            return -1
        result = cursorObj.fetchone()
        if result == None:
            logger.warning("Could not find the job with ID {}".format(jobid))
            return None
        result_dict = {
            'jobId': result[0],
            'jobName': result[1],
            'scriptPath': result[2],
            'timestamp': result[3]
        }
        return (result_dict)
    
    def deleteJobById(self, jobid):
        query = "DELETE FROM {table} WHERE JOB_ID=%s".format(table=self.table)
        job = self.getJobById(jobid)
        if job == -1:
            logger.error("Database error: Failed to fetch the job with ID {}".format(jobid))
            return -1
        elif job == None:
            # logger.warn("Could not find the job with ID {}".format(jobid))
            return None
        try:   
            values=(jobid,)
            cursorObj = self.db_connection.cursor()
            cursorObj.execute(query, values)
            self.db_connection.commit()
        except connection.Error as error:
            logger.error("Database error: Unable to execute the Query {query} : {error}".format(query = query, error = error))
            return -1
        logger.info("Delete successful : {values}".format(values=values))
        return job

    def findJobsBetweenTimestamps(self, epochStartTime, epochEndTime):
        query = "SELECT * FROM {table} WHERE TIMESTAMP>=%s AND TIMESTAMP<=%s".format(table=self.table)
        values=(epochStartTime, epochEndTime)
        try:
            cursorObj = self.db_connection.cursor()
            cursorObj.execute(query, values)
        except connection.Error as error:
            logger.error("Database error: Unable to execute the Query {query} : {error}".format(query = query, error = error))
            return -1
        results = cursorObj.fetchall()
        if len(results) == 0:
            logger.warning("Could not find jobs between {epochStartTime} and {epochEndTime}".format(epochStartTime=epochStartTime, epochEndTime=epochEndTime))
            return None
        jobs = []
        for result in results:
            result_dict = {
            'jobId': result[0],
            'jobName': result[1],
            'scriptPath': result[2],
            'timestamp': result[3]
            }

            jobs.append(result_dict)

        return (jobs)

    def findJobsByJobName(self, jobname):
        query = "SELECT * FROM {table} WHERE JOB_NAME LIKE %s".format(table=self.table)
        values=("%" + jobname + "%",)
        try:
            cursorObj = self.db_connection.cursor()
            cursorObj.execute(query, values)
        except connection.Error as error:
            logger.error("Database error: Unable to execute the Query {query} : {error}".format(query = query, error = error))
            return -1
        results = cursorObj.fetchall()
        if len(results) == 0:
            logger.warning("Could not find jobs with pattern : {jobname}".format(jobname=jobname))
            return None
        jobs = []
        for result in results:
            result_dict = {
            'jobId': result[0],
            'jobName': result[1],
            'scriptPath': result[2],
            'timestamp': result[3]
            }

            jobs.append(result_dict)

        return (jobs)

    def getAllJobs(self):
        query = "SELECT * FROM {table}".format(table=self.table)
        try:
            cursorObj = self.db_connection.cursor()
            cursorObj.execute(query)
        except connection.Error as error:
            logger.error("Database error: Unable to execute the Query {query} : {error}".format(query = query, error = error))
            return -1
        results = cursorObj.fetchall()
        if len(results) == 0:
            return None
        jobs = []
        for result in results:
            result_dict = {
            'jobId': result[0],
            'jobName': result[1],
            'scriptPath': result[2],
            'timestamp': result[3]
            }

            jobs.append(result_dict)

        return (jobs)

    def deleteJobBetweenTimestamps(self, epochStartTime, epochEndTime):
        query = "DELETE FROM {table} WHERE TIMESTAMP>=%s AND TIMESTAMP<=%s".format(table=self.table)
        jobs = self.findJobsBetweenTimestamps(epochStartTime, epochEndTime)
        if jobs == -1:
            logger.error("Database error: Failed to fetch the rows")
            return -1
        elif jobs == None:
            # logger.warn("Could not find the job with ID {}".format(jobid))
            return None
        try:   
            values=(epochStartTime,epochEndTime)
            cursorObj = self.db_connection.cursor()
            cursorObj.execute(query, values)
            self.db_connection.commit()
        except connection.Error as error:
            logger.error("Database error: Unable to execute the Query {query} : {error}".format(query = query, error = error))
            return -1
        logger.info("Delete successful : {values}".format(values=values))
        return jobs

    def updateJobTimeStampWithId(self, jobid, timestamp):

        query = "UPDATE {table} SET TIMESTAMP=%s WHERE JOB_ID=%s".format(table=self.table)
        values = (timestamp, jobid)
        try:
            cursorObj = self.db_connection.cursor()
            cursorObj.execute(query, values)
            self.db_connection.commit()
            if cursorObj.rowcount == 0:
                logger.warning("Could not find the job with ID {}".format(jobid))
                return None
        except connection.Error as error:
            logger.error("Database error: Unable to execute the Query {query} : {error}".format(query = query, error = error))
            return -1

        logger.info("Update successful : {values}".format(values=values))
        return self.getJobById(jobid)

    
    

def populateTable():
    currentTime = int(time.time())
    for i in range(1,101):
        sql_interface.insertJob("test-{}".format(i),"/home/arpit/job_run/test_scripts/script{}.sh".format(i), currentTime+(30*i))

if __name__ == "__main__":
    sql_interface = SqlConnector()
    sql_interface.createTableIfNotPresent()
    jobname = " TesT-_9 "
    jobs = sql_interface.findJobsByJobName(jobname.lower().strip())
    for job in jobs:
        print (job)
    print ("Number of jobs = ", len(jobs))
    # populateTable()

    # epochEndTime = int(time.time())
    # epochStartTime = epochEndTime - 600

    # jobsBetweenTimestamps = sql_interface.findJobsBetweenTimestamps(epochStartTime,epochEndTime)
    # if jobsBetweenTimestamps != None:
    #     for job in jobsBetweenTimestamps:
    #         print (job)
    #     print ("Number of jobs = ", len(jobsBetweenTimestamps))

    sql_interface.closeDBConnection()
