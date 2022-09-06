#!/usr/bin/python3
from sqlConnector import SqlConnector
import subprocess, time
import logging
import sys
import argparse
import json

logger = logging.getLogger("scheduler-logger")
fh = logging.FileHandler("scheduler.log")
fh.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)


# INTERVAL_IN_SECS=600
INTERVAL_IN_SECS=7200

def script_executor(script_path):
    proc = subprocess.Popen(script_path, stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE, shell=True)
    out, err = proc.communicate()
    exit_code = proc.returncode
    return out, err, exit_code


class Scheduler:

    def __init__(self):
        self.jobId = -1
        self.jobName = ""
        self.script_path = ""
        self.timestamp = 0
        self.sql_interface = SqlConnector()

    def addJob(self, jobName, scriptPath, timestamp):

        self.jobName = jobName
        self.script_path = scriptPath
        self.timestamp = timestamp
        jobId = self.sql_interface.insertJob(self.jobName, self.script_path, self.timestamp)

        if jobId == -1:
            logger.error("Job scheduled failed : JobID - {jobid}, Jobname - {jobname}, Script Path - {scriptpath}, TimeStamp {timestamp}".format( 
                jobid=self.jobId, jobname=self.jobName, scriptpath=self.script_path, timestamp=self.timestamp))
            return -1
        self.jobId = jobId
        logger.info("Job scheduled successfully : JobID - {jobid}, Jobname - {jobname}, Script Path - {scriptpath}, TimeStamp {timestamp}".format( 
            jobid=self.jobId, jobname=self.jobName, scriptpath=self.script_path, timestamp=self.timestamp))
        return self.jobId
    
    def changeTimestamp(self, jobId, newtimestamp):
        job = self.sql_interface.updateJobTimeStampWithId(jobId, newtimestamp)
        if job == None :
            logger.error("Failed to update Job with Timestamp {timestamp} : {jobid} not found".format(timestamp=newtimestamp, jobid=jobId))
            return -1
        elif job == -1:
            logger.error("Failed to update Job {jobid} with Timestamp {timestamp} : Database Error".format(timestamp=newtimestamp, jobid=jobId))
            return -1
        self.timestamp = job['timestamp']
        logger. info("Updated Job {jobid} with Timestamp {timestamp} successfully".format(timestamp=newtimestamp, jobid=jobId))
        return job

    def listAllJobs(self):
        jobs = self.sql_interface.getAllJobs()

        if jobs == None:
            logger.warning("No jobs found")
            return -1
        elif jobs == -1:
            logger.fatal("Database error")
            sys.exit(1)
        return jobs

    def executeJobs(self, prune=True):

        currentTimestamp = int(time.time())
        previousTimestamp = currentTimestamp - INTERVAL_IN_SECS
        success = 0
        fail = 0
        if prune == False:
            # jobs = self.sql_interface.findJobsBetweenTimestamps(previousTimestamp, currentTimestamp)
            jobs = self.sql_interface.findJobsBetweenTimestamps(0, currentTimestamp)
        else:
            # jobs = self.sql_interface.deleteJobBetweenTimestamps(previousTimestamp, currentTimestamp)
            jobs = self.sql_interface.deleteJobBetweenTimestamps(0, currentTimestamp)
        if jobs == None:
            logger.warning("No jobs found")
            return -1
        elif jobs == -1:
            logger.fatal("Database error")
            sys.exit(1)
        for job in jobs:
            # out, err, exit_code = script_executor(job['scriptPath'] + ">> execution_output.txt")
            out, err, exit_code = script_executor(job['scriptPath'] )
            if exit_code != 0:
                logger.error ("Script execution errored for job {jobid} : {error} ".format(jobid=job['jobId'], error=err))
                fail = fail + 1
                continue
            
            logger.info("Output of job {jobid} : {output}".format(jobid=job['jobId'], output=out))
            success = success + 1
        logger.info("Success = {success}, Failed={fail}".format(success=success, fail=fail))

    # def executeJobsWithoutPrune(self):
    #     currentTimestamp = int(time.time())
    #     previousTimestamp = currentTimestamp - INTERVAL_IN_SECS
    #     success = 0
    #     fail = 0
    #     jobs = self.sql_interface.findJobsBetweenTimestamps(previousTimestamp, currentTimestamp)
    #     if jobs == None:
    #         logger.warning("No jobs found")
    #         return -1
    #     elif jobs == -1:
    #         logger.fatal("Database error")
    #         sys.exit(1)
    #     for job in jobs:
    #         # out, err, exit_code = script_executor(job['scriptPath'] + ">> execution_output.txt")
    #         out, err, exit_code = script_executor(job['scriptPath'] )
    #         if exit_code != 0:
    #             logger.error ("Script execution errored for job {jobid} : {error} ".format(jobid=job['jobId'], error=err))
    #             fail = fail + 1
    #             continue
    #         logger.info("Output of job {jobid} : {output}".format(jobid=job['jobId'], output=out))
    #         success = success + 1
    #     logger.info("Success = {success}, Failed={fail}".format(success=success, fail=fail))


    def getJobById(self, jobid):
        job = self.sql_interface.getJobById(jobid)
        if job == None:
            logger.warning("No jobs found with the jobid {jobid}".format(jobid=jobid))
            return -1
        elif job == -1:
            logger.fatal("Database error")
            sys.exit(1)
        return job

    def deleteJobById(self, jobid):
        job = self.sql_interface.deleteJobById(jobid)
        if job == None:
            logger.warning("No jobs found with the jobid {jobid}".format(jobid=jobid))
            return -1
        elif job == -1:
            logger.fatal("Database error")
            sys.exit(1)
        return job
    
    def addJobHandlerFunction(self, args):
        jobid = self.addJob(args.jobname, args.scriptpath, args.timestamp)
        if (jobid == -1):
            print ("Failed to add job")
            return -1
        print ("Job {jobid} added successfully".format(jobid=jobid) )
        # print ("add job")
        # print (args.jobname)
        # print (args.scriptpath)
        # print (args.timestamp)

    def changeTimestampHandlerFunction(self, args):
        job = self.changeTimestamp(args.jobid, args.timestamp)
        if job == -1:
            print ("Failed to update the job")
            return -1
        
        print ("Job {jobid} updated with the new timestamp {timestamp}".format(jobid=job["jobId"], timestamp=job["timestamp"]))

    def listAllJobsHandlerFunction(self, args):
        jobs=[]
        if args.jobids is not None:
            try:
                jobids = [int (i) for i in args.jobids.split(",")]
                for jobid in jobids:
                    jobs.append(self.getJobById(jobid))
            except ValueError as err:
                print("Not a valid integer : {err}".format(err=err))
        else:
            jobs = self.listAllJobs()
            if jobs == -1:
                print ("No jobs in the database")
                return 0
        for job in jobs:
            if job == -1:
                print("Could not find the jobid")
                continue
            print (json.dumps(job))

    def deleteJobByIdHandlerFunction(self, args):

        jobs=[]
        if args.jobids is not None:
            try:
                jobids = [int (i) for i in args.jobids.split(",")]
                for jobid in jobids:
                    jobs.append(self.deleteJobById(jobid))
            except ValueError as err:
                print("Not a valid integer : {err}".format(err=err))

        for job in jobs:
            if job == -1:
                print("Could not find the jobid")
                continue
            print (job)

    def executeJobsHandlerFunction(self, args):
        if args.dryrun is True:
            while True:
                self.executeJobs(prune=False)
                time.sleep(INTERVAL_IN_SECS)
        else:
            while True:
                self.executeJobs()
                time.sleep(INTERVAL_IN_SECS)

if __name__ == "__main__":
    sch = Scheduler()
    # sch.addJob("test-job", "/path/to/script", 1662437893)
    # sch.changeTimestamp(78, 1662437800)
    # sch.listAllJobs()
    # time.sleep(20)
    # sch.addJob("test-job-999", "/home/arpit/job_run/test_scripts/script1.sh", 1662307589)

    # while True:
    #     # sch.pruner()
    #     sch.findJobsToPrune()
    #     time.sleep(INTERVAL_IN_SECS)


    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='sub-command help')

    parser_add = subparsers.add_parser('add', help='Add job')
    parser_add.add_argument('--jobname', type=str, help='jobname', required=True)
    parser_add.add_argument('--scriptpath', type=str, help='scriptpath', required=True)
    parser_add.add_argument('--timestamp', type=int, help='timestamp', required=True)
    parser_add.set_defaults(func=sch.addJobHandlerFunction)

    parser_changetime = subparsers.add_parser('changetime', help='Change the timestamp of job execution')
    parser_changetime.add_argument('--jobid', type=int, help='jobid of the job', required=True)
    parser_changetime.add_argument('--timestamp', type=int, help='new timestamp of the job', required=True)
    parser_changetime.set_defaults(func=sch.changeTimestampHandlerFunction)

    parser_list = subparsers.add_parser('list', help='List all jobs')
    parser_list.add_argument('--jobids', type=str, help='comma separated jobids')
    parser_list.set_defaults(func=sch.listAllJobsHandlerFunction)

    parser_deletejob = subparsers.add_parser('delete', help='Delete the job')
    parser_deletejob.add_argument('--jobids', type=str, help='comma separated jobids', required=True)
    parser_deletejob.set_defaults(func=sch.deleteJobByIdHandlerFunction)

    parser_schedule = subparsers.add_parser('schedule', help='Schedule to run the job executions automatically based on timestamp')
    parser_schedule.add_argument('--dryrun', type=bool, help='Execute jobs without deleting the entry from the DB')
    parser_schedule.set_defaults(func=sch.executeJobsHandlerFunction)

    args = parser.parse_args()

    args.func(args=args)