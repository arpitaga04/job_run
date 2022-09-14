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


INTERVAL_IN_SECS=600
# INTERVAL_IN_SECS=7200

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

    def findJobsByName(self, jobname):
        jobs = self.sql_interface.findJobsByJobName(jobname)

        if jobs == None:
            logger.warning("No jobs found")
            return []
        elif jobs == -1:
            logger.fatal("Database error")
            sys.exit(1)
        return jobs

    def executeJobs(self, prune=True):

        currentTimestamp = int(time.time())
        # previousTimestamp = currentTimestamp - INTERVAL_IN_SECS
        success = 0
        fail = 0

        jobs = self.sql_interface.findJobsBetweenTimestamps(0, currentTimestamp)

        if jobs == None:
            logger.info("No jobs found for execution")
            return -1
        elif jobs == -1:
            logger.fatal("Database error")
            sys.exit(1)
        for job in jobs:
            # out, err, exit_code = script_executor(job['scriptPath'] + ">> execution_output.txt")
            out, err, exit_code = script_executor(job['scriptPath'] )
            if prune:
                self.deleteJobById(job['jobId'])
            if exit_code != 0:
                logger.error ("Script execution {scriptPath} errored for job {jobid} : {error} ".format(scriptPath=job['scriptPath'], jobid=job['jobId'], error=err))
                fail = fail + 1
            else:
                logger.info("Output of job {jobid} : {output}".format(jobid=job['jobId'], output=out))
                success = success + 1
        logger.info("Success = {success}, Failed={fail}".format(success=success, fail=fail))

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
        secs = 0
        timestamp = 0
        if args.timestamp is not None:
            timestamp = args.timestamp
        else :
            if (args.days is not None):
                secs = secs + args.days*86400
            if (args.hours is not None):
                secs = secs + args.hours*3600
            if (args.mins is not None):
                secs = secs + args.mins*60
            timestamp = int(time.time()) + secs

        jobid = self.addJob(args.jobname, args.scriptpath, timestamp)
        if (jobid == -1):
            print ("Failed to add job")
            return -1
        print ("job {jobid} added successfully".format(jobid=jobid) )

    def changeTimestampHandlerFunction(self, args):
        secs = 0
        timestamp = 0
        if args.timestamp is not None:
            timestamp = args.timestamp
        else :
            if (args.days is not None):
                secs = secs + args.days*86400
            if (args.hours is not None):
                secs = secs + args.hours*3600
            if (args.mins is not None):
                secs = secs + args.mins*60
            timestamp = int(time.time()) + secs

        job = self.changeTimestamp(args.jobid, timestamp)
        if job == -1:
            print ("Failed to update the job")
            return -1
        
        print ("job {jobid} updated with the new timestamp {timestamp}".format(jobid=job["jobId"], timestamp=job["timestamp"]))

    def listJobsHandlerFunction(self, args):
        jobs=[]
        if args.jobids is not None:
            try:
                jobids = [int (i) for i in args.jobids.split(",")]
                for jobid in jobids:
                    jobs.append(self.getJobById(jobid))
            except ValueError as err:
                print ("Not a valid integer : {err}".format(err=err))
        
        elif args.jobnames is not None:
            jobnames = [i.lower().strip() for i in args.jobnames.split(",")]
            for jobname in jobnames:
                jobs.extend(self.findJobsByName(jobname))

        else:
            jobs = self.listAllJobs()
            if jobs == -1:
                print ("No jobs in the database")
                return 0
        for job in jobs:
            if job == -1:
                print ("Could not find the job")
                continue
            print (json.dumps(job))

    def deleteJobHandlerFunction(self, args):

        jobs=[]
        if args.jobids is not None:
            try:
                jobids = [int (i) for i in args.jobids.split(",")]
                for jobid in jobids:
                    jobs.append(self.deleteJobById(jobid))
            except ValueError as err:
                print ("Not a valid integer : {err}".format(err=err))

        elif args.jobnames is not None:
            jobnames = [i.lower().strip() for i in args.jobnames.split(",")]
            for jobname in jobnames:
                jobs.extend(self.findJobsByName(jobname))

            for job in jobs:
                self.deleteJobById(job['jobId'])
        else :
            print("Please input either jobids or jobnames")
            return
        for job in jobs:
            if job == -1:
                print ("Could not find the jobid")
                continue
            print (json.dumps(job))

    def executeJobsHandlerFunction(self, args):
        if args.dryrun is True:
            prune=False
        else:
            prune=True
        while True:
            self.executeJobs(prune)
            time.sleep(INTERVAL_IN_SECS)


if __name__ == "__main__":
    sch = Scheduler()

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='sub-command help')

    parser_add = subparsers.add_parser('add', help='Add job')
    parser_add.add_argument('--jobname', type=str, help='jobname', required=True)
    parser_add.add_argument('--scriptpath', type=str, help='scriptpath', required=True)
    parser_add.add_argument('--timestamp', type=int, help='Epoch timestamp of execution')
    parser_add.add_argument('--days', type=int, help='Set the execution after --days from current time')
    parser_add.add_argument('--hours', type=int, help='Set the execution after --hours from current time')
    parser_add.add_argument('--mins', type=int, help='Set the execution after --mins from current time')
    parser_add.set_defaults(func=sch.addJobHandlerFunction)
    parser_add.epilog = "Either of the values from timestamp, days, hours, or mins should be specified. Days, hours or minutes can be specified together"

    parser_changetime = subparsers.add_parser('changetime', help='Change the timestamp of job execution')
    parser_changetime.add_argument('--jobid', type=int, help='jobid of the job', required=True)
    parser_changetime.add_argument('--timestamp', type=int, help='new timestamp of the job')
    parser_changetime.add_argument('--days', type=int, help='Set the execution after --days from current time')
    parser_changetime.add_argument('--hours', type=int, help='Set the execution after --hours from current time')
    parser_changetime.add_argument('--mins', type=int, help='Set the execution after --mins from current time')
    parser_changetime.epilog = "Either of the values from timestamp, days, hours, or mins should be specified. Days, hours or minutes can be specified together"
    parser_changetime.set_defaults(func=sch.changeTimestampHandlerFunction)

    parser_list = subparsers.add_parser('list', help='List all jobs')
    parser_list.add_argument('--jobids', type=str, help='comma separated jobids')
    parser_list.add_argument('--jobnames', type=str, help='comma separated jobnames')
    parser_list.set_defaults(func=sch.listJobsHandlerFunction)

    parser_deletejob = subparsers.add_parser('delete', help='Delete the job')
    parser_deletejob.add_argument('--jobids', type=str, help='comma separated jobids')
    parser_deletejob.add_argument('--jobnames', type=str, help='comma separated jobids')
    parser_deletejob.epilog = "Either of the jobids or jobnames are required"
    parser_deletejob.set_defaults(func=sch.deleteJobHandlerFunction)

    parser_schedule = subparsers.add_parser('schedule', help='Schedule to run the job executions automatically based on timestamp')
    parser_schedule.add_argument('--dryrun', type=bool, help='Execute jobs without deleting the entry from the DB')
    parser_schedule.set_defaults(func=sch.executeJobsHandlerFunction)

    args = parser.parse_args()

    args.func(args=args)