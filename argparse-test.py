#!/usr/bin/python3
import argparse
from ast import arg
import time

def addJobHandlerFunction(args):
    print ("add job")
    print (args.jobname)
    print (args.scriptpath)
    print (args.timestamp)

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
    
    print ("Execution Timestamp = {timestamp}".format(timestamp=timestamp))

def changeTimestampHandlerFunction(args):
    print ("changeTimestamp")

    print (args.jobid)
    print (args.timestamp)

def listAllJobsHandlerFunction(args):
    print ("Listing all jobs")



parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(help='sub-command help')

parser_add = subparsers.add_parser('add', help='Add job')
parser_add.add_argument('--jobname', type=str, help='jobname', required=True)
parser_add.add_argument('--scriptpath', type=str, help='scriptpath', required=True)
parser_add.add_argument('--timestamp', type=int, help='timestamp')
parser_add.add_argument('--days', type=int, help='Set the execution after --days from current time')
parser_add.add_argument('--hours', type=int, help='Set the execution after --hours from current time')
parser_add.add_argument('--mins', type=int, help='Set the execution after --mins from current time')
parser_add.set_defaults(func=addJobHandlerFunction)

parser_add.epilog = "Either of the values from timestamp, days, hours, or mins should be specified. Days, hours or minutes can be specified together"

parser_changetime = subparsers.add_parser('changetime', help='Change the timestamp of job execution')
parser_changetime.add_argument('--jobid', type=int, help='jobid of the job', required=True)
parser_changetime.add_argument('--timestamp', type=int, help='new timestamp of the job', required=True)
parser_changetime.set_defaults(func=changeTimestampHandlerFunction)

parser_list = subparsers.add_parser('list', help='List all jobs')
parser_list.set_defaults(func=listAllJobsHandlerFunction)

args = parser.parse_args()

args.func(args=args)
# options.func()
# output = vars()
# print (output)





# # parse some argument lists
# vars(parser.parse_args(['a', '12']))
# # Namespace(bar=12, foo=False)s
# vars(parser.parse_args(['--foo', 'b', '--baz', 'Z']))