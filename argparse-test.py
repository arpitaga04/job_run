#!/usr/bin/python3
import argparse

def addJobHandlerFunction(args):
    print ("add job")
    print (args.jobName)
    print (args.scriptPath)
    print (args.timestamp)

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
parser_add.add_argument('--timestamp', type=int, help='timestamp', required=True)
parser_add.set_defaults(func=add_job)

parser_changetime = subparsers.add_parser('changetime', help='Change the timestamp of job execution')
parser_changetime.add_argument('--jobid', type=int, help='jobid of the job', required=True)
parser_changetime.add_argument('--timestamp', type=int, help='new timestamp of the job', required=True)
parser_changetime.set_defaults(func=changeTimestamp)

parser_list = subparsers.add_parser('list', help='List all jobs')
parser_list.set_defaults(func=listAllJobs)

args = parser.parse_args()

args.func(args=args)
# options.func()
# output = vars()
# print (output)





# # parse some argument lists
# vars(parser.parse_args(['a', '12']))
# # Namespace(bar=12, foo=False)s
# vars(parser.parse_args(['--foo', 'b', '--baz', 'Z']))