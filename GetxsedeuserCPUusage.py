'''
Author: Yaling Zheng
August 2012
Holland Computing Center, University of Nebraska-Lincoln 

This script produces a weekly report of user usage of xsede 

Yaling, just to recap.  If we could run a job once a week (Monday?)
that reports on the last 30 days and shows

- only those users with a valid XSEDE allocation/project-id that had
usage > 0 hours

- for those users, show the usage for the last 30 days (number of
hours total)

- send via email to Mats, Chander, Tanya, Yaling
'''

import os
# from sets import Set
import re
import time
from time import gmtime, strftime
from datetime import datetime, date, timedelta
import MySQLdb
import ConfigParser
import string
# package of parsing options
import optparse
# package of sending emails
import smtplib
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart
import csv
from email.MIMEBase import MIMEBase
#from email.Utils import COMMASPACE, formatdate
from email import Encoders

os.environ['TZ'] = "US/Central"
time.tzset()
Fermilaboffset = time.timezone/3600

now  = datetime.now()
# NOTE - Nebraska runs this on a 30-day  period, starting at 6am local.
# We must convert to a Unix epoch including DST, add the Nebraska offset,
# then to a UTC datetime.
Nebraska_start = datetime(now.year, now.month, now.day, now.hour, now.minute, now.second)
Nebraska_start_epoch = int(time.mktime(Nebraska_start.timetuple())) + Fermilaboffset/3600
LatestEndTime = datetime.utcfromtimestamp(Nebraska_start_epoch)
EarliestEndTime = LatestEndTime - timedelta(30, 0)

# the message (which is in fact the xrootd report) to send to a group
# of people
outputmsg = ""
xsedeusers = ""
xsedeprojects = ""
filename = "MonthlyCpuUsageSummary.csv"
f = open(filename, "w")
writer = csv.writer(f, delimiter=',') 

'''
Parse Arguments. 

First parameter is --users (-u) indicates which users monthly CPU usage are going to 
be shown 

Second parameter is --projects (-p) indicates which projects monthly CPU usage are going to
be shown

Third parameter is -from indicates from which to send the report to If
running under my account yzheng@cse.unl.edu, do I have to use this
account?

fourth parameter --to (-t) is to whom send the report.

Fifth parameter --begin (-b) is the earliest time that job ends at.

Sixth parameter --end (-e) is the latest time that job ends at. 
'''

def parseArguments():
    global xsedeusers
    global xsedeprojects
    parser = optparse.OptionParser()
    parser.add_option("-u", "--users", dest="xsedeusers", default=None, help="the xsede users that we are going to display their usage in the recent 30 days")
    parser.add_option("-p", "--projects", dest="xsedeprojects", default=None, help="the xsede projects that we are going to display their usage in the recent 30 days")
    parser.add_option("-d", "--date", dest="ReportDate", default=None, help="the date of xrootd report, in the form of 2002-02-28")
    parser.add_option("-f", "--from", dest="ReportSender", default=None, help="the sender of the xrootd report (format is email address, by default it is yzheng@cse.unl.edu)")
    parser.add_option("-t", "--to", dest="ReportReceiver", default=None, help="the receiver of the xrootd report (format is email address, by default it is yaling.zheng@gmail.com)")
    parser.add_option("-b", "--begin", dest="JobEarliestEndTime", default=None, help="the earliest UTC end time of the jobs (format 2008-12-03 23:34:45)")
    parser.add_option("-e", "--end", dest="JobLatestEndTime", default=None, help="the latest UTC end time of the jobs (format 2008-12-04 22:04:25)")
    
    options, args = parser.parse_args()
    xsedeusers = options.xsedeusers
    xsedeprojects = options.xsedeprojects
    ReportDate = GetValidDate(options.ReportDate)
    ReportSender = options.ReportSender
    ReportReceiver = options.ReportReceiver
    JobEarliestEndTime = options.JobEarliestEndTime
    JobLatestEndTime = options.JobLatestEndTime

    return ReportDate, ReportSender, ReportReceiver, JobEarliestEndTime, JobLatestEndTime


# Judge whether a date is valid

def GetValidDate(date):
    if date:
        time_t = time.strptime(date, '%Y-%m-%d')
        return datetime(*time_t[:6])
    return None

'''
Connect to rcf-gratia.unl.edu, and prepare database gratia for querying
'''
def ConnectDatabase():
    # read configuration file, get username and password of one user
    config = ConfigParser.ConfigParser()
    config.read("/home/yzheng/xsedereport/mygratiaDBpwd.ini")
    username = config.get("gr-osg-mysql-reports", "username")
    password = config.get("gr-osg-mysql-reports", "password")
    # connect with the database
    try:
        db = MySQLdb.connect("gr-osg-mysql-reports.opensciencegrid.org", username, password, "gratia", 3306)
    except Exception:
        return None, None
    # prepare a cursor oject using cursor() method
    cursor = db.cursor()
    
    # return database cursor, and job latest End time and earliest end time
    return db, cursor


def QueryUsersProjectsUsage(cursor):
    # The database keeps its begin/end times in UTC.
    global EarliestEndTime
    global LatestEndTime
    global xsedeusers
    global outputmsg
    # Compute number (wallduration, CpuUserDuration+CpuSystemDuration) of overflow jobs in all sites
    querystring = """
    SELECT
      LocalUserId, ProjectName, SUM(WallDuration) as MonthlyCpuTime
    FROM JobUsageRecord JUR
    JOIN JobUsageRecord_Meta JURM on JURM.dbid=JUR.dbid
    WHERE 
      ProbeName="condor:osg-xsede.grid.iu.edu" AND
      LocalUserId IN %s AND ProjectName IN %s AND EndTime>=\"%s\" AND EndTime<\"%s\"
    GROUP BY 
      LocalUserId, ProjectName
    ORDER BY
      MonthlyCpuTime DESC
    """ % (xsedeusers, xsedeprojects, EarliestEndTime, LatestEndTime)
    print querystring
    cursor.execute(querystring)
    # Handle each record
    numrows = int(cursor.rowcount)
    global writer
    writer.writerow(["Users", "Projects", "Cpu Time Usage (hours)"])
    msg =  "%20s %20s %15s\n" % ("Users", "Projects", "Cpu Time")
    msg += "%20s %20s %15s" % ("  ", "  ", "Usage (hours)")
    global outputmsg
    outputmsg += msg+"\n"
    for i in range(numrows):
        row = cursor.fetchone()
        #print row[0], row[1]
        username = row[0]
        projectname = row[1]
        usage = "%.0f" % (float(row[2])/3600)
	#print host
	if (usage > 0):
            writer.writerow([username, projectname, str(usage)])
            msg = "%20s %20s %15d" % (username, projectname, int(usage))

            outputmsg += msg+"\n"
 

def QueryUsersUsage(cursor):
    # The database keeps its begin/end times in UTC.
    global EarliestEndTime
    global LatestEndTime
    global xsedeusers
    global outputmsg
    # Compute number (wallduration, CpuUserDuration+CpuSystemDuration) of overflow jobs in all sites
    querystring = """
    SELECT
      LocalUserId, SUM(WallDuration) as MonthlyCpuTime
    FROM JobUsageRecord JUR
    JOIN JobUsageRecord_Meta JURM on JUR.dbid=JURM.dbid
    WHERE 
      ProbeName="condor:osg-xsede.grid.iu.edu" AND
      LocalUserId IN %s AND EndTime>=\"%s\" AND EndTime<\"%s\"
    GROUP BY 
      LocalUserId
    ORDER BY
      MonthlyCpuTime DESC
    """ % (xsedeusers, EarliestEndTime, LatestEndTime)
    print querystring
    cursor.execute(querystring)
    # Handle each record
    numrows = int(cursor.rowcount)
    global writer
    writer.writerow(["", "", ""])
    writer.writerow(["Users", "", "Cpu Time Usage (hours)"])
    msg =  "%20s %25s" % ("Users", "Cpu Time Usage (hours)")
    global outputmsg
    outputmsg += "\n"+msg+"\n"
    for i in range(numrows):
        row = cursor.fetchone()
        #print row[0], row[1]
        username = row[0]
        usage = "%.0f" % (float(row[1])/3600)
	#print host
	if (usage > 0):
            writer.writerow([username, "", str(usage)])
	    msg = "%20s %25d" % (username, int(usage))
           
            outputmsg += msg+"\n"
 

def QueryProjectsUsage(cursor):
    # The database keeps its begin/end times in UTC.
    global EarliestEndTime
    global LatestEndTime
    global xsedeprojects
    global outputmsg
    # Compute number (wallduration, CpuUserDuration+CpuSystemDuration) of overflow jobs in all sites
    querystring = """
    SELECT
      ProjectName, SUM(WallDuration) as MonthlyCpuTime
    FROM JobUsageRecord JUR
    JOIN JobUsageRecord_Meta JURM on JUR.dbid=JURM.dbid
    WHERE 
      ProbeName="condor:osg-xsede.grid.iu.edu" AND
      ProjectName IN %s AND EndTime>=\"%s\" AND EndTime<\"%s\"
    GROUP BY 
      ProjectName
    ORDER BY 
      MonthlyCpuTime DESC
      """ % (xsedeprojects, EarliestEndTime, LatestEndTime)
    print querystring
    cursor.execute(querystring)
    # Handle each record
    numrows = int(cursor.rowcount)
    
    msg =  "%20s %25s" % ("Projects", "Cpu Time Usage (hours)")
    outputmsg += "\n"+msg+"\n"
    global writer
    writer.writerow(["", "", ""])
    writer.writerow(["", "Projects", "Cpu Time Usage (hours)"]) 
    for i in range(numrows):
        row = cursor.fetchone()
        #print row[0], row[1]
        projectname = row[0]
        usage = "%.0f" % (float(row[1])/3600)
	#print host
	if (usage > 0):
            writer.writerow(["", projectname, str(usage)])
            msg = "%20s %25d" % (string.rjust(projectname, 20), int(usage))
    
            outputmsg += msg+"\n"

def SendEmail(ReportDateString, ReportSender, ReportReceiver):
    global outputmsg
    msg = MIMEMultipart()
    # Only get the 2012-04-28 of the ReportDateString
    ReportDate = ReportDateString[:10]
    msg["Subject"] = "xsede users and projects CPU usage report of recent 30 days till " + ReportDate 
    msg["From"] = "yzheng@cse.unl.edu"
    if ReportSender!=None:
        msg["From"] = ReportSender
    msg["To"] = "yaling.zheng@gmail.com,tlevshin@fnal.gov,rynge@isi.edu,cssehgal@fnal.gov"
    #body1 = MIMEText(outputmsg)
    body2 = MIMEText("<pre>%s</pre>" % outputmsg, "html")
    #msg.attach(body1)
    msg.attach(body2)
    part = MIMEBase('application', "octet-stream")
    global filename
    part.set_payload(open(filename, "rb").read())
    Encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename=%s' % filename)
    msg.attach(part)
    if ReportReceiver!=None:
        msg["To"] = ReportReceiver
    s = smtplib.SMTP("localhost")
    s.sendmail(msg["From"], msg["To"].split(","), msg.as_string())
    s.quit()

def main():    
    global EarliestEndTime
    global LatestEndTime
    (ReportDate, ReportSender, ReportReceiver, JobEarliestEndTime, JobLatestEndTime)= parseArguments()
    today = datetime.today()
    ReportDateString = today.strftime("%Y-%m-%d")
    if ReportDate!=None:
        #print str(ReportDate)
        ReportDateString = str(ReportDate)
        ReportDay = time.strptime(str(ReportDate), "%Y-%m-%d 00:00:00")
        Fermilab_start = datetime(ReportDay.tm_year, ReportDay.tm_mon, ReportDay.tm_mday, 7, 0, 0)
        Fermilab_start_epoch = int(time.mktime(Fermilab_start.timetuple())) + Fermilaboffset/3600
        LatestEndTime = datetime.utcfromtimestamp(Fermilab_start_epoch)
        EarliestEndTime = LatestEndTime - timedelta(30, 0)
    if JobEarliestEndTime!= None:
        EarliestEndTime = JobEarliestEndTime
    if JobLatestEndTime != None:
        LatestEndTime =  JobLatestEndTime
    # connect the database server rcf-gratia.unl.edu
    db, cursor = ConnectDatabase()
    if db:
        # query database gratia, and output statistic results
        QueryUsersProjectsUsage(cursor)
        QueryUsersUsage(cursor)
        QueryProjectsUsage(cursor)
        global outputmsg
        print outputmsg
        global f
        f.close()
        db.close()
        SendEmail(ReportDateString, ReportSender, ReportReceiver)
    
# execute main function
if __name__ == "__main__":
    main()

