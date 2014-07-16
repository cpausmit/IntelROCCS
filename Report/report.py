#!/usr/bin/env python
#
# $Id$

import os
import sys
import time
import urllib2
import traceback
import optparse
import logging
import smtplib
import commands
import optparse
import datetime
import logging.config
import ConfigParser
from logging.handlers import RotatingFileHandler
from email.MIMEText import MIMEText
from email.MIMEImage import MIMEImage
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.Utils import formataddr
from email.quopriMIME import encode

import MySQLdb

if not '.' in sys.path:
    sys.path.append('.')

def getValidDate(date):
    """
    Check that this is a correctly formatted date and time.
    If the data are okay, return the date-time string.
    Otherwise, raise an exception to halt proceedings.
    """
    time_t = time.strptime(date, '%Y/%m/%d')
    return datetime.datetime(*time_t[:6])

def parseArguments():
    """
    Parse the command-line arguments and return the values as a tuple.
    """
    parser = optparse.OptionParser()
    parser.add_option("-l", "--loglevel", dest="loglevel", default="info", \
        help="The level of logging verbosity")
    parser.add_option("-c", "--config", dest="config", default="", \
        help="Comma-separated list of configuration files to use.")
    parser.add_option("-s", "--startDate", dest="startDate", default=None, \
        help="Start date and time (format: 2008/08/04 22:34:59)")
    parser.add_option("-e", "--endDate", dest="endDate", default=None, \
        help="End date and time (format: 2008/08/04 22:34:59)")
    parser.add_option("-d", "--db", dest="db", default=None, \
        help="Name of the database entry to use.")
    parser.add_option("-r", "--relDate", dest="rel", default=None, \
        help="Relative date to use (today|yesterday|week|month)")
    parser.add_option('-n', '--name', dest='name', default=None,
        help="Gratia Report name")
    parser.add_option("-D", "--dev", dest="dev", default=False,
        action="store_true", help="Development protection flag.")
    options, args = parser.parse_args()

    if options.rel != None:
        if options.rel == "today" or options.rel == "yesterday":
            if options.rel == "today":
                today = datetime.date.today()
            else:
                today = datetime.date.today() - datetime.timedelta(1, 0)
            year = today.year
            month = today.month
            day = today.day
            options.startDate = datetime.datetime(year, month, day, 0, 0, 0)
            options.endDate = datetime.datetime(year, month, day, 23, 59, 59)
        elif options.rel == 'week':
            today = datetime.date.today()
            weekday = today.isoweekday()
            start = today - datetime.timedelta(weekday, 0)
            year = start.year
            month = start.month
            day = start.day
            options.startDate = datetime.datetime(year, month, day, 0, 0, 0)
            options.endDate = today
        elif options.rel == 'lastweek':
            today = datetime.date.today()
            weekday = today.isoweekday()
            s = today - datetime.timedelta(weekday+7, 0)
            e = today - datetime.timedelta(weekday, 0)
            options.startDate = datetime.datetime(s.year, s.month, s.day,0,0,0)
            options.endDate = datetime.datetime(e.year, e.month, e.day, 0, 0, 0)
        elif options.rel == 'month':
            today = datetime.date.today()
            year = today.year
            month = today.month
            options.endDate = today
            options.startDate = datetime.datetime(year, month, 1, 0, 0, 0)
        elif options.rel == 'lastmonth':
            today = datetime.date.today()
            year = today.year
            month = today.month
            options.startDate = datetime.datetime(year, month-1, 1, 0, 0, 0)
            options.endDate = datetime.datetime(year, month, 1, 0, 0, 0) - \
                datetime.timedelta(0, 1)
        else:
            raise Exception("Unknown relative date option: %s" % options.rel)
        options.startDate = options.startDate.strftime('%Y/%m/%d')
        options.endDate = options.endDate.strftime('%Y/%m/%d')

    loglevel = options.loglevel
    loglevel = getattr(logging, loglevel.upper())

    # The last 4 args should be the start and end dates for the report.
    # Each date has the form yyyy/mm/dd hh:mm:ss and is UTC (aka GMT)
    startDate = getValidDate(options.startDate)
    endDate = getValidDate(options.endDate)
    return loglevel, startDate, endDate, options

def _toStr(toList):
    names = [formataddr(i) for i in zip(*toList)]
    return ', '.join(names)

def saveFile(cp, startDate, name, html):
    """
    Save the HTML form of the report to a file on-disk.
    """
    try:
        archive_dir = cp.get("Gratia", "report_archive_directory")
    except:
        return
    archive_dir = os.path.join(archive_dir, startDate.strftime('%Y/%m/%d'))
    try:
        os.makedirs(archive_dir)
    except OSError, oe:
        if oe.errno != 17:
            raise
    html = "<html><head><title>%s</title></head><body>%s</body>" \
        "</html>" % ("Gratia Report for %s" % time.strftime("%Y-%m-%d"), html)
    filename = "%s-%s.html" % (name, startDate.strftime('%Y-%m-%d'))
    fp = open(os.path.join(archive_dir, filename), 'w')
    fp.write(html)
    fp.close()

def sendEmail( fromEmail, toList, smtpServerHost, subject, reportText, \
        reportHtml, log ):
    """
    This turns the "report" into an email attachment
    and sends it to the EmailTarget(s).
    """
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = formataddr(fromEmail)
    msg["To"] = _toStr(toList)
    msg1 = MIMEMultipart("alternative")
    msgText1 = MIMEText("<pre>%s</pre>" % reportText, "html")
    msgText2 = MIMEText(reportText)
    msg1.attach(msgText2)
    msg1.attach(msgText1)
    msg.attach(msg1)
    msg = msg.as_string()

    log.debug("Report message:\n\n%s" % msg)
    if len(toList[1]) != 0:
        server = smtplib.SMTP(smtpServerHost)
        server.sendmail(fromEmail[1], toList[1], msg)
        server.quit()
    else:
        # The email list isn't valid, so we write it to stdout and hope
        # it reaches somebody who cares.
        print message

def databaseConnection(cp):
    """
    Connect to the Gratia database.
    Given a ConfigParser, look in the [Gratia] section for the database to use.
    Then, use the information in the [<dbname>] section to find the login
    information

    @param cp: ConfigParser object containing the configuration.
    @returns: Database connection object
    """
    section = cp.get("Gratia", "database")
    info = {}
    _add_if_exists(cp, section, "user", info)
    _add_if_exists(cp, section, "passwd", info)
    _add_if_exists(cp, section, "db", info)
    _add_if_exists(cp, section, "host", info)
    _add_if_exists(cp, section, "port", info)
    if 'port' in info:
        info['port'] = int(info['port'])
    return MySQLdb.connect(**info)


def _add_if_exists(cp, section, attribute, info):
    """
    If section.attribute exists in the config file, add its value into a
    dictionary.

    @param cp: ConfigParser object representing our config
    @param section: Section name in cp.
    @param attribute: Attribute in section.
    @param info: Dictionary that we add data into.
    """
    try:
        info[attribute] = cp.get(section, attribute)
    except:
        pass

def main():
    status = 0
    logger = None
    logger = logging.getLogger()
    ProgramName = sys.argv[0]
    try:

        # Get the command line arguments. It throws if they are invalid.
        (loglevel, startDate, endDate, options) = parseArguments()

        configFiles = options.config
        if configFiles == "":
            configFiles = '/etc/gratia_reporting/reporting.cfg'
        configFiles = [i.strip() for i in configFiles.split(',')]
        cp = ConfigParser.ConfigParser()
        cp.read(configFiles)

        if options.db:
            cp.set("Gratia", "database", options.db)

        logging.config.fileConfig(cp.get("Gratia", "logging_config"))
        logger = logging.getLogger()
        if loglevel:
            logger.setLevel(loglevel)

        # Get email information:
        fromName = cp.get("Report Info", "fromName")
        fromEmail = cp.get("Report Info", "fromEmail")
        EmailFromAddress = (fromName, fromEmail)
        toNames = eval(cp.get("Report Info", "toNames"), {}, {})
        toEmails = eval(cp.get("Report Info", "toEmails"), {}, {})
        if len(toNames) > 1 and options.dev:
            raise Exception("Unable to send emails to more than 1 person in" \
                " development mode.")
        EmailToAddresses = (toNames, toEmails)
        SMTPServerHost = cp.get("Report Info", "smtphost")

        # Connect to the database. 
        logger.info("Connecting to RSV DB.")
        conn = databaseConnection(cp)

        logger.info("About to query RSV DB.")

        my_version=None
        if options.name:
            report_module = __import__('gratia_reporting.report_%s' % \
                options.name)
            report_module = getattr(report_module, "report_%s" % options.name)
            report = report_module.Report(conn, startDate, logger, cp)
        else:
            print >> sys.stderr, "No report specified."
            return 2
            

        logger.info("About to send email.")
        html = report.generateHtml()
        sendEmail(EmailFromAddress,
                  EmailToAddresses,
                  SMTPServerHost,
                  report.subject(),
                  report.generatePlain(),
                  html,
                  logger)
        saveFile(cp, startDate, report.name(), html)
    except (SystemExit, KeyboardInterrupt):
        raise
    except Exception:
        # Log the problem and die.
        # format the traceback into a string
        tblist = traceback.format_exception(sys.exc_type,
                                            sys.exc_value,
                                            sys.exc_traceback)
        msg = ProgramName + " caught an exception:\n" + "".join(tblist)
        logger.error(msg)
        print >> sys.stderr, msg
        status = 1

    # Shut down the logger to ensure nothing is lost.
    logger.critical(ProgramName + " shutting down.")
    logging.shutdown()
    return status

if __name__ == '__main__':
    sys.exit(main())