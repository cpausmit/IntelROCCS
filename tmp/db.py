#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Access to MIT database. IP address of machine where script is on need to have permission to access
# the database.
#
# Returns a list with tuples of the data requested. Example of what should be passed:
# query="SELECT * WHERE DatasetName=%s", values=["Dataset1"]
# Values are optional and can be any sequency which can be converted to a tuple.
# Example of returned data: [('Dataset1', 2), ('Dataset2', 5)]
#
# If we can't connect to the databse an exception is thrown.
# If a query fail we print an error message and return an empty list, leaving it up to caller to
# abort or keep executing.
#---------------------------------------------------------------------------------------------------
import os, MySQLdb, ConfigParser
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart
from email.Utils import formataddr
from subprocess import Popen, PIPE

class dbApi():
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), 'api.cfg'))
        self.fromEmail = config.items('from_email')[0]
        self.toEmails = config.items('error_emails')
        host = config.get('db', 'host')
        db = config.get('db', 'db')
        user = config.get('db', 'username')
        passwd = config.get('db', 'password')
        # Will try to connect 3 times before reporting an error
        for attempt in range(3):
            try:
                self.dbCon = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)
            except MySQLdb.Error, e:
                continue
            else:
                break
        else:
            self.error(e.args[1])

    def error(self, e):
        title = "FATAL IntelROCCS Error -- MIT Database"
        text = "FATAL -- %s" % (str(e))
        msg = MIMEMultipart()
        msg['Subject'] = title
        msg['From'] = formataddr(self.fromEmail)
        msg['To'] = self._toStr(self.toEmails)
        msg1 = MIMEMultipart("alternative")
        msgText1 = MIMEText("<pre>%s</pre>" % text, "html")
        msgText2 = MIMEText(text)
        msg1.attach(msgText2)
        msg1.attach(msgText1)
        msg.attach(msg1)
        msg = msg.as_string()
        p = Popen(["/usr/sbin/sendmail", "-toi"], stdin=PIPE)
        p.communicate(msg)
        raise Exception("FATAL -- %s" % (str(e)))

    def _toStr(self, toEmails):
        names = [formataddr(email) for email in toEmails]
        return ', '.join(names)

#===================================================================================================
#  M A I N   F U N C T I O N
#===================================================================================================
    def dbQuery(self, query, values=()):
        data = []
        e = ""
        values = tuple([str(value) for value in values])
        # Will try to call db 3 times before reporting an error
        for attempt in range(3):
            try:
                with self.dbCon:
                    cur = self.dbCon.cursor()
                    cur.execute(query, values)
                    for row in cur:
                        data.append(row)
            except MySQLdb.Error, err:
                e = err.args[1]
                continue
            except TypeError, err:
                e = err
                continue
            else:
                break
        else:
            self.error(e)
        return data
