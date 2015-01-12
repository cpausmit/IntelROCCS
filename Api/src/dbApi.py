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
import sys, os, MySQLdb, datetime, subprocess, ConfigParser

class dbApi():
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read('/usr/local/IntelROCCS/DataDealer/intelroccs.test.cfg')
        host = config.get('DB', 'host')
        db = config.get('DB', 'db')
        user = config.get('DB', 'username')
        passwd = config.get('DB', 'password')
        for attempt in range(3):
            try:
                self.dbCon = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)
            except MySQLdb.Error, e:
                continue
            else:
                break
        else:
            self.error(e, host, db)

    def error(self, e, host, db):
        title = " FATAL IntelROCCS Error -- MIT Database"
        text = " FATAL (%s - %s) -- Could not connect to db %s:%s" % (str(e.args[0]), str(e.args[1]), host, db)
        fromEmail = ("Bjorn Barrefors", "bjorn.peter.barrefors@cern.ch")
        toList = (["Bjorn Barrefors"], ["barrefors@gmail.com"])
        msg = MIMEMultipart()
        msg['Subject'] = title
        msg['From'] = formataddr(fromEmail)
        msg['To'] = self._toStr(toList)
        msg1 = MIMEMultipart("alternative")
        msgText1 = MIMEText("<pre>%s</pre>" % text, "html")
        msgText2 = MIMEText(text)
        msg1.attach(msgText2)
        msg1.attach(msgText1)
        msg.attach(msg1)
        msg = msg.as_string()
        p = Popen(["/usr/sbin/sendmail", "-toi"], stdin=PIPE)
        p.communicate(msg)
        raise Exception("FATAL (%s - %s) -- Could not connect to db %s:%s" % (str(e.args[0]), str(e.args[1]), host, db))

#===================================================================================================
#  M A I N   F U N C T I O N
#===================================================================================================
    def dbQuery(self, query, values=()):
        data = []
        values = tuple([str(value) for value in values])
        for attempt in range(3):
            try:
                mysqlError = 0
                typeError = 0
                with self.dbCon:
                    cur = self.dbCon.cursor()
                    cur.execute(query, values)
                    for row in cur:
                        data.append(row)
            except MySQLdb.Error, e:
                mysqlError = 1
                continue
            except TypeError, e:
                typeError = 1
                continue
            else:
                break
        else:
            if mysqlError:
                print("ERROR -- %s -- \tfor query: %s -- \t and values: %s\n" % (str(e.args[1]), str(query), str(values)))
            else:
                print("ERROR -- %s -- \tfor  query: %s -- \t and values: %s\n" % (str(e), str(query), str(values)))
        return data

