import re 
import sys
import math
import time
import types
import string
import datetime

import gratia_reporting.make_table as make_table
import gratia_reporting.overflow_jobs_report as overflow_jobs_report

transfer_query = """
SELECT
  M.StartTime,
  T.SiteName,
  M.CommonName,
  M.RemoteSite,
  sum(M.Njobs),
  sum(M.TransferSize * Multiplier),
  M.TransferDuration
FROM
  MasterTransferSummary M,
  Probe P,
  Site T,
  SizeUnits su 
WHERE
  M.StorageUnit = su.Unit AND
  P.siteid = T.siteid AND
  M.ProbeName = P.Probename AND
  (StartTime >= %s AND StartTime <= %s) AND
  M.Protocol = "Xrootd"
GROUP BY M.StartTime, P.siteid, M.CommonName, M.RemoteSite
"""

def GB(bytes):
    try:
        return int(round(float(bytes)/1000.**3))
    except:
        return 'UNKNOWN'

def avg(sizes):
    if not sizes:
        return "UNKNOWN"
    return sum(sizes) / float(len(sizes))

def stddev(sizes):
    if not sizes:
        return "UNKNOWN"
    my_avg = avg(sizes)
    sum_dev = sum([(i-my_avg)**2 for i in sizes])
    return math.sqrt(sum_dev/float(len(sizes)))

split_re = re.compile("/CN=")
all_numbers = re.compile("\d")
def cnToUser(cn):
    components = split_re.split(cn)
    max_score = 0
    best = None
    for component in components:
        score = 0
        if all_numbers.match(component):
            continue
        len_score = len(component.split())
        if len_score == 1:
            len_score = len(component.split('-'))
        score += len_score
        if score > max_score:
            best = component
            max_score = score
    if max_score == 0:
        best = component[-1]
    result = []
    for word in best.split():
        word_0 = string.upper(word[0])
        word_rest = string.lower(word[1:])
        word_rest2 = string.upper(word[1:])
        if word[1:] == word_rest2:
            word = word_0 + word_rest
        else:
            word = word_0 + word[1:]
        if not all_numbers.match(word):
            result.append(word)
    result = ' '.join(result)
    return result

ip_re = re.compile("\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}")
def hostToDomain(host):
    if ip_re.match(host):
        # We assume the gratia probe already tried reverse DNS.  Punt
        return "Unknown"
    info = host.split(".")
    result = '.'.join(info[1:])
    if result:
        return result
    return "Unknown"

def compare_item_2(i1, i2):
    if i1[1] > i2[1]:
        return -1
    return i1[1] < i2[1]

class TransferInfo(object):

    def __init__(self, startdate, enddate, conn, log):
        self._db = conn
        self._startdate = startdate
        self._enddate = enddate
        self._log = log
        self._info = {}
        self.query()

    def _execute(self, stmt, *args):
        if isinstance(args, types.ListType) or isinstance(args,
                types.TupleType):
            args2 = tuple(['"%s"' % i for i in args])
            self._log.info(stmt % args2)
        elif isinstance(args, types.DictType):
            args2 = dict([(i, '"%s"' % j) for (i, j) in args.items()])
            self._log.info(stmt % args2)
        else:
            self._log.info(stmt % args)
        timer = -time.time()
        curs = self._db.cursor()
        curs.execute(stmt, args)
        timer += time.time()
        self._log.info("Query took %.2f seconds." % timer)
        return curs
    
    def query(self):
        start_date = self._startdate #date + " 00:00:00"
        end_date   = self._enddate #date + " 23:59:59"
        self.results = self._execute(transfer_query, start_date, end_date).fetchall()
        for result in self.results:
            date, site, cn, client, transfers, volume, duration = result
            key = (date, site, cn)
            info = self._info.setdefault(key, {})
            info['Date'] = date
            info['Site'] = site
            info['User'] = cnToUser(cn)
            info.setdefault('Transfers', 0)
            info['Transfers'] += int(transfers)
            info.setdefault('Volume', 0)
            clients = info.setdefault('Clients', {})
            domain = hostToDomain(client)
            clients.setdefault(hostToDomain(client), 0)
            clients[domain] += volume
            info['Volume'] += int(volume)
        for info in self._info.values():
            all_clients_sum = float(sum([i for i in info['Clients'].values()]))
            new_clients = []
            for domain, volume in info['Clients'].items():
                new_clients.append((domain, volume / all_clients_sum * 100))
            new_clients.sort(compare_item_2)
            info['Clients'] = new_clients

    def getInfo(self):
        return self._info

class Report(object):

    def __init__(self, conn, startDate, logger, cp):
        self._conn = conn
        self._startDate = startDate
        self._logger = logger
        self._cp = cp
        self._info = TransferInfo(startDate - datetime.timedelta(7, 0), 
            startDate, conn, logger)
        overflow_jobs_report.SetConnection(conn)

    def title(self):
        yesterday = self._startDate - datetime.timedelta(1, 0)
        yesterday_tot = 0
        today_tot = 0
        for key, val in self._info.getInfo().items():
            if key[0] == self._startDate:
                today_tot += val['Volume']
            elif key[0] == yesterday:
                yesterday_tot += val['Volume']
        today_tb = today_tot / 1000.0 / 1000.0
        yesterday_tb = yesterday_tot / 1000.0 / 1000.0
        increase_text = ""
        if yesterday_tb > 0:
            diff = (today_tb - yesterday_tb) / yesterday_tb
            if diff == 0:
                increase_text = "Unchanged"
            elif diff > 0:
                increase_text = "%d%% increase" % int(round(diff*100))
            else:
                increase_text = "%d%% decrease" % int(round(-diff*100))
        return 'Xrootd %s | %.2f TB | %s' % (self._startDate.strftime("%Y-%m-%d"), today_tb, increase_text)

    subject = title

    def generatePerUser(self):
        table = make_table.Table(add_numbers=False)
        table.setHeaders(['User', 'Volume GB', '# of Transfers', 'Source Site', 'Client Domain', 'Yesterday Diff', 'One Week Diff'])

        today_info = {}
        for key, val in self._info.getInfo().items():
            if key[0] == self._startDate:
                today_info[key[1:]] = val
        yesterday = self._startDate - datetime.timedelta(1, 0)
        oneweek = self._startDate - datetime.timedelta(7, 0)
        for key, val in self._info.getInfo().items():
            if key[0] == yesterday:
                alt_key = key[1:]
                if alt_key in today_info:
                    today_info[alt_key]['Yesterday'] = val['Volume']
            if key[0] == oneweek:
                alt_key = key[1:]
                if alt_key in today_info:
                    today_info[alt_key]['OneWeek'] = val['Volume']

        def cmpVolume(key1, key2):
            return -cmp(today_info[key1]['Volume'], today_info[key2]['Volume'])

        keys = today_info.keys()
        keys.sort(cmpVolume)
        for key in keys:
            val = today_info[key]
            first_row = True
            for client in val['Clients']:
                client_str = '%s %d%%' % (client[0], int(round(client[1])))
                if 'Yesterday' in val and val['Yesterday']:
                    yesterday = '%d%%' % int(round((val['Volume']-val['Yesterday']) / float(val['Yesterday'])*100))
                else:
                    yesterday = "Unknown"
                if 'OneWeek' in val and val['OneWeek']:
                    oneweek = '%d%%' % int(round((val['Volume']-val['OneWeek']) / float(val['OneWeek'])*100))
                else:
                    oneweek = "Unknown"
                if first_row:
                    table.addRow([val['User'], int(round(val['Volume']/1000.)), val['Transfers'], val['Site'], client_str, yesterday, oneweek])
                else:
                    table.addRow(["", "", "", "", client_str, "", ""])
                first_row = False

        return table.plainText()

    def generatePerSiteClient(self):
        table = make_table.Table(add_numbers=False)
        table.setHeaders(['Source Site', 'Client Domain', 'Volume GB', 'Yesterday Diff', 'One Week Diff'])

        siteclient_info = {}
        yesterday = self._startDate - datetime.timedelta(1, 0)
        oneweek = self._startDate - datetime.timedelta(7, 0)
        for key, val in self._info.getInfo().items():
            (date, site, cn) = key
            for client in val.get('Clients', []):
                new_key = (site, client[0])
                if date == self._startDate:
                    info = siteclient_info.setdefault(new_key, {'Volume': 0, 'Transfers': 0, 'Site': site})
                    info['Volume'] += val['Volume']*client[1]/100.
                elif date == yesterday:
                    info = siteclient_info.setdefault(new_key, {'Volume': 0, 'Transfers': 0, 'Site': site})
                    info.setdefault('Yesterday', 0)
                    info['Yesterday'] += val['Volume'] * client[1]/100.
                elif date == oneweek:
                    info = siteclient_info.setdefault(new_key, {'Volume': 0, 'Transfers': 0, 'Site': site})
                    info.setdefault('OneWeek', 0)
                    info['OneWeek'] += val['Volume'] * client[1]/100.

        keys = siteclient_info.keys()
        def cmpSiteClient(key1, key2):
            return cmp(siteclient_info[key1]['Volume'], siteclient_info[key2]['Volume'])
        keys.sort()
        for key in keys:
            val = siteclient_info[key]
            if 'Yesterday' in val and val['Yesterday']:
                yesterday = '%d%%' % int(round((val['Volume']-val['Yesterday']) / float(val['Yesterday'])*100))
            else:
                yesterday = "Unknown"
            if 'OneWeek' in val and val['OneWeek']:
                oneweek = '%d%%' % int(round((val['Volume']-val['OneWeek']) / float(val['OneWeek'])*100))
            else:
                oneweek = "Unknown"
            table.addRow([val['Site'], key[1], int(round(val['Volume']/1000)), yesterday, oneweek])

        return table.plainText()

    def generatePerSite(self):
        table = make_table.Table(add_numbers=False)
        table.setHeaders(['Source Site', 'Volume GB', '# of Transfers', 'Yesterday Diff', 'One Week Diff'])

        site_info = {}
        yesterday = self._startDate - datetime.timedelta(1, 0)
        oneweek = self._startDate - datetime.timedelta(7, 0)
        for key, val in self._info.getInfo().items():
            (date, site, cn) = key
            if date == self._startDate:
                info = site_info.setdefault(key[1], {'Volume': 0, 'Transfers': 0, 'Site': site})
                info['Volume'] += val['Volume']
                info['Transfers'] += val['Transfers']
            elif date == yesterday:
                info = site_info.setdefault(key[1], {'Volume': 0, 'Transfers': 0, 'Site': site})
                info.setdefault('Yesterday', 0)
                info['Yesterday'] += val['Volume']
            elif date == oneweek:
                info = site_info.setdefault(key[1], {'Volume': 0, 'Transfers': 0, 'Site': site})
                info.setdefault('OneWeek', 0)
                info['OneWeek'] += val['Volume']

        keys = site_info.keys()
        keys.sort()
        for key in keys:
            val = site_info[key]
            if 'Yesterday' in val and val['Yesterday']:
                yesterday = '%d%%' % int(round((val['Volume']-val['Yesterday']) / float(val['Yesterday'])*100))
            else:
                yesterday = "Unknown"
            if 'OneWeek' in val and val['OneWeek']:
                oneweek = '%d%%' % int(round((val['Volume']-val['OneWeek']) / float(val['OneWeek'])*100))
            else:
                oneweek = "Unknown"
            table.addRow([val['Site'], int(round(val['Volume']/1000)), val['Transfers'], yesterday, oneweek])

        return table.plainText()

    def generatePlain(self):
        text = '%s\n  %s\n%s\n\n' % ('='*60, self.title(), '='*60)

        text += self.generatePerSite() + "\n"

        try:
            text += overflow_jobs_report.mainGetOverflowjobsInfo1() + "\n"
        except Exception, e:
            text += "(An error occurred when generating this portion of the report)\n"
            self._logger.exception(e)

        text += self.generatePerSiteClient() + "\n"
       
        text += self.generatePerUser()

        try:
            text += overflow_jobs_report.mainGetOverflowjobsInfo2()
        except Exception, e:
            text += "(An error occurred when generating this portion of the report)\n"
            self._logger.exception(e)

        self._logger.info("\n" + text)
        return text

    def generateHtml(self):
        return '<pre>\n%s\n</pre>\n' % self.generatePlain()

    def name(self):
        return "site_storage_report"
