#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Check and update progress of subscriptions
#---------------------------------------------------------------------------------------------------
import os, datetime, ConfigParser
import makeTable, dbApi, phedexApi

class subscriptionProgress():
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), 'intelroccs.cfg'))
        self.progressPath = config.get('DataDealer', 'progress_path')
        self.dbApi = dbApi.dbApi()
        self.phedexApi = phedexApi.phedexApi()

    def updateProgress(self):
        query = "SELECT Requests.RequestId, Requests.DatasetId, Datasets.DatasetName FROM Requests INNER JOIN Datasets ON Requests.DatasetId=Datasets.DatasetId WHERE Requests.Progress < %s AND Requests.RequestType=%s"
        values = [100, 0]
        data = self.dbApi.dbQuery(query, values=values)
        for row in data:
            jsonData = self.phedexApi.subscriptions(dataset=row[2], request=row[0])
            dataset = jsonData.get('phedex').get('dataset')[0]
            subscription = dataset.get('subscription')[0]
            progress = subscription.get('percent_bytes')
            if progress:
                progress = int(progress)
            else:
                progress = 0
            query = "UPDATE Requests SET Progress=%s WHERE RequestId=%s AND DatasetId=%s"
            values = [progress, row[0], row[1]]
            self.dbApi.dbQuery(query, values=values)

    def checkProgress(self):
        today = datetime.date.today()
        date = today - datetime.timedelta(days=7)
        title = 'AnalysisOps %s | Subscription Progress' % (today.strftime('%Y-%m-%d'))
        text = '%s\n  %s\n%s\n\n' % ('='*68, title, '='*68)

        # Get incomplete subscriptions older than 7 days
        text += "\n\nIncomplete subscriptions made more than 7 days ago\n\n"
        query = "SELECT Requests.Date, Requests.RequestId, Sites.SiteName, Requests.Progress, Datasets.DatasetName FROM Requests INNER JOIN Datasets ON Requests.DatasetId=Datasets.DatasetId INNER JOIN Sites ON Requests.SiteId=Sites.SiteId WHERE Requests.Progress<%s AND Requests.RequestType=%s AND Requests.Date<=%s"
        values = [100, 0, date.strftime('%Y-%m-%d %H:%M:%S')]
        data = self.dbApi.dbQuery(query, values=values)
        oldTable = makeTable.Table(add_numbers=False)
        oldTable.setHeaders(['Date', 'Request', 'Site', 'Progress', '(code) ETA', 'Dataset'])
        for row in data:
            code = 0
            eta = 0
            jsonData = self.phedexApi.blockArrive(dataset=row[4], to_node=row[2])
            blocks = jsonData.get('phedex').get('block')
            for block in blocks:
                dest = block.get('destination')[0]
                tmpEta = dest.get('time_arrive')
                if not tmpEta:
                    eta = "null"
                    code = dest.get('basis')
                    break
                if tmpEta > eta:
                    eta = tmpEta
                    code = dest.get('basis')
            if not (eta == "null"):
                eta = datetime.datetime.fromtimestamp(float(eta))
            oldTable.addRow([row[0], row[1], row[2], str(row[3]) + "%", "(" + str(code) + ") " + str(eta), row[4]])
        text += oldTable.plainText()

        # Get incomplete subscriptions made less than 7 days ago
        text += "\n\nIncomplete subscriptions made less than 7 days and more than 1 day ago\n\n"
        query = "SELECT Requests.Date, Requests.RequestId, Sites.SiteName, Requests.Progress, Datasets.DatasetName FROM Requests INNER JOIN Datasets ON Requests.DatasetId=Datasets.DatasetId INNER JOIN Sites ON Requests.SiteId=Sites.SiteId WHERE Requests.Progress<%s AND Requests.RequestType=%s AND Requests.Date>%s"
        values = [100, 0, date.strftime('%Y-%m-%d %H:%M:%S')]
        data = self.dbApi.dbQuery(query, values=values)
        newTable = makeTable.Table(add_numbers=False)
        newTable.setHeaders(['Date', 'Request', 'Site', 'Progress', '(code) ETA', 'Dataset'])
        for row in data:
            code = 0
            eta = 0
            jsonData = self.phedexApi.blockArrive(dataset=row[4], to_node=row[2])
            blocks = jsonData.get('phedex').get('block')
            for block in blocks:
                dest = block.get('destination')[0]
                tmpEta = dest.get('time_arrive')
                if not tmpEta:
                    eta = "null"
                    code = dest.get('basis')
                    break
                if tmpEta > eta:
                    eta = tmpEta
                    code = dest.get('basis')
            if not (eta == "null"):
                eta = datetime.datetime.fromtimestamp(float(eta))
            newTable.addRow([row[0], row[1], row[2], str(row[3]) + "%", "(" + str(code) + ") " + str(eta), row[4]])
        text += newTable.plainText()

        text += "\n\nCodes\n\nNegative values are for blocks which are not expected to complete without external intervention. Non-negative values are for blocks which are expected to complete.\n-6 : at least one file in the block has no source replica remaining\n-5 : for at least one file in the block, there is no path from source to destination\n-4 : subscription was automatically suspended by router for too many failures\n-3 : there is no active download link to the destination\n-2 : subscription was manually suspended\n-1 : block is still open\n0 : all files in the block are currently routed. FileRouter estimate is used to calculate ETA\n1 : the block is not yet routed because the destination queue is full\n2 : at least one file in the block is currently not routed, because it recently failed to transfer, and is waiting for rerouting"

        # print progress report
        fs = open('%s/data_dealer-%s.report' % (self.progressPath, today.strftime('%Y%m%d')), 'w')
        fs.write(text)
        fs.close()
