#===================================================================================================
#  C L A S S
#===================================================================================================
import os, re, sys
import datetime

class SiteReadiness:
    def __init__(self,siteName):
        self.siteName = siteName
        self.status = {}
        self.wasEverDead = False
        self.wasEverWaiting = False
        self.epochDate = datetime.date(2000,1,1)

    def update(self,state,status,timest):
        self.status[timest] = (state,status)
        if status == 'in':
            if state == 0:
                self.wasEverDead = True
            if state == 1:
                self.wasEverWaiting = True

    def prStatus(self):
        print self.siteName
        for timest in sorted(self.status):
            print str(timest) + ":" + str(self.status[timest][0]) + ' ' + self.status[timest][1] 

    def hadProblems(self):
        if self.wasEverDead or self.wasEverWaiting:
            return True
        return False

    def isDead(self):
        return self.isInState(0)

    def inWaitingRoom(self):
        return self.isInState(1)

    def isInState(self,state):
        for timest in sorted(self.status,reverse=True):
            place = self.status[timest][0]
            status = self.status[timest][1]
            if place == state:
                if status == 'in':
                    return True
                else:
                    return False
        return False

    def wasDead(self,timeStampStr):
        return self.wasInState(timeStampStr,0)

    def wasInWaitingRoom(self,timeStampStr):
       return self.wasInState(timeStampStr,1)

    def wasInState(self,timeStampStr,state):
        timeStamp = datetime.datetime.strptime(timeStampStr,"%Y-%m-%d").date()
        for timest in sorted(self.status,reverse=True):
            place = self.status[timest][0]
            status = self.status[timest][1]
            if place != state:
                continue
            if timeStamp > timest:
                if status == 'in':
                    return True
                else:
                    return False
        return False

    def lastTimeDead(self):
       return self.lastTime(0)

    def lastTimeInWaitingRoom(self):
       return self.lastTime(1)


    def declaredDead(self):
        for timest in sorted(self.status,reverse=True):
            place = self.status[timest][0]
            status = self.status[timest][1]
            if place != 0:
                continue
            if status == 'in':
                return timest
        return None


    def lastTime(self,state):
        lastTimeIn = None
        lastTimeOut = None
        for timest in sorted(self.status,reverse=True):
            place = self.status[timest][0]
            status = self.status[timest][1]
            if place != state:
                continue
            if status == 'out':
                lastTimeOut = timest
            if status == 'in':
                lastTimeIn = timest
                break
        if lastTimeIn == None:
            return self.epochDate
        if lastTimeOut == None:
            return datetime.datetime.now()
        return (lastTimeOut -  datetime.timedelta(days=1))

    def printResults(self):
        isDead = False
        isInwr = False
        for timest in sorted(self.status):
            state = self.status[timest][0]
            status = self.status[timest][1]
            if isDead == False and status == 'in' and state == 0:
                print " - in morgue since    " + str(timest)
                isDead = True
            if isInwr == False and status == 'in' and state == 1:
                print " - in waiting since   " + str(timest)
                isInwr = True
            if isDead and status == 'out' and state == 0:
                print " -- left morgue       " + str(timest)
                isDead = False
            if isInwr and status == 'out' and state == 1:
                print " -- left waiting room " + str(timest)
                isInwr = False
            
