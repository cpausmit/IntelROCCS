#!/usr/bin/python
import subprocess, os, glob, time
from sys import exit # debugging

def get(sites=None,startTime=1378008000):
  certPath = os.environ['USERCERT']
  keyPath = os.environ['USERKEY']
  endTime = int(time.time())
  sPerDay = 86400
  if not sites:
    sites = ['T2_BE_IIHE','T2_ES_IFCA','T2_IT_Pisa','T2_RU_PNPI','T2_US_Caltech','T2_BE_UCL','T2_FI_HIP',
    			'T2_IT_Rome','T2_RU_RRC_KI','T2_US_Florida','T2_BR_SPRACE','T2_FR_CCIN2P3','T2_KR_KNU','T2_RU_SINP',
    			'T2_US_MIT','T2_BR_UERJ','T2_FR_GRIF_IRFU','T2_PK_NCP','T2_TH_CUNSTDA','T2_US_Nebraska','T2_CH_CERN',
    			'T2_FR_GRIF_LLR','T2_PL_Swierk','T2_TR_METU','T2_US_Purdue','T2_CH_CSCS','T2_FR_IPHC','T2_PL_Warsaw',
    			'T2_TW_Taiwan','T2_US_UCSD','T2_CN_Beijing','T2_GR_Ioannina','T2_PT_NCG_Lisbon','T2_UA_KIPT','T2_US_Wisconsin',
    			'T2_DE_DESY','T2_HU_Budapest','T2_RU_IHEP','T2_UK_London_Brunel','T2_DE_RWTH','T2_IN_TIFR','T2_RU_INR','T2_UK_London_IC',
    			'T2_EE_Estonia','T2_IT_Bari','T2_RU_ITEP','T2_UK_SGrid_Bristol','T2_AT_Vienna','T2_ES_CIEMAT','T2_IT_Legnaro',
    			'T2_RU_JINR','T2_UK_SGrid_RALPP']
    sites += ["T1_UK_RAL_Disk", "T1_US_FNAL_Disk", "T1_IT_CNAF_Disk", "T1_DE_KIT_Disk", "T1_RU_JINR_Disk", "T1_FR_CCIN2P3_Disk", "T1_ES_PIC_Disk"]
  for site in sites:
      for tStart in range(startTime,endTime,sPerDay):
          if (endTime - tStart > 3*sPerDay) and (os.path.exists(os.environ['MONITOR_DB']+'/sitesInfo/'+site+'/'+time.strftime("%Y-%m-%d",time.gmtime(tStart)))):
            # always refresh anything within last 3 days in case there was a reporting failure
            continue
          pop_base_url = "https://cmsweb.cern.ch/popdb/"
          args  = '/popularity/DSStatInTimeWindow/' 
          args += '?&sitename=' + site.replace('_Disk','')
          args += '&tstart=' + time.strftime("%Y-%m-%d",time.gmtime(tStart)) 
          args += '&tstop=' + time.strftime("%Y-%m-%d",time.gmtime(tStart + sPerDay))
          pop_url = '%s/%s'%(pop_base_url,args)
          fOutName = os.environ['MONITOR_DB']+'/sitesInfo/'+site+'/'+time.strftime("%Y-%m-%d",time.gmtime(tStart))
          flags = '--ca-directory=/home/snarayan/certs --certificate=%s --private-key=%s'%(certPath,keyPath)
          cmd = "wget -O %s %s '%s'"%(fOutName,flags,pop_url)
          print cmd
          os.system(cmd)
