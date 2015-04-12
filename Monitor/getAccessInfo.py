#!/usr/bin/python
import subprocess, os, glob, time


def get(sites=None):
  startTime = 1378008000
  endTime = int(time.time())
  sPerDay = 86400
  if not sites:
    sites = ['T2_BE_IIHE','T2_ES_IFCA','T2_IT_Pisa','T2_RU_PNPI','T2_US_Caltech','T2_BE_UCL','T2_FI_HIP','T2_IT_Rome','T2_RU_RRC_KI','T2_US_Florida','T2_BR_SPRACE','T2_FR_CCIN2P3','T2_KR_KNU','T2_RU_SINP','T2_US_MIT','T2_BR_UERJ','T2_FR_GRIF_IRFU','T2_PK_NCP','T2_TH_CUNSTDA','T2_US_Nebraska','T2_CH_CERN','T2_FR_GRIF_LLR','T2_PL_Swierk','T2_TR_METU','T2_US_Purdue','T2_CH_CSCS','T2_FR_IPHC','T2_PL_Warsaw','T2_TW_Taiwan','T2_US_UCSD','T2_CN_Beijing','T2_GR_Ioannina','T2_PT_NCG_Lisbon','T2_UA_KIPT','T2_US_Wisconsin','T2_DE_DESY','T2_HU_Budapest','T2_RU_IHEP','T2_UK_London_Brunel','T2_DE_RWTH','T2_IN_TIFR','T2_RU_INR','T2_UK_London_IC','T2_EE_Estonia','T2_IT_Bari','T2_RU_ITEP','T2_UK_SGrid_Bristol','T2_AT_Vienna','T2_ES_CIEMAT','T2_IT_Legnaro','T2_RU_JINR','T2_UK_SGrid_RALPP']
  for site in sites:
      for tStart in range(startTime,endTime,sPerDay):
          if os.path.exists(os.environ['MONITOR_DB']+'/sitesInfo/'+site+'/'+time.strftime("%Y-%m-%d",time.gmtime(tStart))):
            continue
          cmd = os.environ['DETOX_BASE'] + '/' + \
                            'popularityClient.py  /popularity/DSStatInTimeWindow/' + \
                            '\?\&sitename=' + site + '\&tstart=' + time.strftime("%Y-%m-%d",time.gmtime(tStart)) + '\&tstop=' + time.strftime("%Y-%m-%d",time.gmtime(tStart + sPerDay))
          print cmd
          process = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
          strout, error = process.communicate()
          with open(os.environ['MONITOR_DB']+'/sitesInfo/'+site+'/'+time.strftime("%Y-%m-%d",time.gmtime(tStart)),'w') as f:
              f.write(strout)