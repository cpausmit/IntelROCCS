#!/usr/bin/env python

requests_dir = '/home/snarayan/scratch5/requests_dir/'
outdir = '/home/snarayan/public_html/IntelROCCS/'
style = '/home/cmsprod/MitRootStyle/MitRootStyle.C'

site_pattern = 'T1.*'
plot_site_pattern = 'T1.*'
root_name = 'T1'

dataset_pattern = '/.*/.*/.*AOD.*'
name = 'T12_XAODX'

#plot_dataset_pattern = '/.*/.*/MINIAOD.*'
#plot_name = 'T2_MINIAODX'

#plot_dataset_pattern = '/.*/.*/.*AOD.*'
#plot_name = 'T2_XAODX'

#plot_dataset_pattern = '/.*/.*/.*AODSIM'
#plot_name = 'T2_XAODSIM'

#plot_dataset_pattern = '/.*/.*/.*AOD$'
#plot_name = 'T2_XAOD'

dataset_pattern = '/.*/.*/RECO'
name = 'T12_RECO'
plot_dataset_pattern = '/.*/.*/RECO'
plot_name = 'T2_RECO'


sites = [
        'T2_BE_IIHE','T2_ES_IFCA','T2_IT_Pisa','T2_RU_PNPI','T2_US_Caltech','T2_BE_UCL','T2_FI_HIP',
        'T2_IT_Rome','T2_RU_RRC_KI','T2_US_Florida','T2_BR_SPRACE','T2_FR_CCIN2P3','T2_KR_KNU','T2_RU_SINP',
        'T2_US_MIT','T2_BR_UERJ','T2_FR_GRIF_IRFU','T2_PK_NCP','T2_TH_CUNSTDA','T2_US_Nebraska','T2_CH_CERN',
        'T2_FR_GRIF_LLR','T2_PL_Swierk','T2_TR_METU','T2_US_Purdue','T2_CH_CSCS','T2_FR_IPHC','T2_PL_Warsaw',
        'T2_TW_Taiwan','T2_US_UCSD','T2_CN_Beijing','T2_GR_Ioannina','T2_PT_NCG_Lisbon','T2_UA_KIPT',
        'T2_US_Wisconsin','T2_DE_DESY','T2_HU_Budapest','T2_RU_IHEP','T2_UK_London_Brunel','T2_DE_RWTH',
        'T2_IN_TIFR','T2_RU_INR','T2_UK_London_IC','T2_EE_Estonia','T2_IT_Bari','T2_RU_ITEP',
        'T2_UK_SGrid_Bristol','T2_AT_Vienna','T2_ES_CIEMAT','T2_IT_Legnaro','T2_RU_JINR','T2_UK_SGrid_RALPP',
        "T1_UK_RAL_Disk", "T1_US_FNAL_Disk", "T1_IT_CNAF_Disk", "T1_DE_KIT_Disk", "T1_RU_JINR_Disk", 
        "T1_FR_CCIN2P3_Disk", "T1_ES_PIC_Disk"
                    ]

