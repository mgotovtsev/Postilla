import os
import sys
import re
import codecs
from datetime import date, datetime, timedelta

sScriptsDir    = os.path.dirname(sys.argv[0])
sRootDir       = os.path.dirname(sScriptsDir)

sSourceDir       = os.path.join(sRootDir, '01_DataBase')
sScriptsDir      = os.path.join(sRootDir, '02_Scripts')
sLogsDir         = os.path.join(sRootDir, '03_Logs')
sUtilitesDir     = os.path.join(sRootDir, '04_Utilites')
sConfDir         = os.path.join(sScriptsDir, 'config')

sUrl = r'http://pikabu.ru/new'


class XmlConfigFile():
    def __init__(self):
        self.sPikabuConfigXmlFileName = 'parser_pikabu.xml'
        self.sPikabuXMLFilePath = os.path.join(sConfDir, self.sPikabuConfigXmlFileName)

        self.sInfoCollectorConfigXmlFileName = 'info_collector.xml'
        self.sInfoCollectorXMLFilePath = os.path.join(sConfDir, self.sInfoCollectorConfigXmlFileName)

        self.sPostCleanerConfigXmlFileName = 'post_cleaner.xml'
        self.sPostCleanerXMLFilePath = os.path.join(sConfDir, self.sPostCleanerConfigXmlFileName)

        self.sPostPosterConfigXmlFileName = 'post_poster.xml'
        self.sPostPosterXMLFilePath = os.path.join(sConfDir, self.sPostPosterConfigXmlFileName)

        self.sPostPosterConfigCsvFileName = 'PostShedule.csv'
        self.sPostPosterCsvFilePath = os.path.join(sConfDir, self.sPostPosterConfigCsvFileName)

class LogPaths():
    def __init__(self):
        self.sCommonProcessErrorsLogPath  = os.path.join(sLogsDir, "CommonProcessErrorsLog.log")
        self.sParserPikabuLogPath  = os.path.join(sLogsDir, "parser_Pikabu.log")
        self.sInfoCollectorLogPath  = os.path.join(sLogsDir, "info_collector.log")
        self.sPostCleanerLogPath  = os.path.join(sLogsDir, "post_cleaner.log")
        self.sPostPosterLogPath  = os.path.join(sLogsDir, "post_poster.log")

class ParserParameters():
    def __init__(self):
        self.ParseDelaySec   = "ParseDelaySec"
        self.isPausedProcess = "isPausedProcess"
        self.isWork          = "isWork"
        self.InfoCollectorThreadCnt    = "InfoCollectorThreadCnt"
        self.LowerBorderRatingPost     = 'LowerBorderRatingPost'
        self.LowerBrdrUpdateCntPost    = 'LowerBrdrUpdateCntPost'
        self.BackupBorderRatingPost    = 'BackupBorderRatingPost'
        self.BackupDayPost             = 'BackupDayPost'
        self.MaxPostsCntBeforeShrink   = 'MaxPostsCntBeforeShrink'
        self.ShrinkFactor              = 'ShrinkFactor'
        self.ShrinkRatingDivisor       = 'ShrinkRatingDivisor'

class Post():
    def __init__(self):
        self.DbId             = 0
        self.PstSiteId        = 0
        self.PstVotes         = 0
        self.PstCmntCnt       = 0
        self.PstDateTime      = datetime.now()

        self.PstPosVotesCnt   = 0
        self.PstNegVotesCnt   = 0
        self.PstPosCmntRtng   = 0
        self.PstNegCmntRtng   = 0
        self.PstPosCmntCnt    = 0
        self.PstNegCmntCnt    = 0
        self.PstNeuCmntCnt    = 0

        self.PstFbLkCnt       = 0
        self.PstVkLkCnt       = 0
        self.PstTwLkCnt       = 0
        self.PstSvCnt         = 0

        self.PstVotesPerSec   = 0.0
        self.PstCommentPerSec = 0.0
        self.PstSiteName      = ''
        self.PstHref          = ''
        self.PstTitle         = ''
        self.PstAuthorNick    = ''
        self.PstAuthorHref    = ''
        self.listTags         = []

    def GetVotesPerSec(self):
        NowDateTime         = datetime.now()
        DeltaDayTime        = NowDateTime - self.PstDateTime
        DeltaDayTimeSec     = DeltaDayTime.total_seconds()
        if DeltaDayTimeSec != 0:
            self.PstVotesPerSec = float(self.PstVotes) / float(DeltaDayTimeSec)

    def GetCommentPerSec(self):
        NowDateTime         = datetime.now()
        DeltaDayTime        = NowDateTime - self.PstDateTime
        DeltaDayTimeSec     = DeltaDayTime.total_seconds()
        if DeltaDayTimeSec != 0:
            self.PstCommentPerSec = float(self.PstCmntCnt) / float(DeltaDayTimeSec)

class PostSourceSites():
    def __init__(self):
	    self.PickabuUrl = 'pikabu.ru'

MysqlConnectorConfig = {
  'user': 'root',
  'password': '',
  'host': '127.0.0.1',
  'database': 'postila',
  'raise_on_warnings': True,
}

dictModifierMappingToTables = {'' : 'posts',
                               'ByComplexRaitingFromPostsNotText' : 'posts',
                               'ByComplexRaitingFromPosts' : 'posts',
                               'BySimpleRaitingFromArchive' : 'archiveposts'}

objXmlConfigFile    = XmlConfigFile()
objParserParameters = ParserParameters()
objLogPaths         = LogPaths()
objPostSourceSites  = PostSourceSites()
tplFilterWorld = ('pikabu', 'пикабу')
setFilterTitleWorld   = set(['PIKABU', 'ПИКАБУ', 'КОММЕНТАХ'])
setFilterContentWorld = set(['PIKABU', 'ПИКАБУ', 'КОММЕНТАХ', 'ПОМОГИТЕ', 'МИНУСОВ'])
setFilterTags  = set(['anime', 'аниме', 'видео'])
tplSkipTagsVkWallPost = ('моё', 'мое', 'не_моё', 'не_мое')



