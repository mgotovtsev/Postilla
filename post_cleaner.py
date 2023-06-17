import os
import traceback
import xml.sax.saxutils # unescape
import xml.etree.ElementTree as ElementTree

import mysql.connector
from mysql.connector import errorcode

from model_parameters import *
from helper_procedures import *

def RemoveNegativeRatingPosts(nLowerBorderRatingPost, nLowerBrdrUpdateCntPost):
    try:
        # Connect to DB
        MySqlConnection = mysql.connector.connect(**MysqlConnectorConfig)
        PostCursor = MySqlConnection.cursor()

        # Run query
        sDeleteQuery = ("delete from posts where PstVotes < %(nLowerBorderRatingPost)s and PstUpdCnt >= %(nLowerBrdrUpdateCntPost)s")

        PostCursor.execute(sDeleteQuery, {'nLowerBorderRatingPost'  : nLowerBorderRatingPost,
                                          'nLowerBrdrUpdateCntPost' : nLowerBrdrUpdateCntPost})

        sMessage = 'RemoveNegativeRatingPosts. Count rows were deleted %s' % PostCursor.rowcount
        PrintMessageToLog(objLogPaths.sPostCleanerLogPath, sMessage)

        MySqlConnection.commit()
        PostCursor.close()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
          sMessage = "Something is wrong with your user name or password"
          PrintErrorToLog(objLogPaths.sPostCleanerLogPath, sMessage)
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
          sMessage = "Database does not exist"
          PrintErrorToLog(objLogPaths.sPostCleanerLogPath, sMessage)
        else:
          PrintErrorToLog(objLogPaths.sPostCleanerLogPath, err)
    else:
        MySqlConnection.close()

def ArchiveOldPosts(nBackupBorderRatingPost, nBackupDayPost):
    try:
        # Connect to DB
        MySqlConnection = mysql.connector.connect(**MysqlConnectorConfig)

        # Insert rows from "posts" table into "archiveposts" table
        PostCursor = MySqlConnection.cursor()

        sQuery = ("insert into archiveposts"
                  "    select * from posts"
                  "    where PstVotes >= %(nBackupBorderRatingPost)s and DATEDIFF(NOW(), PstDateTime) >= %(nBackupDayPost)s")

        PostCursor.execute(sQuery, {'nBackupBorderRatingPost' : nBackupBorderRatingPost,
                                    'nBackupDayPost'          : nBackupDayPost})
        PostCursor.close()

        # Remove rows from "posts" table
        PostCursor = MySqlConnection.cursor()

        sQuery = ("delete from posts where PstVotes >= %(nBackupBorderRatingPost)s and DATEDIFF(NOW(), PstDateTime) >= %(nBackupDayPost)s")

        PostCursor.execute(sQuery, {'nBackupBorderRatingPost' : nBackupBorderRatingPost,
                                    'nBackupDayPost'          : nBackupDayPost})

        sMessage = 'ArchiveOldPosts. Count rows were archived %s' % PostCursor.rowcount
        PrintMessageToLog(objLogPaths.sPostCleanerLogPath, sMessage)

        PostCursor.close()

        MySqlConnection.commit()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
          sMessage = "Something is wrong with your user name or password"
          PrintErrorToLog(objLogPaths.sPostCleanerLogPath, sMessage)
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
          sMessage = "Database does not exist"
          PrintErrorToLog(objLogPaths.sPostCleanerLogPath, sMessage)
        else:
          PrintErrorToLog(objLogPaths.sPostCleanerLogPath, err)
    else:
        MySqlConnection.close()

def ShrinkPosts(nMaxPostsCntBeforeShrink, nShrinkFactor, nShrinkRatingDivisor):
    try:

        # Get row count in "posts" table
        MySqlConnection = mysql.connector.connect(**MysqlConnectorConfig)
        PostCursor = MySqlConnection.cursor()

        sGetPostCountQuery = ("SELECT count(*) FROM posts ")

        PostCursor.execute(sGetPostCountQuery)

        tplPostCount = PostCursor.fetchone() # Get result row count
        nPostCount   = tplPostCount[0]

        sMessage = 'ShrinkPosts. Posts in DB now %s' % nPostCount
        PrintMessageToLog(objLogPaths.sPostCleanerLogPath, sMessage)

        PostCursor.close()

        # Check row count for shrink "post" table
        if nPostCount < nMaxPostsCntBeforeShrink:
           return

        # Shrink procedure
        nRowCntForDelete = (nPostCount - nMaxPostsCntBeforeShrink) + (nMaxPostsCntBeforeShrink - nShrinkFactor * nMaxPostsCntBeforeShrink)

        PostCursor = MySqlConnection.cursor()
        sRowCntForDelete = "%d " % nRowCntForDelete

        sQuery = ("delete from posts "
                  "order by round(PstVotes / " + str(nShrinkRatingDivisor) + ", 0), PstVotes, datediff(now(), PstDateTime) desc "
                  "limit " + sRowCntForDelete)

        PostCursor.execute(sQuery)

        sMessage = 'ShrinkPosts. Count rows were deleted %s' % PostCursor.rowcount
        PrintMessageToLog(objLogPaths.sPostCleanerLogPath, sMessage)

        MySqlConnection.commit()
        PostCursor.close()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
          sMessage = "Something is wrong with your user name or password"
          PrintErrorToLog(objLogPaths.sPostCleanerLogPath, sMessage)
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
          sMessage = "Database does not exist"
          PrintErrorToLog(objLogPaths.sPostCleanerLogPath, sMessage)
        else:
          PrintErrorToLog(objLogPaths.sPostCleanerLogPath, err)
    else:
        MySqlConnection.close()

def ReadXMLParemeters(sXMLFilePath):
    """
       Function for read parameters from XML file
    """
    try:
        # <check input parameters>

        if not os.path.exists(sXMLFilePath):
            sMessage = 'Xml file not exists!'
            PrintWarningToLog(sLogFilePath, sMessage)
            return None

        # </check input parameters>

        dictParams = {} # References section

        RootTree = ElementTree.parse(sXMLFilePath) # Exception
        RootNode = RootTree.getroot() # Exception

        for Param in RootNode.iter('param'):
            if Param.get(objParserParameters.ParseDelaySec):
               dictParams[objParserParameters.ParseDelaySec] = int(Param.get(objParserParameters.ParseDelaySec))

            if Param.get(objParserParameters.isPausedProcess):
               dictParams[objParserParameters.isPausedProcess] = True if Param.get(objParserParameters.isPausedProcess).upper() == 'TRUE' else False

            if Param.get(objParserParameters.isWork):
               dictParams[objParserParameters.isWork] = True if Param.get(objParserParameters.isWork).upper() == 'TRUE' else False

            if Param.get(objParserParameters.LowerBorderRatingPost):
               dictParams[objParserParameters.LowerBorderRatingPost] = int(Param.get(objParserParameters.LowerBorderRatingPost))

            if Param.get(objParserParameters.LowerBrdrUpdateCntPost):
               dictParams[objParserParameters.LowerBrdrUpdateCntPost] = int(Param.get(objParserParameters.LowerBrdrUpdateCntPost))

            if Param.get(objParserParameters.BackupBorderRatingPost):
               dictParams[objParserParameters.BackupBorderRatingPost] = int(Param.get(objParserParameters.BackupBorderRatingPost))

            if Param.get(objParserParameters.BackupDayPost):
               dictParams[objParserParameters.BackupDayPost] = int(Param.get(objParserParameters.BackupDayPost))

            if Param.get(objParserParameters.MaxPostsCntBeforeShrink):
               dictParams[objParserParameters.MaxPostsCntBeforeShrink] = int(Param.get(objParserParameters.MaxPostsCntBeforeShrink))

            if Param.get(objParserParameters.ShrinkFactor):
               dictParams[objParserParameters.ShrinkFactor] = float(Param.get(objParserParameters.ShrinkFactor))

            if Param.get(objParserParameters.ShrinkRatingDivisor):
               dictParams[objParserParameters.ShrinkRatingDivisor] = float(Param.get(objParserParameters.ShrinkRatingDivisor))

    except:
        sMessage = traceback.format_exc()
        print(sMessage)
        return None

    return dictParams

def main():

    print("Post_Cleaner")

    # Read params
    ParserParams = ReadXMLParemeters(objXmlConfigFile.sPostCleanerXMLFilePath)
    isWork       = ParserParams[objParserParameters.isWork]

    while(isWork):

        # Continue if pause exists
        if not ParserParams[objParserParameters.isPausedProcess]:

            # Remove negative rating pasts
            nLowerBorderRatingPost  = ParserParams.get(objParserParameters.LowerBorderRatingPost)
            nLowerBrdrUpdateCntPost = ParserParams.get(objParserParameters.LowerBrdrUpdateCntPost)
            RemoveNegativeRatingPosts(nLowerBorderRatingPost, nLowerBrdrUpdateCntPost)

            # Archive old posts
            nBackupBorderRatingPost = ParserParams.get(objParserParameters.BackupBorderRatingPost)
            nBackupDayPost          = ParserParams.get(objParserParameters.BackupDayPost)
            ArchiveOldPosts(nBackupBorderRatingPost, nBackupDayPost)

            # Shrink posts DB table
            nMaxPostsCntBeforeShrink = ParserParams.get(objParserParameters.MaxPostsCntBeforeShrink)
            nShrinkFactor            = ParserParams.get(objParserParameters.ShrinkFactor)
            nShrinkRatingDivisor     = ParserParams.get(objParserParameters.ShrinkRatingDivisor)
            ShrinkPosts(nMaxPostsCntBeforeShrink, nShrinkFactor, nShrinkRatingDivisor)

        # Sleep
        time.sleep(ParserParams[objParserParameters.ParseDelaySec])

        # Read Xml attributes again
        ParserParams = ReadXMLParemeters(objXmlConfigFile.sPostCleanerXMLFilePath)

        # Check on stop process
        isWork = ParserParams[objParserParameters.isWork]

if __name__ == '__main__':
    main()