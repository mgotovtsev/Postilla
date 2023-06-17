import os
import traceback
import xml.sax.saxutils # unescape
import xml.etree.ElementTree as ElementTree

from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import errorcode

from model_parameters import *
from helper_procedures import *

from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

# Get list with all posts sorted by PostId (asc)
def GetListAllPosts():
    listAllPosts = []
    try:

        # Connect to DB
        MySqlConnection = mysql.connector.connect(**MysqlConnectorConfig)
        PostCursor = MySqlConnection.cursor()

        # Run query
        sGetPostsQuery = ("SELECT id, PstSiteName, PstHref, PstDateTime FROM posts order by id")

        PostCursor.execute(sGetPostsQuery)

        # Fetch result into list

        for (nId, sPstSiteName, sPstHref, PstDateTime) in PostCursor:
            listAllPosts.append([nId, sPstSiteName, sPstHref, PstDateTime])

        PostCursor.close()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
          sMessage = "Something is wrong with your user name or password"
          PrintErrorToLog(objLogPaths.sInfoCollectorLogPath, sMessage)
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
          sMessage = "Database does not exist"
          PrintErrorToLog(objLogPaths.sInfoCollectorLogPath, sMessage)
        else:
          PrintErrorToLog(objLogPaths.sInfoCollectorLogPath, err)
    else:
        MySqlConnection.close()

        #sMessage = '%s posts selected from DB' % len(listAllPosts)
        #PrintMessageToLog(objLogPaths.sInfoCollectorLogPath, sMessage)

    return listAllPosts

def RunTasks(listParams):
    try:
        if listParams:

            InfoCollectorFunction = listParams[0]
            nId                   = listParams[1]
            sPstHref              = listParams[2]
            PstDateTime           = listParams[3]

            time.sleep(3)
            print ('Collect info %s' % sPstHref)
            InfoCollectorFunction(nId, sPstHref, PstDateTime)
    except:
        sMessage = traceback.format_exc()
        PrintErrorToLog(objLogPaths.sInfoCollectorLogPath, sMessage)

# Get info for each post
def InfoCollectorDistributor(listAllPosts, nThreadCnt):
    dictInfoDisributor = {objPostSourceSites.PickabuUrl : PikabuPostInfoCollector}

    listTask = []
    for (nId, sPstSiteName, sPstHref, PstDateTime) in listAllPosts:
        InfoCollectorFunction = dictInfoDisributor.get(sPstSiteName, None)
        if InfoCollectorFunction:
           listTask.append([InfoCollectorFunction, nId, sPstHref, PstDateTime])

    # Sets the pool size
    pool = ThreadPool(nThreadCnt)

    # Update post info
    pool.map(RunTasks, listTask)

    #close the pool and wait for the work to finish
    pool.close()
    pool.join()

# Pikabu post info collector
def PikabuPostInfoCollector(nId, sPostHref, PstDateTime):
    try:

        sHtmlText = GetHtml(sPostHref)

        try:
            objSoup = BeautifulSoup(sHtmlText, "html.parser")
        except:
            objSoup = None

        if not objSoup:
            return

        StoryContainer = objSoup.find('div', class_='story')
        SocialButtons  = objSoup.find('div', class_='b-social__buttons')
        StoryRating    = objSoup.find('div', class_='b-story-info')
        StoryComments  = objSoup.find('div', class_='b-comments_type_main')

        objPost = Post()
        objPost.PstDateTime = PstDateTime

        # Get comments
        if StoryContainer:
            CommentsLink = StoryContainer.find('a', class_='story__comments-count story__to-comments')
            if CommentsLink:
                CommentsText = CommentsLink.text.strip()
                objPost.PstCmntCnt = TryInt(CommentsText.split(' ')[0])
                objPost.GetCommentPerSec()

        # Get social shared counts
        if SocialButtons:
            FbSocButton = SocialButtons.find('div', class_='b-social-button b-social-button_type_facebook')
            if FbSocButton:
                FbSocSpan = FbSocButton.find('span', class_='b-social-button__counter')
                if FbSocButton:
                    objPost.PstFbLkCnt = TryInt(FbSocSpan.text.strip())

            VkSocButton = SocialButtons.find('div', class_='b-social-button b-social-button_type_vk')
            if VkSocButton:
                VkSocSpan = FbSocButton.find('span', class_='b-social-button__counter')
                if VkSocSpan:
                    objPost.PstVkLkCnt = TryInt(VkSocSpan.text.strip())

            TwSocButton = SocialButtons.find('div', class_='b-social-button b-social-button_type_twitter')
            if TwSocButton:
                TwSocSpan = FbSocButton.find('span', class_='b-social-button__counter')
                if TwSocSpan:
                    objPost.PstTwLkCnt = TryInt(TwSocSpan.text.strip())

            SvButton = SocialButtons.find('div', class_='b-social-button b-social-button_type_save')
            if SvButton:
                objPost.PstSvCnt = TryInt(SvButton['data-count'])

        # Get story counts
        if StoryRating:
            StoryRating = StoryRating.find('div', class_='b-story__rating')
            if StoryRating:
                    objPost.PstPosVotesCnt = TryInt(StoryRating['data-pluses'])
                    objPost.PstNegVotesCnt = TryInt(StoryRating['data-minuses'])
                    objPost.PstVotes = objPost.PstPosVotesCnt - objPost.PstNegVotesCnt
                    objPost.GetVotesPerSec()

        # Get positive, negative, neutral comment count
        if StoryComments:
            PstPosCmntRtng = 0
            PstNegCmntRtng = 0
            PstPosCmntCnt  = 0
            PstNegCmntCnt  = 0
            PstNeuCmntCnt  = 0

            listCommentRaiting = StoryComments.find_all('div', class_='b-comment__rating-count')

            for RaitingElem in listCommentRaiting:
                nCommentRaiting = TryInt(RaitingElem.text.strip())
                if nCommentRaiting == 0:
                    PstNeuCmntCnt += 1
                elif nCommentRaiting > 0:
                    PstPosCmntCnt += 1
                    PstPosCmntRtng += nCommentRaiting
                elif nCommentRaiting < 0:
                    PstNegCmntCnt += 1
                    PstNegCmntRtng += nCommentRaiting

            objPost.PstPosCmntRtng = PstPosCmntRtng
            objPost.PstNegCmntRtng = PstNegCmntRtng
            objPost.PstPosCmntCnt  = PstPosCmntCnt
            objPost.PstNegCmntCnt  = PstNegCmntCnt
            objPost.PstNeuCmntCnt  = PstNeuCmntCnt

        '''
        print(objPost.PstSiteId, 'PstSiteId')
        print(objPost.PstVotes, 'PstVotes')
        print(objPost.PstCmntCnt, 'PstCmntCnt')
        print(objPost.PstDateTime, 'PstDateTime')
        print(objPost.PstPosVotesCnt, 'PstPosVotesCnt')
        print(objPost.PstNegVotesCnt, 'PstNegVotesCnt')
        print(objPost.PstPosCmntRtng, 'PstPosCmntRtng')
        print(objPost.PstNegCmntRtng, 'PstNegCmntRtng')
        print(objPost.PstPosCmntCnt, 'PstPosCmntCnt')
        print(objPost.PstNegCmntCnt, 'PstNegCmntCnt')
        print(objPost.PstNeuCmntCnt, 'PstNeuCmntCnt')
        print(objPost.PstFbLkCnt, 'PstFbLkCnt')
        print(objPost.PstVkLkCnt, 'PstVkLkCnt')
        print(objPost.PstTwLkCnt, 'PstTwLkCnt')
        print(objPost.PstSvCnt, 'PstSvCnt')
        print(objPost.PstVotesPerSec, 'PstVotesPerSec')
        print(objPost.PstCommentPerSec, 'PstCommentPerSec')
        '''

        UpdateInfoPostInDb(nId, objPost)

    except:
        sMessage = traceback.format_exc()
        print(sMessage)
        print(nId, sPostHref)

# Update post info into DB
def UpdateInfoPostInDb(nId, objPost):
    try:
        MySqlConnection = mysql.connector.connect(**MysqlConnectorConfig)

        # Check post in database (in "posts" table)
        # buffered=True for resolve problem with "Unread result found"
        # http://stackoverflow.com/questions/29772337/python-mysql-connector-unread-result-found-when-using-fetchone
        PostCursor = MySqlConnection.cursor(buffered = True)

        ##TBD: Separate procedure
        sGetPostByIdQuery = ("SELECT count(*) FROM posts "
                             "WHERE Id = %(nId)s")

        dictPostData ={'nId' : nId}

        PostCursor.execute(sGetPostByIdQuery, dictPostData)

        tplDbPostCount = PostCursor.fetchone() # Get result row count
        nDbPostCount   = tplDbPostCount[0]

        # Skip exists post in db
        if nDbPostCount == 0:
           return

        sUpdPostQuery = ("UPDATE posts "
                         "SET PstVotes         = %(PstVotes)s,        "
                         "    PstCmntCnt       = %(PstCmntCnt)s,      "
                         "    PstPosVotesCnt   = %(PstPosVotesCnt)s,  "
                         "    PstNegVotesCnt   = %(PstNegVotesCnt)s,  "
                         "    PstPosCmntRtng   = %(PstPosCmntRtng)s,  "
                         "    PstNegCmntRtng   = %(PstNegCmntRtng)s,  "
                         "    PstPosCmntCnt    = %(PstPosCmntCnt)s,   "
                         "    PstNegCmntCnt    = %(PstNegCmntCnt)s,   "
                         "    PstNeuCmntCnt    = %(PstNeuCmntCnt)s,   "
                         "    PstVkLkCnt       = %(PstVkLkCnt)s,      "
                         "    PstFbLkCnt       = %(PstFbLkCnt)s,      "
                         "    PstTwLkCnt       = %(PstTwLkCnt)s,      "
                         "    PstSvCnt         = %(PstSvCnt)s,        "
                         "    PstVotesPerSec   = %(PstVotesPerSec)s,  "
                         "    PstCommentPerSec = %(PstCommentPerSec)s,"
                         "    PstUpdCnt        = PstUpdCnt + 1        "
                         "WHERE Id = %(nId)s")

        # Prepare update params
        dictPostData = {'PstVotes'         : objPost.PstVotes,
                        'PstCmntCnt'       : objPost.PstCmntCnt,
                        'PstPosVotesCnt'   : objPost.PstPosVotesCnt,
                        'PstNegVotesCnt'   : objPost.PstNegVotesCnt,
                        'PstPosCmntRtng'   : objPost.PstPosCmntRtng,
                        'PstNegCmntRtng'   : objPost.PstNegCmntRtng,
                        'PstPosCmntCnt'    : objPost.PstPosCmntCnt,
                        'PstNegCmntCnt'    : objPost.PstNegCmntCnt,
                        'PstNeuCmntCnt'    : objPost.PstNeuCmntCnt,
                        'PstVkLkCnt'       : objPost.PstVkLkCnt,
                        'PstFbLkCnt'       : objPost.PstFbLkCnt,
                        'PstTwLkCnt'       : objPost.PstTwLkCnt,
                        'PstSvCnt'         : objPost.PstSvCnt,
                        'PstVotesPerSec'   : objPost.PstVotesPerSec,
                        'PstCommentPerSec' :objPost.PstCommentPerSec,
                        'nId' : nId}

        PostCursor.execute(sUpdPostQuery, dictPostData)

        MySqlConnection.commit()
        PostCursor.close()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
          sMessage = "Something is wrong with your user name or password"
          PrintErrorToLog(objLogPaths.sInfoCollectorLogPath, sMessage)
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
          sMessage = "Database does not exist"
          PrintErrorToLog(objLogPaths.sInfoCollectorLogPath, sMessage)
        else:
          PrintErrorToLog(objLogPaths.sInfoCollectorLogPath, err)
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

            if Param.get(objParserParameters.InfoCollectorThreadCnt):
               dictParams[objParserParameters.InfoCollectorThreadCnt] = int(Param.get(objParserParameters.InfoCollectorThreadCnt))

    except:
        sMessage = traceback.format_exc()
        print(sMessage)
        return None

    return dictParams

def main():

    print("Info_Collector")

    # Read params
    ParserParams = ReadXMLParemeters(objXmlConfigFile.sInfoCollectorXMLFilePath)
    isWork       = ParserParams[objParserParameters.isWork]

    while(isWork):

        # Continue if pause exists
        if not ParserParams[objParserParameters.isPausedProcess]:

           # Get all posts from DB
           listAllPosts = GetListAllPosts()

           # Update each post in MySql DB
           InfoCollectorDistributor(listAllPosts, ParserParams[objParserParameters.InfoCollectorThreadCnt])

        # Sleep
        time.sleep(ParserParams[objParserParameters.ParseDelaySec])

        # Read Xml attributes again
        ParserParams = ReadXMLParemeters(objXmlConfigFile.sInfoCollectorXMLFilePath)

        # Check on stop process
        isWork = ParserParams[objParserParameters.isWork]

if __name__ == '__main__':
    main()