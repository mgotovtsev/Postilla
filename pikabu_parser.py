import os
import traceback
import xml.sax.saxutils # unescape
import xml.etree.ElementTree as ElementTree

from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import errorcode

from model_parameters import *
from helper_procedures import *

def Parse(sHtml):

    listPosts = []

    if not sHtml:
        sMessage = 'Problem with getting html for parse!'
        PrintWarningToLog(objLogPaths.sParserPikabuLogPath, sMessage)
        return listPosts

    try:
        objSoup          = BeautifulSoup(sHtml, "html.parser")
        StoriesContainer = objSoup.find('div', class_='stories')

        if not StoriesContainer:
           sMessage = 'Can not found any posts!'
           PrintWarningToLog(objLogPaths.sParserPikabuLogPath, sMessage)
           return listPosts

        listStories      = StoriesContainer.find_all('div', class_='story')

        for Story in listStories:

            # Filter post by inner text

            listPostTextBlocks = Story.find_all('div', class_='b-story-block b-story-block_type_text')

            isSkippedPost = False

            for TextBlock in listPostTextBlocks:
                for sWordForFilter in tplFilterWorld:
                    if sWordForFilter in TextBlock.text.lower():
                        isSkippedPost = True
                        break

                if isSkippedPost == True:
                    break

            if isSkippedPost == True:
               continue

            objPost = Post()

            objPost.PstSiteName = objPostSourceSites.PickabuUrl

            # Get post link, id, title
            PostTitleLink  = Story.find('a', class_='story__title-link ')

            if not PostTitleLink:
               continue

            objPost.PstSiteId = int(Story['data-story-id'])
            objPost.PstHref  = PostTitleLink['href']
            objPost.PstTitle = PostTitleLink.text

            # Get vote count
            PostVotesCnt = Story.find('div', class_='story__rating-count')
            if PostVotesCnt:
                PostVotesBlock = PostVotesCnt.find('i')
                if not PostVotesBlock:
                   objPost.PstVotes = TryInt(PostVotesCnt)

            # Get date
            PostTimeStamp = Story.find('div', class_='story__date')
            if PostTimeStamp:
               nPostTimeStamp = TryInt(PostTimeStamp['title'])
               objPost.PstDateTime = datetime.fromtimestamp(nPostTimeStamp)

            # Get tags
            listPostTags = Story.find_all('a', class_='story__tag')

            if listPostTags and len(listPostTags) > 0:
               objPost.listTags = [PostTag.text.strip() for PostTag in listPostTags]

            # Filter post by tags
            if len(setFilterTags.intersection(set(objPost.listTags))) > 0:
               continue

            # Get comments
            CommentsLink = Story.find('a', class_='story__comments-count story__to-comments')
            if CommentsLink:
               CommentsText = CommentsLink.text
               objPost.PstCmntCnt = TryInt(CommentsText.split(' ')[0])

            # Get author nick and href
            AuthorLink = Story.find('a', class_='story__author')
            if AuthorLink:
               objPost.PstAuthorNick = AuthorLink.text
               objPost.PstAuthorHref = AuthorLink['href']

            # Calculate rank parameters
            objPost.GetVotesPerSec()
            objPost.GetCommentPerSec()

            listPosts.append(objPost)

        ##sMessage = '%s posts parsed' % len(listPosts)
        ##PrintMessageToLog(objLogPaths.sParserPikabuLogPath, sMessage)
    except:
        sMessage = traceback.format_exc()
        PrintErrorToLog(objLogPaths.sParserPikabuLogPath, sMessage)
        raise SystemExit # Exception

    return listPosts

def AddNewPostsIntoDb(listPosts, sLogFilePath):

    if len(listPosts) == 0:
       sMessage = "No posts for insert into DB"
       PrintWarningToLog(objLogPaths.sParserPikabuLogPath, sMessage)
       return

    try:

        MySqlConnection = mysql.connector.connect(**MysqlConnectorConfig)
        listPostValues  = []
        nInsertedPstCnt = 0

        # Processing each post
        for objPost in listPosts:

            # Check post in database (in "posts" table)
            # buffered=True for resolve problem with "Unread result found"
            # http://stackoverflow.com/questions/29772337/python-mysql-connector-unread-result-found-when-using-fetchone
            PostCursor = MySqlConnection.cursor(buffered = True)

            ##TBD: Separate procedure
            sGetPostByIdQuery = ("SELECT count(*) FROM posts "
                                 "WHERE PstSiteId = %(PstSiteId)s and PstSiteName = %(PstSiteName)s")

            dictPostData ={'PstSiteId'   : objPost.PstSiteId,
                           'PstSiteName' : objPost.PstSiteName}

            PostCursor.execute(sGetPostByIdQuery, dictPostData)

            tplDbPostCount = PostCursor.fetchone() # Get result row count
            nDbPostCount   = tplDbPostCount[0]

            # Skip exists post in db
            if nDbPostCount != 0:
               continue

            sMessage = '\tAdded post: %s' % objPost.PstHref
            PrintMessageToLog(objLogPaths.sParserPikabuLogPath, sMessage)

            nInsertedPstCnt += 1

            ##TBD: Separate procedure
            # Get Id for all tags. If tag not exist in "tags" table then add this tag.
            listPostTagIds = listPostTagIds =[0 for i in range(10)] # Empty list with result tag Ids

            for TagIndex, sTagName in enumerate(objPost.listTags):

                # Get tag id from "tags" table
                sGetTagQuery = ("SELECT Id FROM tags "
                                "WHERE TagName = %(TagName)s")

                dictTagData = {'TagName' : sTagName}

                PostCursor.execute(sGetTagQuery, dictTagData)

                tplTagId = PostCursor.fetchone() # Fetch TagId

                if not tplTagId:
                   # Insert tag if that not exists and get inserted TagId

                    sAddTagQuery = ("INSERT INTO tags "
                                    "(TagName)"
                                    "VALUES (%(TagName)s)")

                    PostCursor.execute(sAddTagQuery, dictTagData) # Insert new tag

                    nTagId = PostCursor.lastrowid
                else:
                    nTagId   = tplTagId[0]

                # Fill result list with tag ids
                listPostTagIds[TagIndex] = nTagId

            # Prepare insert batch
            dictPostData = {'PstSiteId'        : objPost.PstSiteId,
                            'PstSiteName'      : objPost.PstSiteName,
                            'PstHref'          : objPost.PstHref,
                            'PstTitle'         : objPost.PstTitle,
                            'PstVotes'         : objPost.PstVotes,
                            'PstCmntCnt'       : objPost.PstCmntCnt,
                            'PstPosCmntCnt'    : objPost.PstPosCmntCnt,
                            'PstNegCmntCnt'    : objPost.PstNegCmntCnt,
                            'PstDateTime'      : objPost.PstDateTime,
                            'PstAuthorNick'    : objPost.PstAuthorNick,
                            'PstAuthorHref'    : objPost.PstAuthorHref,
                            'TagId1'           : listPostTagIds[0],
                            'TagId2'           : listPostTagIds[1],
                            'TagId3'           : listPostTagIds[2],
                            'TagId4'           : listPostTagIds[3],
                            'TagId5'           : listPostTagIds[4],
                            'TagId6'           : listPostTagIds[5],
                            'TagId7'           : listPostTagIds[6],
                            'TagId8'           : listPostTagIds[7],
                            'TagId9'           : listPostTagIds[8],
                            'TagId10'          : listPostTagIds[9],
                            'PstVotesPerSec'   : objPost.PstVotesPerSec,
                            'PstCommentPerSec' : objPost.PstCommentPerSec}

            listPostValues.append(dictPostData)

        ##TBD: Separate procedure
        # Insert post into "posts" table

        sAddPostQuery = ("INSERT INTO posts "
                         "(PstSiteId, PstSiteName, PstHref, PstTitle, PstVotes, PstCmntCnt, PstPosCmntCnt,  \
                          PstNegCmntCnt, PstDateTime, PstAuthorNick, PstAuthorHref, TagId1, TagId2, TagId3, \
                          TagId4, TagId5, TagId6, TagId7, TagId8, TagId9, TagId10, PstVotesPerSec, PstCommentPerSec)"
                         "VALUES (%(PstSiteId)s  , %(PstSiteName)s   , %(PstHref)s          , %(PstTitle)s     , \
                                  %(PstVotes)s   , %(PstCmntCnt)s    , %(PstPosCmntCnt)s    , %(PstNegCmntCnt)s, \
                                  %(PstDateTime)s, %(PstAuthorNick)s , %(PstAuthorHref)s    , %(TagId1)s       , \
                                  %(TagId2)s     , %(TagId3)s        , %(TagId4)s           , %(TagId5)s       , \
                                  %(TagId6)s     , %(TagId7)s        , %(TagId8)s           , %(TagId9)s       , \
                                  %(TagId10)s    , %(PstVotesPerSec)s, %(PstCommentPerSec)s)")

        PostCursor.executemany(sAddPostQuery, listPostValues)

        MySqlConnection.commit()
        PostCursor.close()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
          sMessage = "Something is wrong with your user name or password"
          PrintErrorToLog(objLogPaths.sParserPikabuLogPath, sMessage)
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
          sMessage = "Database does not exist"
          PrintErrorToLog(objLogPaths.sParserPikabuLogPath, sMessage)
        else:
          PrintErrorToLog(objLogPaths.sParserPikabuLogPath, err)
    else:
        MySqlConnection.close()
        sMessage = '%s posts inserted into DB' % nInsertedPstCnt
        PrintMessageToLog(sLogFilePath, sMessage)

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

    except:
        sMessage = traceback.format_exc()
        print(sMessage)
        return None

    return dictParams


def main():

    print("Pikabu_Parser")

    # Read parser params
    ParserParams = ReadXMLParemeters(objXmlConfigFile.sPikabuXMLFilePath)
    isWork       = ParserParams[objParserParameters.isWork]

    while(isWork):

        # Continue if pause exists
        if not ParserParams[objParserParameters.isPausedProcess]:

           # Parse posts
           listPosts = Parse(GetHtml(sUrl))

           # Add each post into MySql DB
           AddNewPostsIntoDb(listPosts, objLogPaths.sParserPikabuLogPath)

        # Sleep
        time.sleep(ParserParams[objParserParameters.ParseDelaySec])

        # Read Xml attributes again
        ParserParams = ReadXMLParemeters(objXmlConfigFile.sPikabuXMLFilePath)

        # Check on stop process
        isWork = ParserParams[objParserParameters.isWork]

if __name__ == '__main__':
    main()

