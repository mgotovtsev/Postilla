import os
import re
import csv
import sys
import urllib
import requests
import traceback
from datetime import datetime

from bs4 import BeautifulSoup

import xml.sax.saxutils # unescape
import xml.etree.ElementTree as ElementTree

import mysql.connector
from mysql.connector import errorcode

import vk

from model_parameters import *
from helper_procedures import *

nMaxLoadingImageCount            = 5
nMaxAttachmentsInNotWikiPagePost = 10
nWallTextCharsCnt                = 256
nMaxPostChars                    = 1000
album_id               = '237524537'
group_id               = '119200682'
user_id                = '16152952'
gif_container_group_id = '123574255'
GroupName              = 'postilla'
app_id                 = 'your_vk_app_id',
user_login             = 'your_vk_login',	
user_password          = 'your_vk_password'	

def GetShaduleTable():
    try:

        sFldPostNumber   = 'PostNumber'
        sFldPostTime     = 'PostTime'
        sFldPostModifier = 'PostModifier'
        listTaskShadule  = []
        with open(objXmlConfigFile.sPostPosterCsvFilePath, 'r') as fileCsv: # Exception

            readerCensusCsv = csv.DictReader(fileCsv, delimiter=';', quotechar='"') # Exception

            # Fill result dictinary
            for dictRowCensusCsv in readerCensusCsv:
                if dictRowCensusCsv[sFldPostNumber] != '':
                    nPostNumber   = int(dictRowCensusCsv[sFldPostNumber])
                    sPostTime     = dictRowCensusCsv[sFldPostTime]
                    sPostModifier = dictRowCensusCsv[sFldPostModifier]

                    PostTime = datetime.strptime(sPostTime, '%H:%M')

                    listTaskShadule.append([nPostNumber, PostTime, sPostModifier])

    except:
        sMessage = traceback.format_exc()
        PrintErrorToLog(objLogPaths.sPostPosterLogPath, sMessage)


    return listTaskShadule

def GetNextPostSecondsDelayAndModifier():
    try:
        # Read shedule table
        listTaskShadule = GetShaduleTable()

        # Define delay for next post time and get modifier
        for Task in listTaskShadule:

            TaskDateTime      = Task[1]
            NowDateTime       = datetime.now()
            sNowDateTime      = datetime.strftime(NowDateTime, '%H:%M')
            NowDateTimeHrsMin = datetime.strptime(sNowDateTime, '%H:%M')
            TimeDiff          = TaskDateTime - NowDateTimeHrsMin
            nTimeDiffSec      = TimeDiff.total_seconds()

            if nTimeDiffSec >= 0:
                sModifier = Task[2]
                return nTimeDiffSec, sModifier
        else:
            return 3600, ''

    except:
        sMessage = traceback.format_exc()
        PrintErrorToLog(objLogPaths.sPostPosterLogPath, sMessage)

def TagReplacer(sTagValue, sReplaceChar = '_'):
    sReplaceSymbols = ' !"#$%&\'()*+,-./:;<=>?@[\]^_`{|}~'
    sTagValueRes = ''

    for sChr in sTagValue:
        if sChr in sReplaceSymbols:
            sChr = sReplaceChar
        sTagValueRes += sChr

    return sTagValueRes

def GetPostFromDb(sModifier = ''):
    try:

        sModifier = sModifier.strip()

        objPost = Post()

        MySqlConnection = mysql.connector.connect(**MysqlConnectorConfig)
        PostCursor      = MySqlConnection.cursor(buffered = True)

        # Prepare post query
        if sModifier == 'BySimpleRaitingFromArchive':
            # Get post with max (PstVotes) from "archiveposts" table
            sQuery = ("SELECT PstSiteName, PstHref, PstTitle, "
                      "(select TagName from tags where Id = TagId1) Tag1,  "
                      "(select TagName from tags where Id = TagId2) Tag2,  "
                      "(select TagName from tags where Id = TagId3) Tag3,  "
                      "(select TagName from tags where Id = TagId4) Tag4,  "
                      "(select TagName from tags where Id = TagId5) Tag5,  "
                      "(select TagName from tags where Id = TagId6) Tag6,  "
                      "(select TagName from tags where Id = TagId7) Tag7,  "
                      "(select TagName from tags where Id = TagId8) Tag8,  "
                      "(select TagName from tags where Id = TagId9) Tag9,  "
                      "(select TagName from tags where Id = TagId10) Tag10, "
                      "Id "
                      "FROM archiveposts "
                      "ORDER BY (IFNULL(PstVotes, 0)) desc "
                      "LIMIT 1 ")

        elif sModifier in ['ByComplexRaitingFromPosts', '']:
            # Get post with max (PstVotes + PstPosCmntRtng + PstNegCmntRtng) from "post" table

            sQuery = ("SELECT PstSiteName, PstHref, PstTitle, "
                      "(select TagName from tags where Id = TagId1) Tag1,  "
                      "(select TagName from tags where Id = TagId2) Tag2,  "
                      "(select TagName from tags where Id = TagId3) Tag3,  "
                      "(select TagName from tags where Id = TagId4) Tag4,  "
                      "(select TagName from tags where Id = TagId5) Tag5,  "
                      "(select TagName from tags where Id = TagId6) Tag6,  "
                      "(select TagName from tags where Id = TagId7) Tag7,  "
                      "(select TagName from tags where Id = TagId8) Tag8,  "
                      "(select TagName from tags where Id = TagId9) Tag9,  "
                      "(select TagName from tags where Id = TagId10) Tag10, "
                      "Id "
                      "FROM posts "
                      "ORDER BY (IFNULL(PstVotes, 0) + IFNULL(PstPosCmntRtng, 0) + IFNULL(PstNegCmntRtng, 0)) desc "
                      "LIMIT 1 ")
        elif sModifier in ['ByComplexRaitingFromPostsNotText', '']:
            # Get post with max (PstVotes + PstPosCmntRtng + PstNegCmntRtng) from "post" table and not text in tags

            sQuery = ("SELECT PstSiteName, PstHref, PstTitle, "
                      "(select TagName from tags where Id = TagId1) Tag1,  "
                      "(select TagName from tags where Id = TagId2) Tag2,  "
                      "(select TagName from tags where Id = TagId3) Tag3,  "
                      "(select TagName from tags where Id = TagId4) Tag4,  "
                      "(select TagName from tags where Id = TagId5) Tag5,  "
                      "(select TagName from tags where Id = TagId6) Tag6,  "
                      "(select TagName from tags where Id = TagId7) Tag7,  "
                      "(select TagName from tags where Id = TagId8) Tag8,  "
                      "(select TagName from tags where Id = TagId9) Tag9,  "
                      "(select TagName from tags where Id = TagId10) Tag10, "
                      "Id "
                      "FROM posts "
                      "where not (TagId1 in (select Id from tags where upper(TagName) like '%ТЕКСТ%') or "
                      "           TagId2 in (select Id from tags where upper(TagName) like '%ТЕКСТ%') or "
                      "           TagId3 in (select Id from tags where upper(TagName) like '%ТЕКСТ%') or "
                      "           TagId4 in (select Id from tags where upper(TagName) like '%ТЕКСТ%') or "
                      "           TagId5 in (select Id from tags where upper(TagName) like '%ТЕКСТ%') or "
                      "           TagId6 in (select Id from tags where upper(TagName) like '%ТЕКСТ%') or "
                      "           TagId7 in (select Id from tags where upper(TagName) like '%ТЕКСТ%')) "
                      "ORDER BY (IFNULL(PstVotes, 0) + IFNULL(PstPosCmntRtng, 0) + IFNULL(PstNegCmntRtng, 0)) desc "
                      "LIMIT 1 ")
        # Get Post
        PostCursor.execute(sQuery)
        tplDbPostInfo = PostCursor.fetchone() # Get result

        if tplDbPostInfo:

            objPost.PstSiteName = tplDbPostInfo[0]
            objPost.PstHref     = tplDbPostInfo[1]
            objPost.PstTitle    = tplDbPostInfo[2]
            objPost.TagId1      = '' if tplDbPostInfo[3] is None or tplDbPostInfo[3].isdigit() else TagReplacer(tplDbPostInfo[3])
            objPost.TagId2      = '' if tplDbPostInfo[4] is None or tplDbPostInfo[4].isdigit() else TagReplacer(tplDbPostInfo[4])
            objPost.TagId3      = '' if tplDbPostInfo[5] is None or tplDbPostInfo[5].isdigit() else TagReplacer(tplDbPostInfo[5])
            objPost.TagId4      = '' if tplDbPostInfo[6] is None or tplDbPostInfo[6].isdigit() else TagReplacer(tplDbPostInfo[6])
            objPost.TagId5      = '' if tplDbPostInfo[7] is None or tplDbPostInfo[7].isdigit() else TagReplacer(tplDbPostInfo[7])
            objPost.TagId6      = '' if tplDbPostInfo[8] is None or tplDbPostInfo[8].isdigit() else TagReplacer(tplDbPostInfo[8])
            objPost.TagId7      = '' if tplDbPostInfo[9] is None or tplDbPostInfo[9].isdigit() else TagReplacer(tplDbPostInfo[9])
            objPost.TagId8      = '' if tplDbPostInfo[10] is None or tplDbPostInfo[10].isdigit() else TagReplacer(tplDbPostInfo[10])
            objPost.TagId9      = '' if tplDbPostInfo[11] is None or tplDbPostInfo[11].isdigit() else TagReplacer(tplDbPostInfo[11])
            objPost.TagId10     = '' if tplDbPostInfo[12] is None or tplDbPostInfo[12].isdigit() else TagReplacer(tplDbPostInfo[12])
            objPost.DbId        = tplDbPostInfo[13]
            PostCursor.close()

        else:
            objPost = None

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
          sMessage = "Something is wrong with your user name or password"
          PrintErrorToLog(objLogPaths.sPostPosterLogPath, sMessage)
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
          sMessage = "Database does not exist"
          PrintErrorToLog(objLogPaths.sPostPosterLogPath, sMessage)
        else:
          PrintErrorToLog(objLogPaths.sPostPosterLogPath, err)
    else:
        MySqlConnection.close()
        return objPost

def GetPostContent(objPost):
    try:

        sPostHref = objPost.PstHref

        print('Get post content %s' % sPostHref)

        listPostContent = []

        # Get post html
        sHtmlText = GetHtml(sPostHref)
        if sHtmlText == 'Forbidden':
            print("Link %s Forbidden!" % sPostHref)
            return []

        if not sHtmlText:
            print("CHECK INTERNET CONNECTION!!!")
            raise Exception('Some problem with connection!')
        ##print(sHtmlText)
        objSoup = BeautifulSoup(sHtmlText, "html.parser")
        StoryContainer = objSoup.find('div', class_='story')
        ##StoryContainer = objSoup.find('div', class_='b-story-blocks__wrapper')

        # Get post content
        if StoryContainer:
            StoryWrapper = StoryContainer.find('div', class_='story__wrapper')
            ##StoryWrapper = StoryContainer.find('div', class_='b-story-blocks__wrapper')

            if StoryWrapper:

                # Get available content types
                setParagraphs = set()
                for child in StoryWrapper.recursiveChildGenerator():
                    if str(type(child)) == "<class 'bs4.element.Tag'>":

                        #print(child)

                        # Get text content
                        ParagraphElements = child.find_all(['p', 'a'], recursive = True)
                        if ParagraphElements:

                            for ParagraphElement in ParagraphElements:

                                if ParagraphElement.has_attr('href') and ParagraphElement['href'].split('.')[-1].upper() in ['JPG', 'PNG', 'GIF']:
                                    break

                                for sPostText in ParagraphElement.stripped_strings:

                                    if (sPostText != '' and
                                        not sPostText in setParagraphs and
                                        not [sPostPart for sPostPart in setParagraphs if sPostText in sPostPart] # Not in other exist text
                                        ):

                                        # Word filter
                                        setAllWords = set(TagReplacer(sPostText.upper(), ' ').split(' '))
                                        setFilterIntersection = setAllWords.intersection(setFilterContentWorld)

                                        if (setFilterIntersection and not ('PIKABU' in setFilterIntersection and
                                                                           'HTTP' in setAllWords and
                                                                           sPostText.upper().count('PIKABU') == 1)):
                                            return None

                                        listPostContent.append(sPostText)
                                        setParagraphs.add(sPostText)

                        # Get gif content
                        GifElement = child.find('div', class_='b-gifx__player', recursive=False)
                        if GifElement and GifElement.has_attr('data-src'):
                            sGifHref = GifElement['data-src']
                            listPostContent.append(sGifHref)

                        # Get image content
                        ImgElement = child.find('img', recursive=False)
                        if ImgElement and ImgElement.has_attr('data-large-image') and ImgElement.has_attr('src'):
                            sImgHref = ImgElement['data-large-image']

                            if sImgHref == '':
                                sImgHref = ImgElement['src']

                            listPostContent.append(sImgHref)

    except:
        sMessage = traceback.format_exc()
        PrintErrorToLog(objLogPaths.sPostPosterLogPath, sMessage)
        raise Exception

##    for i in listPostContent:
##        print(i)

    return listPostContent

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

            if Param.get(objParserParameters.isPausedProcess):
               dictParams[objParserParameters.isPausedProcess] = True if Param.get(objParserParameters.isPausedProcess).upper() == 'TRUE' else False

            if Param.get(objParserParameters.isWork):
               dictParams[objParserParameters.isWork] = True if Param.get(objParserParameters.isWork).upper() == 'TRUE' else False

    except:
        sMessage = traceback.format_exc()
        PrintErrorToLog(objLogPaths.sPostPosterLogPath, sMessage)
        return None

    return dictParams


def VkAuth():
    Session = vk.AuthSession(app_id        = app_id,
                             user_login    = user_login,
                             user_password = user_password,
                             scope         = 'wall,photos,pages,docs')
    VkApi = vk.API(Session)

    return VkApi


def OpenPhotos(listPhotoLinks):

    if not isinstance(listPhotoLinks, list):
        listPhotoLinks = [listPhotoLinks]

    listPhotos = []

    for i, sFileHref in enumerate(listPhotoLinks):
        sFileType     = sFileHref.split('.')[-1]
        bPhotoContent = urllib.request.urlopen(sFileHref).read()
        listPhotos.append(
                          ('file%s' % i, ('pic.' + sFileType, bPhotoContent))
                         )
    return listPhotos


def UploadPhoto(VkApi, album_id, group_id, listPhotoLinks, sPostHref = None):

    dictUploadServerParams = {'album_id': album_id}
    dictUploadServerParams['group_id'] = group_id

    upload_url = VkApi.photos.getUploadServer(**dictUploadServerParams)['upload_url']

    http = requests.Session()
    http.proxies = None
    http.headers = {'User-agent': 'Mozilla/5.0 (Windows NT 6.1; rv:40.0) '
                    'Gecko/20100101 Firefox/40.0'}

    listPhotos = OpenPhotos(listPhotoLinks)
    VkResponse = (http.post(upload_url, files = listPhotos)).json()

    if 'album_id' not in VkResponse:
        VkResponse['album_id'] = VkResponse['aid']

    dictUploadServerParams.update(VkResponse)

    dictUploadServerParams.update({
                                    'latitude':    None,
                                    'longitude':   None,
                                    'caption':     sPostHref,
                                    'description': None
                                  })

    LoadPhotoResults = VkApi.photos.save(**dictUploadServerParams)

    return LoadPhotoResults


def UploadPhotoFromPost(listPostContent, sPostHref):
    try:
        def UploadAndReplaceContent(VkApi, album_id, group_id, listPhotoLinks, listPhotoIndex, listPostContent):

            LoadPhotoResults = UploadPhoto(VkApi, album_id, group_id, listPhotoLinks)

            for nRplaceIndex, nIndexPostContent in enumerate(listPhotoIndex):
                owner_id = LoadPhotoResults[nRplaceIndex]['owner_id']
                pid      = LoadPhotoResults[nRplaceIndex]['pid']
                sPhotoVkLink = '[[photo%s_%s|577px| ]]' % (owner_id, pid)
                listPostContent[nIndexPostContent] = sPhotoVkLink

            return listPostContent

        if not listPostContent:
            return listPostContent

        sHrefPattern = 'http://'

        VkApi = VkAuth()

        nImgCountIndex = 0
        listPhotoLinks = []
        listPhotoIndex = []

        for i, sPostLine in enumerate(listPostContent):

            if (sPostLine.startswith(sHrefPattern) and
               sPostLine.split('.')[-1].upper() in ['PNG','JPG']):
                nImgCountIndex += 1
                listPhotoLinks.append(sPostLine)
                listPhotoIndex.append(i)

                if nImgCountIndex == nMaxLoadingImageCount:

                    listPostContent = UploadAndReplaceContent(VkApi, album_id, group_id, listPhotoLinks, listPhotoIndex, listPostContent)

                    listPhotoLinks = []
                    listPhotoIndex = []
                    nImgCountIndex = 0

        else:

            if nImgCountIndex > 0:

                listPostContent = UploadAndReplaceContent(VkApi, album_id, group_id, listPhotoLinks, listPhotoIndex, listPostContent)

                listPhotoLinks = []
                listPhotoIndex = []
                nImgCountIndex = 0

        return listPostContent
    except:
        sMessage = 'Error with upload photo to VK!'
        PrintErrorToLog(objLogPaths.sPostPosterLogPath, sMessage)
        return None

def PrepareGifToUpload(sPhotoLink):
    try:

        listGifContent = []

        sFileType     = sPhotoLink.split('.')[-1]
        bPhotoContent = urllib.request.urlopen(sPhotoLink).read()
        listGifContent.append(
                          ('file', ('pic.' + sFileType, bPhotoContent))
                         )
        return listGifContent

    except:
        sMessage = 'Error with upload GIF to VK! PrepareGifToUpload'
        PrintErrorToLog(objLogPaths.sPostPosterLogPath, sMessage)
        return None


def UploadGif(VkApi, sGifHref):
    try:
        dictUploadServerParams = {'group_id' : gif_container_group_id}

        upload_url = VkApi.docs.getUploadServer(**dictUploadServerParams)['upload_url']

        http = requests.Session()
        http.proxies = None
        http.headers = {'User-agent': 'Mozilla/5.0 (Windows NT 6.1; rv:40.0) '
                        'Gecko/20100101 Firefox/40.0'}

        listGifContent = PrepareGifToUpload(sGifHref)
        VkResponse = (http.post(upload_url, files = listGifContent)).json()

        dictUploadServerParams = {
                                   'file':    VkResponse['file'],
                                   'title':   None,
                                   'tags':    None
                                 }

        LoadGifResults = VkApi.docs.save(**dictUploadServerParams)

        return LoadGifResults[0]

    except:
        sMessage = 'Error with upload GIF to VK! UploadGif'
        PrintErrorToLog(objLogPaths.sPostPosterLogPath, sMessage)
        return None


def UploadGifsFromPost(listPostContent):
    try:
        if not listPostContent:
            return listPostContent

        sHrefPattern = 'http://'

        VkApi = VkAuth()

        for nIndex, sPostLine in enumerate(listPostContent):

            if (sPostLine.startswith(sHrefPattern) and
               sPostLine.split('.')[-1].upper() in ['GIF']):

                LoadPhotoResults = UploadGif(VkApi, sPostLine)

                if LoadPhotoResults:

                    owner_id = LoadPhotoResults['owner_id']
                    id       = LoadPhotoResults['did']

                    sGifVkLink = '[[doc%s_%s|577px| ]]' % (owner_id, id)
                    listPostContent[nIndex] = sGifVkLink

        return listPostContent

    except:
        sMessage = 'Error with upload GIF to VK! UploadGifsFromPost'
        PrintErrorToLog(objLogPaths.sPostPosterLogPath, sMessage)
        return None

def SaveToWikiPage(listPostContent, sPostHref, sPostTitle):
    try:
        VkApi = VkAuth()

        sPostWikiText = ''
        for sPostLine in listPostContent:
            sPostWikiText += '%s\r\n' % sPostLine

        ##sPostWikiText += '\r\n\r\n%s' % sPostHref

        sPostTitle = sPostTitle.replace('.','').strip()

        WikiPageResponse = VkApi.pages.save(text = sPostWikiText, group_id = group_id, user_id = user_id, title = sPostTitle)

        return WikiPageResponse

    except:
        sMessage = 'Error with SaveToWikiPage!'
        PrintErrorToLog(objLogPaths.sPostPosterLogPath, sMessage)
        return None

def PostPageOnWallWithWiki(WikiPageResponse, objPost, listPostContent, isHeaderNeed, nTotalTextLength):
    try:

        VkApi = VkAuth()

        sPostWallText   = ''
        sFirstPostPhoto = ''
        sWallText       = ''

        # Get first photo and all text
        for sPostLine in listPostContent:
            if sPostLine.startswith('[[photo') and sFirstPostPhoto == '':
                sFirstPostPhoto = sPostLine
            elif not sPostLine.startswith('[['):
                sPostWallText += '%s\r\n' % sPostLine

        # Get tags
        ##sTagText = '#%s #%s #%s #%s #%s #%s #%s #%s #%s #%s' % (objPost.TagId1, objPost.TagId2, objPost.TagId3, objPost.TagId4, objPost.TagId5, objPost.TagId6, objPost.TagId7, objPost.TagId8 ,objPost.TagId9 ,objPost.TagId10)
        sTagText = '#%s #%s' % (objPost.TagId1, objPost.TagId2)

        # Skip some tags
        for sSkipTagsVkWallPost in tplSkipTagsVkWallPost:
            if sSkipTagsVkWallPost in sTagText:
                sTagText = sTagText.replace(sSkipTagsVkWallPost, '')

        sTagText = sTagText.replace('# ', '')

        if len(sTagText) > 2 and sTagText[-2:] == ' #':
            sTagText = sTagText[:-2]
        elif sTagText == '#':
            sTagText = ''

        if sTagText != '':
            sTagText += ' \r\n\r\n'

        ##sTagText = sTagText.replace(' ', '@%s ' % GroupName) # Local hash tag

        # Get started nWallTextCharsCnt chars from text
        ## TBD: Tag on/off parameter to implement
        sWallText = sTagText

        nCharCnt = 0
        for sPostChar in sPostWallText:
            sWallText += sPostChar
            nCharCnt  += 1
            if  nCharCnt >= nWallTextCharsCnt and sPostChar == '.':
                break

        # Add href to wiki page
        sShortWikiHref = 'page-%s_%s' % (group_id, WikiPageResponse)
        sWikiHref  = 'https://new.vk.com/page-%s_%s' % (group_id, WikiPageResponse)
        sWallText += '..\r\n\r\nПодробнее: %s' % sWikiHref

        # Attachments
        if sFirstPostPhoto != '':
            sFirstPostPhoto = sFirstPostPhoto.split('|')[0][2:]
            attachments = '%s,%s' % (sFirstPostPhoto, sShortWikiHref)
        else:
            attachments = ''

        # Post to wall
        dictWallPostResult = VkApi.wall.post(owner_id = '-%s' % group_id, from_group = 1, message = sWallText, attachments = attachments, signed = 0)
        PostId = int(dictWallPostResult['post_id'])
        return PostId

    except:
        sMessage = 'Error with PostPageOnWallWithWiki!'
        PrintErrorToLog(objLogPaths.sPostPosterLogPath, sMessage)
        return None

def PostPageOnWallWithOutWiki(WikiPageResponse, objPost, listPostContent, isHeaderNeed, nTotalTextLength):
    try:

        VkApi = VkAuth()

        sPostWallText       = ''
        listPostAttachments = []

        if isHeaderNeed:
            sPostWallText += objPost.PstTitle + '\r\n\r\n'

        # Get first photo and all text
        for sPostLine in listPostContent:
            if sPostLine.startswith('[[') :
                listPostAttachments.append(sPostLine)
            elif not sPostLine.startswith('[['):
                sPostWallText += '%s\r\n' % sPostLine

        # Get tags
        ##sTagText = '#%s #%s #%s #%s #%s #%s #%s #%s #%s #%s' % (objPost.TagId1, objPost.TagId2, objPost.TagId3, objPost.TagId4, objPost.TagId5, objPost.TagId6, objPost.TagId7, objPost.TagId8 ,objPost.TagId9 ,objPost.TagId10)
        sTagText = '#%s #%s' % (objPost.TagId1, objPost.TagId2)

        # Skip some tags
        for sSkipTagsVkWallPost in tplSkipTagsVkWallPost:
            if sSkipTagsVkWallPost in sTagText:
                sTagText = sTagText.replace(sSkipTagsVkWallPost, '')

        sTagText = sTagText.replace('# ', '')

        if len(sTagText) > 2 and sTagText[-2:] == ' #':
            sTagText = sTagText[:-2]
        elif sTagText == '#':
            sTagText = ''

        if sTagText != '':
            sTagText = '\r\n' + sTagText + ' '

##        sTagText = sTagText.replace(' ', '@%s ' % GroupName)

        # Get started nWallTextCharsCnt chars from text
        ## TBD: Tag on/off parameter to implement
        sPostWallText += sTagText

        # Add href to source page
        ##sPostWallText += '\r\n%s' % objPost.PstHref

        # Attachments
        sAttachments = ''
        for sPostAttachment in listPostAttachments:
            sPostAttachment  = sPostAttachment.split('|')[0][2:]
            #sAttachments    += '%s,%s,' % (sPostAttachment, sPostAttachment)
            sAttachments    += '%s,' % (sPostAttachment)

        if sAttachments != '':
            sAttachments = sAttachments[:-1]

        # Post to wall
        dictWallPostResult = VkApi.wall.post(owner_id = '-%s' % group_id, from_group = 1, message = sPostWallText, attachments = sAttachments, signed = 0)
        PostId = int(dictWallPostResult['post_id'])
        return PostId

    except:
        sMessage = 'Error with PostPageOnWallWithOutWiki!'
        PrintErrorToLog(objLogPaths.sPostPosterLogPath, sMessage)
        return None

def UpdateDatabaseInfo(objPost, PostId, WikiPageResponse, nAttachmentsCnt, nTotalTextLength, sModifier):
    try:
        # Connect to DB
        MySqlConnection = mysql.connector.connect(**MysqlConnectorConfig)

        sSourceTable = dictModifierMappingToTables[sModifier] # Define source table. archiveposts or posts

        # Insert rows from "posts" or "archiveposts" table into "posted_public" table
        PostCursor = MySqlConnection.cursor()

        sQuery = ("insert into posted_public"
                  "    select (select (max(Id) + 1) Id from posted_public) as Id, PstSiteId, PstSiteName, PstHref, PstTitle, PstVotes, PstPosVotesCnt, PstNegVotesCnt, PstPosCmntRtng, PstNegCmntRtng, PstCmntCnt, PstPosCmntCnt, PstNegCmntCnt, PstNeuCmntCnt, PstFbLkCnt, PstVkLkCnt, PstTwLkCnt, PstSvCnt, PstDateTime, PstAuthorNick, PstAuthorHref, TagId1, TagId2, TagId3, TagId4, TagId5, TagId6, TagId7, TagId8, TagId9, TagId10, PstVotesPerSec, PstCommentPerSec, PstUpdCnt, "
                  "    %(nPostId)s as PostedPublicId, "
                  "    %(nWikiPageResponse)s as WikiPageId, "
                  "    %(nTextLength)s as TextLength, "
                  "    %(nAttachmentCnt)s as AttachmentCnt "
                  "    from " + sSourceTable + " "
                  "    where Id = %(nDbId)s")

        PostCursor.execute(sQuery, {'nDbId'             : objPost.DbId,
                                    'nPostId'           : PostId,
                                    'nWikiPageResponse' : WikiPageResponse,
                                    'nTextLength'       : nTotalTextLength,
                                    'nAttachmentCnt'    : nAttachmentsCnt})

        PostCursor.close()

        # Remove rows from "posts" or "archiveposts" table
        PostCursor = MySqlConnection.cursor()

        sQuery = ("delete from " + sSourceTable + " where Id = %(nDbId)s")

        PostCursor.execute(sQuery, {'nDbId' : objPost.DbId})

        PostCursor.close()

        MySqlConnection.commit()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
          sMessage = "Something is wrong with your user name or password"
          PrintErrorToLog(objLogPaths.sPostPosterLogPath, sMessage)
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
          sMessage = "Database does not exist"
          PrintErrorToLog(objLogPaths.sPostPosterLogPath, sMessage)
        else:
          PrintErrorToLog(objLogPaths.sPostPosterLogPath, err)
    else:
        MySqlConnection.close()


def ClassifyContent(listPostContent):

    listContentClasses = []

    # Get content classes
    for sPostItem in listPostContent:
        if sPostItem.startswith('[[photo'):
            listContentClasses.append(['P', 0])
            continue
        elif sPostItem.startswith('[[doc'):
            listContentClasses.append(['D', 0])
            continue
        elif sPostItem.startswith('[[video'):
            listContentClasses.append(['V', 0])
            continue
        else:
            listContentClasses.append(['T', len(sPostItem)])

    isHeaderNeed = True

    # Define post parameters
    isComplexPost       = False
    nAttachmentsCnt     = 0
    nTotalTextLength    = 0

    for listPostClass in listContentClasses:

        if listPostClass[0] == 'T' and nAttachmentsCnt > 1:
            isComplexPost = True

        if listPostClass[0] == 'T':
            nTotalTextLength += listPostClass[1]

        if listPostClass[0] in ['P', 'D', 'V']:
            nAttachmentsCnt += 1

    isWikiPageNeed = isComplexPost

    if nTotalTextLength > nMaxPostChars:
        isWikiPageNeed = True

    if nAttachmentsCnt == 0:
        isHeaderNeed = False

    return isWikiPageNeed, isHeaderNeed, nAttachmentsCnt, nTotalTextLength


def PostContentIntoGroup(listPostContent, objPost, sModifier):
    try:

        PrintMessageToLog(objLogPaths.sPostPosterLogPath, 'Start posted %s' % objPost.PstHref)

        if not listPostContent:
            UpdateDatabaseInfo(objPost, 0, 0, 0, 0, sModifier)
            return None

        listPostContent = UploadPhotoFromPost(listPostContent, objPost.PstHref)

        if not listPostContent:
            UpdateDatabaseInfo(objPost, 0, 0, 0, 0, sModifier)
            return None

        listPostContent = UploadGifsFromPost(listPostContent)

        if not listPostContent:
            UpdateDatabaseInfo(objPost, 0, 0, 0, 0, sModifier)
            return None

        isWikiPageNeed, isHeaderNeed, nAttachmentsCnt, nTotalTextLength = ClassifyContent(listPostContent)

        if isWikiPageNeed:
            WikiPageResponse = SaveToWikiPage(listPostContent, objPost.PstHref, objPost.PstTitle)
        else:
            WikiPageResponse = None

        if not WikiPageResponse and isWikiPageNeed:
            UpdateDatabaseInfo(objPost, 0, 0, 0, 0, sModifier)
            return None

        if isWikiPageNeed and WikiPageResponse:
            PostId = PostPageOnWallWithWiki(WikiPageResponse, objPost, listPostContent, isHeaderNeed, nTotalTextLength)
        else:
            PostId = PostPageOnWallWithOutWiki(WikiPageResponse, objPost, listPostContent, isHeaderNeed, nTotalTextLength)

        if not PostId:
            UpdateDatabaseInfo(objPost, 0, 0, 0, 0, sModifier)
            return None

        UpdateDatabaseInfo(objPost, PostId, WikiPageResponse, nAttachmentsCnt, nTotalTextLength, sModifier)

        PrintMessageToLog(objLogPaths.sPostPosterLogPath, 'https://new.vk.com/postilla?w=wall-119200682_%s' % PostId)
        PrintMessageToLog(objLogPaths.sPostPosterLogPath, 'Success!')
        return []

    except:
        sMessage = traceback.format_exc()
        PrintErrorToLog(objLogPaths.sPostPosterLogPath, sMessage)
        return None


def PostByShedule():
    try:

        print("Post_Poster")

        # Read parser params
        ParserParams = ReadXMLParemeters(objXmlConfigFile.sPostPosterXMLFilePath)
        isWork       = ParserParams[objParserParameters.isWork]
        listPostContent = []

        while(isWork):

            # Continue if pause exists
            if not ParserParams[objParserParameters.isPausedProcess]:

                # Sleep for next task
                PrintMessageToLog(objLogPaths.sPostPosterLogPath, 'Get next delay from shedule')
                nTimeDiffSec, sModifier = GetNextPostSecondsDelayAndModifier()

                if not listPostContent is None:
                    PrintMessageToLog(objLogPaths.sPostPosterLogPath, '\tWaiting %s min' % round((nTimeDiffSec / 60.0), 1))
                    time.sleep(nTimeDiffSec)
                else:
                    PrintMessageToLog(objLogPaths.sPostPosterLogPath, '\tNo waiting')


                # Get post from DB (href, tags)
                PrintMessageToLog(objLogPaths.sPostPosterLogPath, 'Get post from DB')
                objPost = GetPostFromDb(sModifier)

                if not objPost:
                    PrintMessageToLog(objLogPaths.sPostPosterLogPath, '\tErrors with get post info from DB!')
                    listPostContent = []
                    continue

                # Parse post content
                listPostContent = GetPostContent(objPost)

                setFilteredWord = (set(TagReplacer(objPost.PstTitle.upper(), ' ').split(' ')).intersection(setFilterTitleWorld))
                if setFilteredWord:
                    PrintMessageToLog(objLogPaths.sPostPosterLogPath, '\tPost title contain word from filter dictionary. PostDbId %s' % objPost.DbId)
                    listPostContent = None

                # Post content into group
                PrintMessageToLog(objLogPaths.sPostPosterLogPath, 'Start post to VK wall')
                listPostContent = PostContentIntoGroup(listPostContent, objPost, sModifier)

                time.sleep(60)

            else:
                # Continue sleep for next task
                time.sleep(1)
                nTimeDiffSec, sModifier = GetNextPostSecondsDelayAndModifier()
                time.sleep(nTimeDiffSec)


            # Read Xml attributes again
            ParserParams = ReadXMLParemeters(objXmlConfigFile.sPostPosterXMLFilePath)

            # Check on stop process
            isWork = ParserParams[objParserParameters.isWork]

    except:
        sMessage = traceback.format_exc()
        PrintErrorToLog(objLogPaths.sPostPosterLogPath, sMessage)
        raise Exception('Error in PostByShedule!')

PostByShedule()
