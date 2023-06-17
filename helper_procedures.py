#BUILTIN------------------------------------------------------------------------
import os
import sys
import time
import fnmatch
import traceback
import subprocess
import urllib.request

#PROJECT------------------------------------------------------------------------
from model_parameters import *

#-------------------------------------------------------------------

def PrintMessageInFile(sLogFilePath, sMessage):
    try:
        print(sMessage)
        fileLogFile = open(sLogFilePath, 'a') #exception
        fileLogFile.write(sMessage + "\n")    #exception
    except:
        print (traceback.format_exc())
    finally:
        if 'fileLogFile' in locals(): fileLogFile.close() #exception

#-------------------------------------------------------------------

def PrintMessageToLog(sLogFilePath, sMessage = ''):
    sMessage = time.strftime('%d %b %H:%M:%S') + "\tINFO\t" + str(sMessage)
    PrintMessageInFile(sLogFilePath, sMessage)

#-------------------------------------------------------------------

def PrintErrorToLog(sLogFilePath, sMessage = ''):
    sMessage = time.strftime('%d %b %H:%M:%S') + "\tERROR\t" + str(sMessage)
    PrintMessageInFile(sLogFilePath, sMessage)

#-------------------------------------------------------------------

def PrintWarningToLog(sLogFilePath, sMessage = ''):
    sMessage = time.strftime('%d %b %H:%M:%S') + "\tERROR\t" + str(sMessage)
    PrintMessageInFile(sLogFilePath, sMessage)

#-------------------------------------------------------------------
def TryInt(nSomeVarToInt):
    try:
        return int(nSomeVarToInt)
    except:
        return 0

#-------------------------------------------------------------------

def GetHtml(sUrl, Iter = 0):
    try:
        sResponse = urllib.request.urlopen(sUrl)
        if sResponse:
            return sResponse.read()
        else:
            return None
    except:
        if Iter == 3:
            return None
        else:
            Iter += 1
            time.sleep(0.5)
            return GetHtml(sUrl, Iter)

#-------------------------------------------------------------------