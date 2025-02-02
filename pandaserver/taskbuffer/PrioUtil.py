import re
import json


# special permission
PERMISSION_KEY = 'k'
PERMISSION_PROXY = 'p'
PERMISSION_SUPER_USER = 's'
PERMISSION_SUPER_GROUP = 'g'



# convert UTF-8 to ASCII in json dumps
def unicodeConvert(input):
    if isinstance(input,dict):
        retMap = {}
        for tmpKey,tmpVal in input.iteritems():
            retMap[unicodeConvert(tmpKey)] = unicodeConvert(tmpVal)
        return retMap
    elif isinstance(input,list):
        retList = []
        for tmpItem in input:
            retList.append(unicodeConvert(tmpItem))
        return retList
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    return input



# decode
def decodeJSON(inputStr):
    return json.loads(inputStr,object_hook=unicodeConvert)



# calculate priority for user jobs
def calculatePriority(priorityOffset,serNum,weight):
    priority = 1000 + priorityOffset - (serNum / 5) - int(100 * weight)
    return priority
