'''
Created on Jul 7, 2010

@author: mike
'''
import pdb
import urllib
import urllib2
import base64
import random
import time
import datetime
import simplejson as json

import logging
log = logging.getLogger(__name__)

BROKER_BASE_URL='http://localhost:8800/osgibroker/event'
STS_BASE_URL='http://wotkit.sensetecnic.com/api'
STS_ID=''
STS_KEY=''

def init(url, id, key):
    global STS_BASE_URL, STS_ID, STS_KEY
    log.debug("Setting wotkit url: " + url)
    log.debug("Setting wotkit id: " + id)
    log.debug("Setting wotkit key: " + key)
    
    STS_BASE_URL = url + "/api"
    STS_ID = id
    STS_KEY = key

def getWotkitUrl():
    return STS_BASE_URL

def _checkPassword(user, password):
    """ If user or password is empty, change it to the default configured credentials 
    """
    if not user or not password:
        return (STS_ID, STS_KEY)
    else:
        return (user, password)
    
class SenseTecnicError(Exception):
    """Exception raised for errors in the input.

    Attributes:
        msg  -- explanation of the error
        e    -- exception that caused this exception
    """

    def __init__(self, msg, e=None):
        self.msg = msg
        self.e = e

def sendData(sensor, user, password, attributes):
    #use SenseTecnic
    sendDataSenseTecnic(sensor, user, password, attributes);

def sendDataSenseTecnic(sensor, user, password, attributes):
    encoded_args = urllib.urlencode(attributes)

    user, password = _checkPassword(user, password)

    # send authorization headers premptively otherwise we get redirected to a login page
    base64string = base64.encodestring('%s:%s' % (user, password))[:-1]
        
    headers = {
        'User-Agent': 'httplib',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': "Basic %s" % base64string
    }
    
    url = STS_BASE_URL+'/sensors/'+sensor+'/data'
    req = urllib2.Request(url,encoded_args,headers)
    try:
        urllib2.urlopen(req)
    except urllib2.URLError, e:
        print 'error - sending event to sensor: %s' % (sensor)
        print e.reason
        return -1
    return 0

def getSensor(sensorName, user, password):
    # send authorization headers preemptively otherwise we get redirected to a login page
    user, password = _checkPassword(user, password)
    base64string = base64.encodestring('%s:%s' % (user, password))[:-1]
        
    headers = {
        'User-Agent': 'httplib',
        'Content-Type': 'application/json',
        'Authorization': "Basic %s" % base64string
    }
    
    url = STS_BASE_URL+'/sensors/'+sensorName
    req = urllib2.Request(url,None,headers)
    try:
        result = urllib2.urlopen(req)
        sensor = json.load(result)
        
    except urllib2.HTTPError, e:
        if (e.code == 404):
            raise SenseTecnicError('%s does not exist' % (sensorName), e)
            return
        
        if (e.code != 204):
            print '%s while getting: %s' % (e,sensorName)
            raise SenseTecnicError(e,e)
            return
               
    except urllib2.URLError, e:
        print 'error - getting sensor: %s' % (sensorName)
        raise SenseTecnicError('URLError while registering %s' % (sensorName),e)
        return
    
    return sensor

def registerSensor(sensor, user, password):
    jsonSensor = json.dumps(sensor)

    user, password = _checkPassword(user, password)
    # send authorization headers preemptively otherwise we get redirected to a login page
    base64string = base64.encodestring('%s:%s' % (user, password))[:-1]
        
    headers = {
        'User-Agent': 'httplib',
        'Content-Type': 'application/json',
        'Authorization': "Basic %s" % base64string
    }
    
    url = STS_BASE_URL+'/sensors'
    req = urllib2.Request(url,jsonSensor, headers)
    try:
        urllib2.urlopen(req)
    except urllib2.HTTPError, e:
        if (e.code != 204):
            print '%s while registering: %s' % (e,sensor["name"])
            raise SenseTecnicError(e,e)
               
    except urllib2.URLError, e:
        print 'error - registering sensor: %s' % (sensor["name"])
        raise SenseTecnicError('URLError while registering %s' % (sensor["name"]),e)
    
    return 0


