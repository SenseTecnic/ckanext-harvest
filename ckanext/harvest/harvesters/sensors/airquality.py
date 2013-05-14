#-------------------------------------------------------------------------------
# Name:        air-quality.py
# Purpose:     Gathers a selection of BC air quality data hourly
#              and sends it to the WoTKit
#
# Created:     March 1st, 2013   
#-------------------------------------------------------------------------------
#!/usr/bin/env python

import pdb
import sys
import string
import time

import urllib
import urllib2
from bs4 import BeautifulSoup, NavigableString
import sensetecnic
import requests

import logging
log = logging.getLogger(__name__)

#WoTKit key for user: sensetecnic
key_id=''
key_password=''

#BC Air Qualtity Site
#Readings from site updated ~hourly; may keep some old readings with new readings
POLLUTION_DATA_URL = "http://www.bcairquality.ca/aqo/xml/Current_Hour.xml";

#Fields saved to wotkit
field_name_dict = {'C--O':['co', 'Carbon Monoxide'], 'H2-S':['h2s', 'Hydrogen Sulfide'], 'NO-2':['no2','Nitrogen Dioxide'], 'OZNE':['ozone','Ozone (O3)'],
                   'PM10':['pm10','Coarse dust particles (d=10um)'], 'PM25':['pm25','Fine particles (d=2.5um)'],'SO-2':['so2','Sulfur Dioxide']}

#Dictionary of 'registered' stations and their last update
last_station_update ={}

def getURL(URLString):
# takes a URL to open, returns an open file or a -1

    try:
        request = requests.get(URLString, headers={'User-Agent': 'httplib', 'Content-Type': 'application/json'})
    except Exception as e:
        print e.message
        return -1
    return request.text
# end def getURL()

def getPollutionData ():
    global last_station_update

    sensors = []

    #Get XML DATA
    f = getURL(POLLUTION_DATA_URL)
    if f == -1: return None

    decoded = f.decode("ascii")
    soup = BeautifulSoup(decoded.encode('utf-8'))
    
    #Iterate through all stations
    allStations= soup.findAll('strd')
    for station in allStations:

        station_id = station['id']
        sensor_name = "aq_%s" % (station['id'])
        sensors.append(sensor_name)
        registeredStation = last_station_update.has_key(station_id) 
        
        #If station 'unregistered', save station information
        station_info= {}
        if not registeredStation:
            station_info['id'] = station_id
            station_info['descn'] = station['descn']
            station_info['name'] = station['name']
            station_info['city'] = station['city']
            #Converting latitude and longitude into decimal
            #BECAUSE IN BC, will assume direction is N W (as such, lat>0, lng<0)
            try:
                raw_lat = station['lat'].split()
                raw_lng = station['long'].split()
                station_info['lat'] = float(raw_lat[0]) + (float(raw_lat[1])*60 + float(raw_lat[2]))/3600
                station_info['lng'] = -float(raw_lng[0]) + (float(raw_lng[1])*60 + float(raw_lng[2]))/3600
            except:
                station_info['lat'] = 0
                station_info['lng'] = 0

        #Iterate through all readings for a station
        temp_last_update = [] #collects timestamps for all readings more recent than last station update    
        allReadings = station.findAll('rd')
        for reading in allReadings:
            try:
                date_raw = reading['dt']
                date_struct = time.strptime(date_raw,'%Y,%m,%d,%H,%M,%S')
                date = int(time.mktime(date_struct)*1000)
                           
                #If station 'registered', get only readings more recent than last station update 
                #Else, get ALL readings for that station
                if registeredStation:  
                    if last_station_update[station_id] >= date :
                        print "Already read data for Station %s from %s:%s" % (station_id, date_struct[3], date_struct[4])
                        continue
                       
                #Iterate through all parameter fields for a reading
                wotkit_data = {}
                field_info = {}
                allParameters = station.findAll('pv')
                for params in allParameters:
                    raw_fieldname = params['nm']
                    
                    #Only save parameter fields in field_name_dictionary 
                    if field_name_dict.has_key(raw_fieldname):
                        field = field_name_dict[raw_fieldname][0]
                        field_longname = field_name_dict[raw_fieldname][1]
                        value = float(params['vl'])
                        wotkit_data.update({field:value})
    
                        #If station unregistered, save parameter field info
                        if not registeredStation:
                            field_info.update({field:field_longname})
                #end "for params in allParameters:"
                            
                #If relevant data collected, send to corresponding WoTKit sensor            
                if wotkit_data:               
                    if not registeredStation:
                        #Only check if the sensor exists ONCE
                        if not temp_last_update:                   
                            checkWOTKITSensorsExist(station_info, field_info)
                            
                    temp_last_update.append(date)
                    
                    wotkit_data.update({'timestamp':date, 'value':0})
                    sensetecnic.sendData(sensor_name, key_id , key_password, wotkit_data)
                    print sensor_name, wotkit_data
                else:
                    print 'No relevant data from Station %s' % (station_id)
            except Exception as e:
                log.error("Failed to update sensor readings for " + sensor_name + ", Error: " + e.message)
                continue
        #end "for reading in allReadings"

        if temp_last_update:
            last_station_update[station_id] = max(temp_last_update) #updates last station update 
        
    #end "for station in allStations"                

    return sensors 
# end def getPollutionData()                
           
def checkWOTKITSensorsExist(station, fields):
    #Assumes if you cannot get sensor data, sensor doesn't exist and creates it.
    sensor_name = "aq_%s" % (station['id'])
    
    try:
        sensetecnic.getSensor(sensor_name, key_id , key_password);
        print "ROSEY Sensor %s already exists." % (sensor_name)
    except:
        sensor = {}
        sensor = {  "name":sensor_name,
                    "longName":"Station %s" % (station['name']),
                    "description":"%s: %s in %s" %(station['descn'], station['name'],station['city']),
                    "latitude": station['lat'] ,
                    "longitude": station['lng'],
                    "private":False,
                    "tags":["air quality", "BC"] }

        field_list = []

        for name, longname in fields.items():
            field_list.append({"name":name,"type":"NUMBER",
                               "required":False,"longName":longname})
            
        field_list.append({"name":"lat","type":"NUMBER","required":True,"longName":"latitude"})
        field_list.append({"name":"lng","type":"NUMBER","required":True,"longName":"longitude"})
        field_list.append({"name":"value","type":"NUMBER","required":True,"longName":"Data"})
        field_list.append({"name":"message","type":"STRING","required":False,"longName":"Message"})

        sensor.update({"fields":field_list})
        
        try:
            sensetecnic.registerSensor(sensor, key_id , key_password);
            print ("Created WoTKit Sensor:",sensor_name)
        except sensetecnic.SenseTecnicError as e:
            print "ERROR: %s" % (e)
# end def checkWOTKITSensorsExist() 
    
     
def updateWotkit():
    result = getPollutionData()
    if not result:
        raise Exception("Failed to update data to wotkit")
    return result
    #1hr (data updated ~ hourly)
    #time.sleep(3600)  
# end def main
