import urllib2

from ckan.lib.base import c
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action
from ckan.lib.helpers import json

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError

import requests
from hashlib import sha1
import importlib
from ckanclient import CkanClient

import logging
log = logging.getLogger(__name__)

from base import HarvesterBase

class WotkitHarvester(HarvesterBase):
    '''
    A Harvester for CKAN instances
    '''
    config = None

    api_version = '3'

    def info(self):
        return {
            'name': 'wotkit',
            'title': 'WOTKIT',
            'description': 'Harvests sensor data and feeds it into the wotkit',
            'form_config_interface':'Text'
        }

    def _set_config(self,config_str):
        if config_str:
            self.config = json.loads(config_str)

            if 'api_version' in self.config:
                self.api_version = self.config['api_version']

            log.debug('Using config: %r', self.config)
        else:
            self.config = {}


    def validate_config(self,config):
        """ Validate config string provided through harvester UI (must be valid json) which provide options for wotkit harvest
        """

        if not config:
            return config


        config_obj = json.loads(config)
        if not 'gather_sensors' in config_obj:
            raise ValueError('gather_sensors must exist in config')

        if 'gather_sensors' in config_obj:
            if not isinstance(config_obj['gather_sensors'], list):
                raise ValueError('gather_sensors must be a list')
            
            for sensor_module in config_obj['gather_sensors']:
                if not isinstance(sensor_module, dict):
                    raise ValueError('Each object in gather_sensor must be a dictionary (map)')
                if "module" not in sensor_module:
                    raise ValueError('Each object in gather_sensor must contain "module"')
                if "package_name" not in sensor_module:
                    raise ValueError('Each object in gather_sensor must contain "package_name"')

                    
                try:
                    importlib.import_module("ckanext.harvest.harvesters.sensors." + sensor_module["module"], "ckanext")
                except Exception as e:
                    raise Exception("Failed to import wotkit sensor harvester module for: " + sensor_module["module"] + 
                                        ". Make sure module is defined in ckanext-harvest/ckanext/harvest/harvesters/sensors/")

        return config


    def gather_stage(self,harvest_job):
        log.debug('In WotkitHarvester gather_stage (%s)' % harvest_job.source.url)
        get_all_packages = True
        package_ids = []

        self._set_config(harvest_job.source.config)

        sensors = self.config["gather_sensors"]
        ids = []
        for sensor in sensors:
            try:
                importlib.import_module("ckanext.harvest.harvesters.sensors." + sensor["module"], "ckanext")
                id = sha1(sensor["module"]).hexdigest()
                obj = HarvestObject(guid=id, job=harvest_job, content=sensor["module"])
                obj.save()
                ids.append(obj.id)
            except ImportError as e:
                log.error("Failed to import " + sensor["module"] + ". There is no valid module under harvesters/sensors/")
            except Exception as e:
                log.error(e.message)

        return ids

    def fetch_stage(self,harvest_object):
        return True

    def import_stage(self,harvest_object):
        log.debug('In WotkitHarvester import_stage, fetching the data and pushing it to the Wotkit')
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id,
                    harvest_object, 'Import')
            return False

        self._set_config(harvest_object.job.source.config)
        moduleName = harvest_object.content
        
        # Extract packagename from config
        packageName = None
        for sensor_module in self.config['gather_sensors']:
            if sensor_module['module'] == moduleName:
                packageName = sensor_module['package_name']
                
        package_dict = {
                    'resources': []
        }
        try:
            # Hacky solution to import wotkit aggregation modules dropped in .sensors/ so things could be changed through config params
            module = importlib.import_module("ckanext.harvest.harvesters.sensors." + moduleName, "ckanext")
            # All wotkit modules defined with updateWotkit function (ducktyping)
            # Must return list of sensor names that corresponds to {WOTKIT_API_URL}/sensors/{SENSOR_NAME_HERE} for wotkit api access
            try:
                updated_sensors = module.updateWotkit()
            except Exception as e:
                log.error("Failed to update wotkit for module: " + moduleName + ". " + e.message)
                raise e

            package_dict['notes'] = "Harvested with wotkit_harvester"       
                
            # Somewhat redundant step that attempts to fetch all sensor data from wotkit that was just pushed
            import sensors.sensetecnic as sensetecnic            
            wotkit_url = sensetecnic.getWotkitUrl()
            
            for sensorName in updated_sensors:
                # Use default wotkit credentials supplied by .ini config
                try:
                    sensor_dict = sensetecnic.getSensor(sensorName, None, None)
                except Exception as e:
                    log.error("Failed to get sensor: " + sensorName + " from wotkit. Skipping resource creation on ckan")
                    continue
            
            
                package_dict['resources'].append({'url': wotkit_url + "/sensors/" + sensorName,
                                                  'name': sensorName,
                                                  'format': 'application/json',
                                                  'description': sensor_dict["description"],
                                                  '__extras': sensor_dict})
            
        except ValidationError,e:
            self._save_object_error('Invalid package with GUID %s: %r' % (harvest_object.guid, e.error_dict),
                    harvest_object, 'Import')
        except Exception, e:
            self._save_object_error('%r'%e,harvest_object,'Import')

        package_dict['id'] = harvest_object.guid,
        package_dict['name'] = self._gen_new_name(packageName)
        
        return self._create_or_update_package(package_dict, harvest_object)  

