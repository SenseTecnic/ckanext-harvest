import urllib2

from ckan.lib.base import c
from ckan.plugins import PluginImplementations
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action
from ckan.lib.helpers import json

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError

import requests
from hashlib import sha1
import datetime
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

    def _getContext(self):
        # Check API version
        if self.config:
            api_version = self.config.get('api_version','2')
            #TODO: use site user when available
            user_name = self.config.get('user',u'harvest')
        else:
            api_version = '2'
            user_name = u'harvest'

        context = {
            'model': model,
            'session': Session,
            'user': user_name,
            'api_version': api_version
        }
        return context

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
                    context = self._getContext()
                    data_dict = {"module": sensor_module["module"]}
                    get_action("wotkit_get_sensor_module_import")(context, data_dict)
                except Exception as e:
                    raise Exception("Failed to import wotkit sensor harvester module for: " + sensor_module["module"] + 
                                        ". Make sure module is defined in ckanext-harvest/ckanext/harvest/harvesters/sensors/")
        return config

    def gather_stage(self,harvest_job):
        PluginImplementations
        log.debug('In WotkitHarvester gather_stage (%s)' % harvest_job.source.url)
        get_all_packages = True
        package_ids = []

        self._set_config(harvest_job.source.config)

        sensors = self.config["gather_sensors"]
        ids = []
        for sensor in sensors:
            try:
                id = sha1(sensor["module"]).hexdigest()
                obj = HarvestObject(guid=id, job=harvest_job, content=sensor["module"])
                obj.save()
                ids.append(obj.id)
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
        
        # Extract packagename from config supplied from web browser
        packageName = None
        for sensor_module in self.config['gather_sensors']:
            if sensor_module['module'] == moduleName:
                packageName = sensor_module['package_name']
                
        try:
            # Hacky solution that calls as action that imports modules in the wotkit extension
            context = self._getContext()
            data_dict = {"module": moduleName}
            
            # Call action defined in wotkit extension
            package_dict = get_action("wotkit_harvest_module")(context, data_dict)
            package_dict['id'] = harvest_object.guid,
            package_dict['name'] = self._gen_new_name(packageName)
            package_dict['notes'] = "Harvested with wotkit_harvester. This dataset is queried every 15 minutes and populated in the Wotkit."

            package_dict['extras'] = {'last-update': str(datetime.datetime.now())}
            package_dict['groups'] = {'id': 'wotkit-datasets'}
            
            return self._create_or_update_package(package_dict, harvest_object)  
        except ValidationError,e:
            self._save_object_error('Invalid package with GUID %s: %r' % (harvest_object.guid, e.error_dict),
                    harvest_object, 'Import')
        except Exception, e:
            self._save_object_error('%r'%e,harvest_object,'Import')

        return False
        
        

