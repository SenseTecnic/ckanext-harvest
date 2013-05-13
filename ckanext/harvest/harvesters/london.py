#coding: utf-8
import urllib2
import string
from datetime import datetime
from csv import DictReader
import logging
from hashlib import sha1

from ckan.lib.helpers import json

from ckanext.harvest.model import HarvestObject
from base import HarvesterBase



log = logging.getLogger(__name__)

class DataLondonGovUkHarvester(HarvesterBase):
    CATALOGUE_URL = "http://data.london.gov.uk"
    CATALOGUE_CSV_URL = "http://data.london.gov.uk/datafiles/datastore-catalogue.csv"

    config = None
    
    def _set_config(self,config_str):
        if config_str:
            self.config = json.loads(config_str)

            if 'api_version' in self.config:
                self.api_version = self.config['api_version']

            log.debug('Using config: %r', self.config)
        else:
            self.config = {}
            
    def info(self):
        return {
            'name': 'data_london_gov_uk',
            'title': 'data.london.gov.uk',
            'description': 'CSV Import from GLA Datastore'
        }

    def skip_filter(self, row):
        if self.config and 'whitelist_filter' in self.config:
            for searchTerm in self.config['whitelist_filter']:
                if searchTerm in row.get('TITLE') or searchTerm in row.get('CATEGORIES'):
                    log.debug("Hit in whitelist filter, will get this source: " + row.get('TITLE'))
                    return False
            return True
        else:
            return False

    def gather_stage(self, harvest_job):
        log.debug('In DataLondonGovUk gather_stage')

        self._set_config(harvest_job.source.config)

        csvfh = urllib2.urlopen(self.CATALOGUE_CSV_URL)
        csv = DictReader(csvfh)
        ids = []
        for row in csv:
            if self.skip_filter(row):
                continue

                
                
                        
                
            id = sha1('%s/%s' % (self.CATALOGUE_URL,row.get('DRUPAL_NODE'))).hexdigest()
            row = dict([(k, v.decode('latin-1')) for k, v in row.items()])
            obj = HarvestObject(guid=id, job=harvest_job,
                    content=json.dumps(row))
            obj.save()
            ids.append(obj.id)
        return ids

    def fetch_stage(self, harvest_object):
        return True

    def import_stage(self,harvest_object):
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id,harvest_object,'Import')
            return False

        try:
            row = json.loads(harvest_object.content)
            def csplit(txt):
                return [t.strip() for t in txt.split(",")]

            package_dict = {
                    'title': row['TITLE'],
                    'url': row['URL'],
                    'notes': row['LONGDESC'],
                    'author': row['AUTHOR_NAME'],
                    'maintainer': row['MAINTAINER'],
                    'maintainer_email': row['MAINTAINER_EMAIL'],
                    'tags': csplit(row['TAGS']),
                    'license_id': 'ukcrown',
                    'extras': {
                        'date_released': row['RELEASE_DATE'],
                        'categories': csplit(row['CATEGORIES']),
                        'geographical_granularity': row['GEOGRAPHY'],
                        'geographical_coverage': row['EXTENT'],
                        'temporal_granularity': row['UPDATE_FREQUENCY'],
                        'temporal_coverage': row['DATE_RANGE'],
                        'license_summary': row['LICENSE_SUMMARY'],
                        'license_details': row['license_details'],
                        'spatial_reference_system': row['spatial_ref'],
                        'harvest_dataset_url': row['DATASTORE_URL'],
                        # Common extras
                        'harvest_catalogue_name': 'London Datastore',
                        'harvest_catalogue_url': 'http://data.london.gov.uk',
                        'eu_country': 'UK',
                        'eu_nuts1': 'UKI'

                    },
                    'resources': []
                }

            def pkg_format(prefix, mime_type):
                if row.get(prefix + "_URL"):
                    package_dict['resources'].append({
                        'url': row.get(prefix + "_URL"),
                        'format': mime_type,
                        'description': "%s version" % prefix.lower()
                        })

            pkg_format('EXCEL', 'application/vnd.ms-excel')
            pkg_format('CSV', 'text/csv')
            pkg_format('TAB', 'text/tsv')
            pkg_format('XML', 'text/xml')
            pkg_format('GOOGLEDOCS', 'api/vnd.google-spreadsheet')
            pkg_format('JSON', 'application/json')
            pkg_format('SHP', 'application/octet-stream+esri')
            pkg_format('KML', 'application/vnd.google-earth.kml+xml')
        except Exception, e:
            log.exception(e)
            self._save_object_error('%r' % e, harvest_object, 'Import')

        package_dict['id'] = harvest_object.guid
        package_dict['name'] = self._gen_new_name(package_dict['title'])
        return self._create_or_update_package(package_dict, harvest_object)



