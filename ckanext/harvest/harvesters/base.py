import logging


from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound
from ckan.logic.action.create import package_create_rest
from ckan.logic.action.update import package_update_rest
from ckan.logic.action.get import package_show
from ckan.logic.schema import default_package_schema
from ckan.lib.navl.validators import ignore_missing,ignore
from ckan.lib.munge import munge_title_to_name, munge_tag

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError

from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester

log = logging.getLogger(__name__)

class HarvesterBase(SingletonPlugin):
    '''
    Generic class for publicdata.eu harvesters
    '''
    implements(IHarvester)

    def _gen_new_name(self,title):
        name = munge_title_to_name(title).replace('_', '-')
        while '--' in name:
            name = name.replace('--', '-')
        return name

    def _check_name(self,name):
        like_q = u'%s%%' % name
        pkg_query = Session.query(Package).filter(Package.name.ilike(like_q)).limit(100)
        taken = [pkg.name for pkg in pkg_query]
        if name not in taken:
            return name
        else:
            counter = 1
            while counter < 101:
                if name+str(counter) not in taken:
                    return name+str(counter)
                counter = counter + 1
            return None

    def _save_gather_error(self,message,job):
        err = HarvestGatherError(message=message,job=job)
        err.save()
        log.error(message)

    def _save_object_error(self,message,obj,stage=u'Fetch'):
        err = HarvestObjectError(message=message,object=obj,stage=stage)
        err.save()
        log.error(message)

    def _create_harvest_objects(self, remote_ids, harvest_job):
        try:
            object_ids = []
            if len(remote_ids):
                for remote_id in remote_ids:
                    # Create a new HarvestObject for this identifier
                    obj = HarvestObject(guid = remote_id, job = harvest_job)
                    obj.save()
                    object_ids.append(obj.id)
                return object_ids
            else:
               self._save_gather_error('No remote datasets could be identified', harvest_job)
        except Exception, e:
            self._save_gather_error('%r' % e.message, harvest_job)

    def _create_or_update_package(self, package_dict, harvest_object):
        '''
        Creates a new package or updates an exisiting one according to the
        package dictionary provided. The package dictionary should look like
        the REST API response for a package:

        http://ckan.net/api/rest/package/statistics-catalunya

        Note that the package_dict must contain an id, which will be used to
        check if the package needs to be created or updated (use the remote
        dataset id).

        If the remote server provides the modification date of the remote
        package, add it to package_dict['metadata_modified'].

        '''
        try:
            #from pprint import pprint 
            #pprint(package_dict)
            ## change default schema
            schema = default_package_schema()
            schema['id'] = [ignore_missing, unicode]
            schema['__junk'] = [ignore]

            context = {
                'model': model,
                'session': Session,
                'user': u'harvest',
                'api_version':'2',
                'schema': schema,
            }

            tags = package_dict.get('tags', [])
            tags = [munge_tag(t) for t in tags]
            tags = list(set(tags))
            package_dict['tags'] = tags

            # Check if package exists
            context.update({'id':package_dict['id']})
            try:
                existing_package_dict = package_show(context)
                # Check modified date
                if not 'metadata_modified' in package_dict or \
                   package_dict['metadata_modified'] > existing_package_dict.get('metadata_modified'):
                    log.info('Package with GUID %s exists and needs to be updated' % harvest_object.guid)
                    # Update package
                    updated_package = package_update_rest(package_dict, context)

                    harvest_object.package_id = updated_package['id']
                    harvest_object.save()
                else:
                    log.info('Package with GUID %s not updated, skipping...' % harvest_object.guid)

            except NotFound:
                # Package needs to be created
                del context['id']

                # Check if name has not already been used
                package_dict['name'] = self._check_name(package_dict['name'])

                log.info('Package with GUID %s does not exist, let\'s create it' % harvest_object.guid)
                new_package = package_create_rest(package_dict, context)
                harvest_object.package_id = new_package['id']
                harvest_object.save()

            return True

        except ValidationError,e:
            log.exception(e)
            self._save_object_error('Invalid package with GUID %s: %r'%(harvest_object.guid,e.error_dict),harvest_object,'Import')
        except Exception, e:
            log.exception(e)
            self._save_object_error('%r'%e,harvest_object,'Import')

        return None






