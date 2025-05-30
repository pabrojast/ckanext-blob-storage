"""Template helpers for ckanext-blob-storage
"""
from os import path
from typing import Any, Dict, Optional

import ckan.plugins.toolkit as toolkit
from six.moves.urllib.parse import urlparse


from ckanext.authz_service.authz_binding.common import OptionalCkanContext, get_user_context

SERVER_URL_CONF_KEY = 'ckanext.blob_storage.storage_service_url'
STORAGE_NAMESPACE_CONF_KEY = 'ckanext.blob_storage.storage_namespace'


def resource_storage_prefix(package_name, org_name=None):
    # type: (str, Optional[str]) -> str
    """Get the resource storage prefix for a package name
    """
    if org_name is None:
        org_name = storage_namespace()
    return '{}/{}'.format(org_name, package_name)


def resource_authz_scope(package_name, actions=None, org_name=None, resource_id=None, activity_id=None):
    # type: (str, Optional[str], Optional[str], Optional[str], Optional[str]) -> str
    """Get the authorization scope for package resources
    """
    if actions is None:
        actions = 'read,write'
    if resource_id is None:
        resource_id = '*'
    scope = 'obj:{}/{}:{}'.format(
        resource_storage_prefix(package_name, org_name),
        _resource_version(resource_id, activity_id),
        actions
    )
    return scope


def _resource_version(resource_id, activity_id):
    result = resource_id
    if activity_id:
        result += "/{}".format(activity_id)
    return result


def server_url():
    # type: () -> Optional[str]
    """Get the configured server URL
    """
    url = toolkit.config.get(SERVER_URL_CONF_KEY)
    if not url:
        raise ValueError("Configuration option '{}' is not set".format(
            SERVER_URL_CONF_KEY))
    if url[-1] == '/':
        url = url[0:-1]
    return url


def storage_namespace():
    """Get the storage namespace for this CKAN instance
    """
    ns = toolkit.config.get(STORAGE_NAMESPACE_CONF_KEY)
    if ns:
        return ns
    return 'ckan'


def organization_name_for_package(package):
    # type: (Dict[str, Any]) -> Optional[str]
    """Get the organization name for a known, fetched package dict
    """
    context = {'ignore_auth': True}
    org = package.get('organization')
    if not org and package.get('owner_org'):
        org = toolkit.get_action('organization_show')(context, {'id': package['owner_org']})
    if org:
        return org.get('name')
    return None


def resource_filename(resource):
    """Get original file name from resource
    """
    if 'url' not in resource:
        return resource['name']

    if resource['url'][0:6] in {'http:/', 'https:'}:
        url_path = urlparse(resource['url']).path
        return path.basename(url_path)
    return resource['url']


def check_resource_in_dataset(resource_id, dataset_id, context=None):
    # type: (str, str, OptionalCkanContext) -> bool
    """Check that a resource exists in the dataset
    """
    if context is None:
        context = get_user_context()
    try:
        ds = toolkit.get_action('package_show')(context, {"id": dataset_id})
        for resource in ds['resources']:
            if resource['id'] == resource_id:
                return True
    except (toolkit.ObjectNotFound, toolkit.NotAuthorized):
        pass

    return False


def find_activity_resource(context, activity_id, resource_id, dataset_id):
    if not (activity_id and toolkit.check_ckan_version(min_version='2.9')):
        return None

    try:
        activity = toolkit.get_action(u'activity_show')(
            context, {u'id': activity_id, u'include_data': True})
        activity_dataset = activity['data']['package']

        assert (activity_dataset['name'] == dataset_id) or (activity_dataset['id'] == dataset_id)

        activity_resources = activity_dataset['resources']
        for r in activity_resources:
            if r['id'] == resource_id:
                resource = r
                return resource
    except AssertionError or toolkit.NotFound:
        pass

    return None


def find_activity_package(context, activity_id, resource_id, dataset_id):
    if not (activity_id and toolkit.check_ckan_version(min_version='2.9')):
        return None

    try:
        activity = toolkit.get_action(u'activity_show')(
            context, {u'id': activity_id, u'include_data': True})
        activity_dataset = activity['data']['package']

        assert (activity_dataset['name'] == dataset_id) or (activity_dataset['id'] == dataset_id)

        activity_resources = activity_dataset['resources']
        for r in activity_resources:
            if r['id'] == resource_id:
                package = activity_dataset
                return package
    except AssertionError or toolkit.NotFound:
        pass

    return None



