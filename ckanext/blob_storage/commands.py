import click
import sys
import logging

@click.group(name='blob-storage', short_help='Blob storage commands')
def blob_storage():
    """Commands for managing blob storage."""
    pass

@blob_storage.command('migrate')
@click.option('--from-bucket', is_flag=True, help='Migrate resources from a bucket')
@click.argument('bucket_url', required=False)
def migrate(from_bucket, bucket_url):
    """Migrate resources to blob storage.
    
    If --from-bucket is specified, resources will be migrated from the specified bucket URL.
    Otherwise, resources will be migrated from the CKAN upload directory.
    """
    # Import here to avoid circular imports
    import os
    import errno
    import tempfile
    import time
    import shutil
    import logging
    from contextlib import contextmanager
    from typing import Any, Dict, Generator, Tuple
    
    import requests
    from ckan.lib.helpers import _get_auto_flask_context  # noqa  we need this for Flask request context
    from ckan.model import Resource, Session, User
    from ckan.plugins import toolkit
    from flask import Response
    from giftless_client import LfsClient
    from giftless_client.types import ObjectAttributes
    from six import binary_type, string_types
    from sqlalchemy.orm import load_only
    from sqlalchemy.orm.attributes import flag_modified
    from werkzeug.wsgi import FileWrapper
    
    from ckanext.blob_storage import helpers
    from ckanext.blob_storage.download_handler import call_download_handlers
    
    # Set up logging
    log = logging.getLogger(__name__)
    
    # Migrate functionality
    site_user = toolkit.get_action('get_site_user')({'ignore_auth': True}, {})
    user = User.get(site_user['name'])
    max_failures = 3
    retry_delay = 3
    
    @contextmanager
    def app_context():
        context = _get_auto_flask_context()
        try:
            context.push()
            yield context
        finally:
            context.pop()
            
    @contextmanager
    def db_transaction(session):
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        else:
            session.commit()
            
    def update_storage_props(resource, lfs_props):
        # type: (Resource, Dict[str, Any]) -> None
        """Update the resource with new storage properties
        """
        resource.extras['lfs_prefix'] = lfs_props['lfs_prefix']
        resource.extras['sha256'] = lfs_props['sha256']
        resource.size = lfs_props['size']
        flag_modified(resource, 'extras')
        
    def get_resource_dataset(resource_obj):
        # type: (Resource) -> Tuple[Dict[str, Any], Dict[str, Any]]
        """Fetch the CKAN dataset dictionary for a DB-fetched resource
        """
        context = {"ignore_auth": True, "use_cache": False}
        dataset = toolkit.get_action('package_show')(context, {"id": resource_obj.package_id})
        resource = [r for r in dataset['resources'] if r['id'] == resource_obj.id][0]

        return dataset, resource
        
    def _needs_migration(resource):
        # type: (Resource) -> bool
        """Check the attributes of a resource to see if it was migrated
        """
        if not (resource.extras.get('lfs_prefix') and resource.extras.get('sha256')):
            return True

        expected_prefix = '/'.join([helpers.storage_namespace(), resource.package_id])
        return resource.extras.get('lfs_prefix') != expected_prefix
        
    def get_unmigrated_resources():
        # type: () -> Generator[Resource, None, None]
        """Generator of un-migrated resource
        """
        session = Session()
        session.revisioning_disabled = True

        # Start from inspecting all uploaded, undeleted resources
        all_resources = session.query(Resource).filter(
            Resource.url_type == 'upload',
            Resource.state != 'deleted',
        ).order_by(
            Resource.created
        ).options(load_only("id", "extras", "package_id"))

        for resource in all_resources:
            if not _needs_migration(resource):
                log.debug("Skipping resource %s as it was already migrated", resource.id)
                continue

            with db_transaction(session):
                locked_resource = session.query(Resource).filter(Resource.id == resource.id).\
                    with_for_update(skip_locked=True).one_or_none()

                if locked_resource is None:
                    log.debug("Skipping resource %s as it is locked (being migrated?)", resource.id)
                    continue

                # let's double check as the resource might have been migrated by another process by now
                if not _needs_migration(locked_resource):
                    continue

                yield locked_resource
                
    def download_remote_resource(resource_url, file_name):
        """Download a remote resource and save it to a local file"""
        with requests.get(resource_url, stream=True) as source, open(file_name, 'wb') as dest:
            source.raise_for_status()
            log.debug("Resource downloading, HTTP status code is %d, Content-type is %s",
                     source.status_code,
                     source.headers.get('Content-type', 'unknown'))
            for chunk in source.iter_content(chunk_size=1024 * 16):
                dest.write(chunk)
        log.debug("Remote resource downloaded to %s", file_name)
        
    def _save_redirected_response_data(response, file_name):
        # type: (Response, str) -> None
        """Download the URL of a remote resource we got redirected to, and save it locally
        """
        resource_url = response.headers['Location']
        log.debug("Resource is at %s, downloading ...", resource_url)
        download_remote_resource(resource_url, file_name)
        
    def _save_downloaded_response_data(response, file_name):
        # type: (Response, str) -> None
        """Get an HTTP response object with open file containing a resource and save the data locally
        to a temporary file
        """
        with open(file_name, 'wb') as f:
            if isinstance(response.response, (string_types, binary_type)):
                log.debug("Response contains inline string data, saving to %s", file_name)
                f.write(response.response)
            elif isinstance(response.response, FileWrapper):
                log.debug("Response is a werkzeug.wsgi.FileWrapper, copying to %s", file_name)
                for chunk in response.response:
                    f.write(chunk)
            elif hasattr(response.response, 'read'):  # assume an open stream / file
                log.debug("Response contains an open file object, copying to %s", file_name)
                shutil.copyfileobj(response.response, f)
            else:
                raise ValueError("Don't know how to handle response type: {}".format(type(response.response)))
                
    @contextmanager
    def download_resource(resource, dataset):
        # type: (Dict[str, Any], Dict[str, Any]) -> str
        """Download the resource to a local file and provide the file name
        """
        resource_file = tempfile.mktemp(prefix='ckan-blob-migration-')
        try:
            response = call_download_handlers(resource, dataset)
            if response.status_code == 200:
                _save_downloaded_response_data(response, resource_file)
            elif response.status_code in {301, 302}:
                _save_redirected_response_data(response, resource_file)
            else:
                raise RuntimeError("Unexpected download response code: {}".format(response.status_code))
            yield resource_file
        finally:
            try:
                os.unlink(resource_file)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
                    
    def get_upload_authz_token(dataset_id):
        # type: (str) -> str
        """Get an authorization token to upload the file to LFS
        """
        authorize = toolkit.get_action('authz_authorize')
        if not authorize:
            raise RuntimeError("Cannot find authz_authorize; Is ckanext-authz-service installed?")

        context = {'ignore_auth': True, 'auth_user_obj': user}
        scope = helpers.resource_authz_scope(dataset_id, actions='write')
        authz_result = authorize(context, {"scopes": [scope]})

        if not authz_result or not authz_result.get('token', False):
            raise RuntimeError("Failed to get authorization token for LFS server")

        if len(authz_result['granted_scopes']) == 0:
            raise toolkit.NotAuthorized("You are not authorized to upload resources")

        return authz_result['token']
    
    def upload_resource(resource_file, dataset_id, lfs_namespace, filename):
        # type: (str, str, str, str) -> ObjectAttributes
        """Upload a resource file to new storage using LFS server
        """
        token = get_upload_authz_token(dataset_id)
        lfs_client = LfsClient(helpers.server_url(), token)
        with open(resource_file, 'rb') as f:
            props = lfs_client.upload(f, lfs_namespace, dataset_id, filename=filename)

        # Only return standard object attributes
        return {k: v for k, v in props.items() if k[0:2] != 'x-'}
        
    def migrate_resource(resource_obj):
        # type: (Resource) -> None
        dataset, resource_dict = get_resource_dataset(resource_obj)
        resource_name = helpers.resource_filename(resource_dict)

        with download_resource(resource_dict, dataset) as resource_file:
            log.debug("Starting to upload file: %s", resource_file)
            lfs_namespace = helpers.storage_namespace()
            props = upload_resource(resource_file, dataset['id'], lfs_namespace, resource_name)
            props['lfs_prefix'] = '{}/{}'.format(lfs_namespace, dataset['id'])
            props['sha256'] = props.pop('oid')
            log.debug("Upload complete; sha256=%s, size=%d", props['sha256'], props['size'])

        update_storage_props(resource_obj, props)
        
    def migrate_resource_from_bucket(resource_obj, bucket_base_url):
        """Migrar un recurso específico desde el bucket"""
        dataset, resource_dict = get_resource_dataset(resource_obj)
        resource_name = helpers.resource_filename(resource_dict)
        
        # Construir la URL completa del recurso en el bucket
        if bucket_base_url:
            # Adaptación para estructuras de bucket con formato /resources/{resource_id}/{filename}
            resource_url = f"{bucket_base_url.rstrip('/')}/resources/{resource_obj.id}/{resource_dict['url']}"
        else:
            # Usar la URL tal como está (asumiendo que ya es una URL completa)
            resource_url = resource_dict['url']
            
        log.debug("Resource URL in bucket: %s", resource_url)
        
        # Descargar el archivo del bucket a un archivo temporal
        resource_file = tempfile.mktemp(prefix='ckan-blob-migration-')
        try:
            log.debug("Downloading resource from bucket to %s", resource_file)
            download_remote_resource(resource_url, resource_file)
            
            # Subir el archivo al almacenamiento LFS
            log.debug("Starting to upload file: %s", resource_file)
            lfs_namespace = helpers.storage_namespace()
            props = upload_resource(resource_file, dataset['id'], lfs_namespace, resource_name)
            props['lfs_prefix'] = '{}/{}'.format(lfs_namespace, dataset['id'])
            props['sha256'] = props.pop('oid')
            log.debug("Upload complete; sha256=%s, size=%d", props['sha256'], props['size'])
            
            # Actualizar los metadatos del recurso
            update_storage_props(resource_obj, props)
        finally:
            try:
                os.unlink(resource_file)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
                    
    def migrate_from_bucket(bucket_base_url=None):
        """Migrar recursos que están en un bucket externo"""
        migrated = 0
        for resource_obj in get_unmigrated_resources():
            log.info("Starting to migrate resource from bucket %s [%s]", resource_obj.id, resource_obj.name)
            failed = 0
            while failed < max_failures:
                try:
                    migrate_resource_from_bucket(resource_obj, bucket_base_url)
                    log.info("Finished migrating resource %s from bucket", resource_obj.id)
                    migrated += 1
                    break
                except Exception:
                    log.exception("Failed to migrate resource %s from bucket, retrying...", resource_obj.id)
                    failed += 1
                    time.sleep(retry_delay)
            else:
                log.error("Skipping bucket resource %s [%s] after %d failures", 
                         resource_obj.id, resource_obj.name, failed)
                
        log.info("Finished migrating %d resources from bucket", migrated)
        
    def migrate_all_resources():
        """Do the actual migration
        """
        migrated = 0
        for resource_obj in get_unmigrated_resources():
            log.info("Starting to migrate resource %s [%s]", resource_obj.id, resource_obj.name)
            failed = 0
            while failed < max_failures:
                try:
                    migrate_resource(resource_obj)
                    log.info("Finished migrating resource %s", resource_obj.id)
                    migrated += 1
                    break
                except Exception:
                    log.exception("Failed to migrate resource %s, retrying...", resource_obj.id)
                    failed += 1
                    time.sleep(retry_delay)
            else:
                log.error("Skipping resource %s [%s] after %d failures", resource_obj.id, resource_obj.name, failed)

        log.info("Finished migrating %d resources", migrated)
    
    # Main execution
    with app_context() as context:
        context.g.user = site_user['name']
        context.g.userobj = user
        
        if from_bucket:
            migrate_from_bucket(bucket_url)
        else:
            migrate_all_resources()

def get_commands():
    return [blob_storage] 