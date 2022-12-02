import logging
import mock
import pytest

from ckan.plugins import toolkit
from ckan.tests import factories, helpers

log = logging.getLogger(__name__)


@pytest.mark.usefixtures('clean_db', 'with_plugins')
class TestBlobStorageActivityDownload(object):
    def test_can_download_release_resource_whether_it_exists_in_current_version_of_package_or_not(self, app):

        user = factories.User()
        dataset = helpers.call_action('package_create', name='dataset_for_bug_test')
        resource = helpers.call_action('resource_create', package_id=dataset["id"])

        helpers.call_action('resource_delete', id=resource['id'])

        activity_list = helpers.call_action('package_activity_list', id=dataset['id'], include_hidden_activity=True)
        activity_before_deleted_resource = activity_list[1]

        with mock.patch('ckanext.blob_storage.blueprints.call_download_handlers', return_value=''):
            url = toolkit.url_for(
                'blob_storage.download',
                id=dataset['id'],
                resource_id=resource['id'],
                activity_id=activity_before_deleted_resource['id'],
                filename="test.csv",
                preview=1
            )
            # REMOTE_USER is needed to access the resource since resources from old activities are not public
            app.get(url, status=200, extra_environ={'REMOTE_USER': user['name']})




