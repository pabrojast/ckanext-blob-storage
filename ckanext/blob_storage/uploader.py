class DummyUploader(object):
    # this class implements a dummy IUploader interface which allows extensions like
    # ckanext-validation to detect that there's a non-standard storage plugin used
    # in CKAN
    def __init__(self, resource):
        return None

    def get_path(self, id):
        return None

    def upload(self, id, max_size):
        return None
