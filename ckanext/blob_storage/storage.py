from ckan.lib.uploader import ResourceUpload


class ResourceBlobStorage:
    def __init__(self, resource):
        self.delegate = ResourceUpload(resource)

    def get_path(self, id):
        return self.delegate.get_path(id)

    def upload(self, id, max_size):
        return self.delegate.upload(id, max_size)
