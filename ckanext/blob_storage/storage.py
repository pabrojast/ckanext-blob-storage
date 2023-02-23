#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cgi
from datetime import datetime

from ckan import model
from ckan.lib import munge
from .download_handler import download_handler


class ResourceBlobStorage(object):
    def __init__(self, resource):
        super(ResourceBlobStorage, self).__init__()

    def get_path(self, id):
        return download_handler(id)

    def upload(self, id, max_size):
        return None
