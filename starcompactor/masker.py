from __future__ import absolute_import, division, print_function, unicode_literals
import os
import hashlib


class Masker:
    def __init__(self, method='sha256', truncate=None):
        self.salt = os.urandom(32)
        self.method = method
        self.truncate = truncate

    def __call__(self, data):
        h = hashlib.new(self.method, self.salt)
        h.update(data.encode('utf-8'))
        return h.hexdigest()[:self.truncate]
