"""
sentry.search.base
~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010-2014 by the Sentry Team, see AUTHORS for more details.
:license: BSD, see LICENSE for more details.
"""

from __future__ import absolute_import


class SearchBackend(object):
    def __init__(self, **options):
        pass

    def index(self, group, event):
        pass

    def query(self, project, query=None, status=None, tags=None,
              bookmarked_by=None, sort_by='date'):
        raise NotImplementedError

    def upgrade(self):
        pass
