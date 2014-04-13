"""
sentry.search.django.backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010-2014 by the Sentry Team, see AUTHORS for more details.
:license: BSD, see LICENSE for more details.
"""

from __future__ import absolute_import

from sentry.search.base import SearchBackend


class DjangoSearchBackend(SearchBackend):
    def search(self, project, query=None, status=None, tags=None,
               bookmarked_by=None, sort_by='date'):
        from sentry.models import Group

        queryset = Group.objects.filter(project=project)
        if query:
            queryset = queryset.filter(message__icontains=query)

        if status is not None:
            queryset = queryset.filter(status=status)

        if bookmarked_by:
            queryset = queryset.filter(
                bookmark_set__project=project,
                bookmark_set__user=bookmarked_by,
            )

        if tags:
            for k, v in tags.iteritems():
                queryset = queryset.filter(**dict(
                    grouptag__key=k,
                    grouptag__value=v,
                ))

        queryset = queryset.order_by('-last_seen')
        return queryset.values_list('id', flat=True)
