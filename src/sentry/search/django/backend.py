"""
sentry.search.django.backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010-2014 by the Sentry Team, see AUTHORS for more details.
:license: BSD, see LICENSE for more details.
"""

from __future__ import absolute_import

from sentry.search.base import SearchBackend, SearchResult
from sentry.utils.db import get_db_engine


SORT_CLAUSES = {
    'priority': 'sentry_groupedmessage.score',
    'date': 'EXTRACT(EPOCH FROM sentry_groupedmessage.last_seen)',
    'new': 'EXTRACT(EPOCH FROM sentry_groupedmessage.first_seen)',
    'freq': 'sentry_groupedmessage.times_seen',
    'tottime': 'sentry_groupedmessage.time_spent_total',
    'avgtime': '(sentry_groupedmessage.time_spent_total / sentry_groupedmessage.time_spent_count)',
}
SCORE_CLAUSES = SORT_CLAUSES.copy()

SQLITE_SORT_CLAUSES = SORT_CLAUSES.copy()
SQLITE_SORT_CLAUSES.update({
    'date': "(julianday(sentry_groupedmessage.last_seen) - 2440587.5) * 86400.0",
    'new': "(julianday(sentry_groupedmessage.first_seen) - 2440587.5) * 86400.0",
})
SQLITE_SCORE_CLAUSES = SQLITE_SORT_CLAUSES.copy()

MYSQL_SORT_CLAUSES = SORT_CLAUSES.copy()
MYSQL_SORT_CLAUSES.update({
    'date': 'UNIX_TIMESTAMP(sentry_groupedmessage.last_seen)',
    'new': 'UNIX_TIMESTAMP(sentry_groupedmessage.first_seen)',
})
MYSQL_SCORE_CLAUSES = MYSQL_SORT_CLAUSES.copy()

ORACLE_SORT_CLAUSES = SCORE_CLAUSES.copy()
ORACLE_SORT_CLAUSES.update({
    'date': "(cast(sentry_groupedmessage.last_seen as date)-TO_DATE('01/01/1970 00:00:00', 'MM-DD-YYYY HH24:MI:SS')) * 24 * 60 * 60",
    'new': "(cast(sentry_groupedmessage.first_seen as date)-TO_DATE('01/01/1970 00:00:00', 'MM-DD-YYYY HH24:MI:SS')) * 24 * 60 * 60",
})
ORACLE_SCORE_CLAUSES = ORACLE_SORT_CLAUSES.copy()

MSSQL_SORT_CLAUSES = SCORE_CLAUSES.copy()
MSSQL_SORT_CLAUSES.update({
    'date': "DATEDIFF(s, '1970-01-01T00:00:00', sentry_groupedmessage.last_seen)",
    'new': "DATEDIFF(s, '1970-01-01T00:00:00', sentry_groupedmessage.first_seen)",
})
MSSQL_SCORE_CLAUSES = MSSQL_SORT_CLAUSES.copy()


class DjangoSearchBackend(SearchBackend):
    def index(self, event):
        pass

    def query(self, project, query=None, status=None, tags=None,
              bookmarked_by=None, sort_by='date', date_filter='last_seen',
              date_from=None, date_to=None, cursor=None):
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

        if date_filter == 'first_seen':
            if date_from:
                queryset = queryset.filter(first_seen__gte=date_from)
            elif date_to:
                queryset = queryset.filter(first_seen__lte=date_to)
        elif date_filter == 'last_seen':
            if date_from and date_to:
                queryset = queryset.filter(
                    groupcountbyminute__date__gte=date_from,
                    groupcountbyminute__date__lte=date_to,
                )
            elif date_from:
                queryset = queryset.filter(last_seen__gte=date_from)
            elif date_to:
                queryset = queryset.filter(last_seen__lte=date_to)

        engine = get_db_engine('default')
        if engine.startswith('sqlite'):
            score_clause = SQLITE_SORT_CLAUSES.get(sort_by)
            filter_clause = SQLITE_SCORE_CLAUSES.get(sort_by)
        elif engine.startswith('mysql'):
            score_clause = MYSQL_SORT_CLAUSES.get(sort_by)
            filter_clause = MYSQL_SCORE_CLAUSES.get(sort_by)
        elif engine.startswith('oracle'):
            score_clause = ORACLE_SORT_CLAUSES.get(sort_by)
            filter_clause = ORACLE_SCORE_CLAUSES.get(sort_by)
        elif engine in ('django_pytds', 'sqlserver_ado', 'sql_server.pyodbc'):
            score_clause = MSSQL_SORT_CLAUSES.get(sort_by)
            filter_clause = MSSQL_SCORE_CLAUSES.get(sort_by)
        else:
            score_clause = SORT_CLAUSES.get(sort_by)
            filter_clause = SCORE_CLAUSES.get(sort_by)

        if sort_by == 'tottime':
            queryset = queryset.filter(time_spent_count__gt=0)
        elif sort_by == 'avgtime':
            queryset = queryset.filter(time_spent_count__gt=0)

        if score_clause:
            queryset = queryset.extra(
                select={'sort_value': score_clause},
            )
            # HACK: don't sort by the same column twice
            if sort_by == 'date':
                queryset = queryset.order_by('-last_seen')
            else:
                queryset = queryset.order_by('-sort_value', '-last_seen')

            if cursor:
                queryset = queryset.extra(
                    where=['%s > %%s' % filter_clause],
                    params=[float(cursor)],
                )

        return SearchResult(list(queryset.values_list('id', flat=True)))
