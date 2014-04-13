# -*- coding: utf-8 -*-

from __future__ import absolute_import

from sentry.search.elastic_search.backend import ElasticSearchBackend
from sentry.testutils import TestCase


class ElasticSearchTest(TestCase):
    def setUp(self):
        from elasticsearch import Elasticsearch

        self.conn = Elasticsearch()
        try:
            self.conn.indices.delete(index='test-sentry-1')
        except Exception:
            pass

        self.backend = ElasticSearchBackend(index_prefix='test-')
        self.backend.upgrade()

    def test_simple(self):
        project = self.project
        group1 = self.create_group(
            project=project,
            checksum='a' * 40,
            message='foo',
        )
        event1 = self.create_event(
            event_id='a' * 40,
            group=group1,
        )
        group2 = self.create_group(
            project=project,
            checksum='b' * 40,
            message='bar',
        )
        event2 = self.create_event(
            event_id='b' * 40,
            group=group2,
        )

        self.backend.index(group1, event1)
        self.backend.index(group2, event2)

        self.conn.indices.refresh(index='test-sentry-1')

        results = self.backend.search(project, query='foo')
        assert len(results) == 1
        assert results[0] == group1.id

        results = self.backend.search(project, query='bar')
        assert len(results) == 1
        assert results[0] == group2.id

        project2 = self.create_project(self.team)

        results = self.backend.search(project2, query='bar')
        assert len(results) == 0
