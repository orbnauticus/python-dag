#!/usr/bin/env python

"""
A directed acyclic graph stored in a sqlite table
"""

from collections.abc import MutableSet
import os
import re
import sqlite3


import topsort


class SqliteDAG(MutableSet):
    def __init__(self, connection, table, source_field, sink_field):
        self.connection = connection
        self.table = table
        self.source_field = source_field
        self.sink_field = sink_field
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS {table}(
            {source} NOT NULL, {sink} NOT NULL,
            UNIQUE ({source}, {sink}) ON CONFLICT REPLACE);
            """)

    def execute(self, statement, values=()):
        statement = statement.format(**self.properties())
        #print(re.sub('\s+', ' ', statement).strip(), values or '')
        return self.connection.execute(statement, values)

    def properties(self):
        return dict(
            table='`{}`'.format(self.table.replace('`', '``')),
            source='`{}`'.format(self.source_field.replace('`', '``')),
            sink='`{}`'.format(self.sink_field.replace('`', '``')),
        )

    @classmethod
    def connect(cls, path, table, source_field, sink_field):
        return cls(sqlite3.connect(path), table, source_field, sink_field)

    def add(self, edge):
        self.execute(
            """
            INSERT INTO {table}({source}, {sink}) VALUES (?, ?);
            """, edge)

    def discard(self, edge):
        self.execute(
            """
            DELETE FROM {table} WHERE {source}=? AND {sink}=?;
            """, edge)

    def __contains__(self, edge):
        rows = self.execute(
            """
            SELECT 1 FROM {table} WHERE {source}=? AND {sink}=?;
            """, edge)

    def __iter__(self):
        return iter(self.execute(
            """
            SELECT {source},{sink} FROM {table};
            """))

    def __len__(self):
        return self.execute(
            """
            SELECT sum(1) FROM {table};
            """).fetchone()[0]

    def nodes(self):
        return (row[0] for row in self.execute(
            """
            SELECT DISTINCT * FROM
            (SELECT {source} FROM {table}
            UNION SELECT {sink} FROM {table});
            """))

    def topsort(self):
        # Copy contents into a working table we can destroy
        self.execute(
            """
            CREATE TEMPORARY TABLE WORKING_DAG(
            {source}, {sink}, leaf INT DEFAULT 0);
            """)
        self.execute(
            """
            INSERT INTO WORKING_DAG({source}, {sink})
            SELECT {source}, {sink} FROM {table};
            """)
        # Create fake edges so that nodes without outgoing edges are reported
        #  in the final pass
        self.execute(
            """
            INSERT INTO WORKING_DAG({source}, leaf)
            SELECT DISTINCT {sink}, 1 FROM DAG;
            """)
        while True:
            nodes_with_no_incoming_edges = set(row[0] for row in self.execute(
                """
                SELECT DISTINCT {source} FROM WORKING_DAG
                WHERE {source} NOT IN (
                    SELECT {sink} FROM WORKING_DAG WHERE leaf = 0);
                """).fetchall())
            if not nodes_with_no_incoming_edges:
                break
            yield nodes_with_no_incoming_edges
            self.execute(
                """
                DELETE FROM WORKING_DAG WHERE {source} NOT IN (
                SELECT {sink} FROM WORKING_DAG WHERE leaf = 0);
                """)
        cycle = self.execute(
            """
            SELECT {source}, {sink} FROM WORKING_DAG WHERE leaf = 0;
            """).fetchall()
        self.execute(
            """
            DROP TABLE WORKING_DAG;
            """)
        if cycle:
            raise topsort.CycleError(cycle)
