Graphite KairosDB Finder
================

A plugin for using graphite with the cassandra-based Kairosdb storage
backend.

Requires  Graphite-web 0.10.X.

Graphite-web 0.10.X is currently unreleased. You'll need to install from
source.

.. _documentation: http://graphite-api.readthedocs.org/en/latest/

Using with graphite-web
-----------------------

In your graphite's ``local_settings.py``::

    STORAGE_FINDERS = (
        ‘kairosdb.KairosdbFinder',
    )

    KAIROSDB_URL = 'http://host:port'

Where ``host:port`` is the location of the Kiaorsdb HTTP API, 

e.g. http://localhost:8080/api/v1