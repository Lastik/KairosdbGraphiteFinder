

from metricType import MetricType

mt = MetricType()
mt.openConnection()
mt.getAllMetricTypeRecords()
mt.deleteMetricTypeDocuments(['a.b.c.d', 'a.b.c', 'a.b', 'a'])
mt.getAllMetricTypeRecords()
mt.getByName("a.b.c1.d1")
mt.getAllMetricTypeRecords()
mt.getByName("a.b.c1.d2")
mt.getAllMetricTypeRecords()
mt.getByName("a.b.c2.d3")
mt.getAllMetricTypeRecords()


