
#import os
import traceback
import re
from pprint import pformat
from django.conf import settings
from django.core.cache import cache

from graphite.logger import log
from graphite.node import BranchNode, LeafNode
from graphite.util import find_escaped_pattern_fields

from graphite.finders.metricType import MetricType

from cassandra.cluster import Cluster
from cassandra.query import dict_factory

###############################################################################
###### from reader:
#import sys
#import re
import requests
import time
import math
import json
import multiprocessing
from multiprocessing.pool import ThreadPool

from graphite.intervals import Interval, IntervalSet
from graphite.readers import FetchInProgress
from graphite import settings 

KAIROSDB_MAX_REQUESTS = 10
KAIROSDB_REQUEST_POOL = ThreadPool(KAIROSDB_MAX_REQUESTS)
KAIROSDB_URL = settings.KAIROSDB_URL

###############################################################################

class TimeoutError(Exception):
    pass

###############################################################################

def call_with_timeout(func, args, kwargs, timeout):
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    # define a wrapper of `return_dict` to store the result.
    def function(return_dict):
        return_dict['value'] = func(*args, **kwargs)
    p = multiprocessing.Process(target=function, args=(return_dict,))
    p.start()
    # Force a max. `timeout` or wait for the process to finish
    p.join(timeout)
    # If thread is still active, it didn't finish: raise TimeoutError
    if p.is_alive():
        p.terminate()
        p.join()
        raise TimeoutError
    else:
        return return_dict['value']

###############################################################################
# call_with_timeout(requests.get, args=(url,), kwargs={'timeout': 10}, timeout=60)
###############################################################################

class KairosdbUtils(object):
    def kairosdb_time_to_graphite_time(self, timeval):
        return timeval / 1000

    def graphite_time_to_kairosdb_time(self, timeval):
        return timeval * 1000

    def get_kairosdb_url(self, kairosdb_uri, url):
        full_url = "%s/%s" % (kairosdb_uri, url)
        log.info("kairosdb.KairosdbUtils.get_kairosdb_url(): url: %s" % (url))
        tstart = time.time()
        result = requests.get(full_url, timeout=5)
        delay = time.time() - tstart
        log.info("kairosdb.KairosdbUtils.get_kairosdb_url(): full url: %s, delay: %5.8f, result: %s" % (full_url, delay, result))
        return result.json()

    def post_kairosdb_url(self, kairosdb_uri, url, data):
        full_url = "%s/%s" % (kairosdb_uri, url)
        log.info("kairosdb.KairosdbUtils.post_kairosdb_url(): url: %s, data: %s" % (url, pformat(data)))
        #return requests.post(full_url, data, timeout=0.5).json()
        #resp = requests.post(full_url, data, timeout=0.5).json()
        tstart = time.time()
        ret = None
        try:
            #ret = call_with_timeout(requests.post, args=(full_url,data), kwargs={'timeout': 2}, timeout=5)
            ret = requests.post(full_url, data, timeout=15.0)
        except Exception as e:
            tb = traceback.format_exc()
            log.info("kairosdb.KairosdbUtils.post_kairosdb_url(): EXCEPTION: %s, tb: %s, url: %s, data: %s" % (e, tb, full_url, pformat(data)))
        delay = time.time() - tstart
        name = "NameNotFoundStringIsOdd: %s" % (str(data))
        try:
            # data is already a json-string, search string for name.
            match = re.search(".*name(.*)tags.*", data) 
            name = match and match.group() and match.group(1) or "NoMatch"
        except Exception as e:
            pass
        log.info("KairosDBcallDelay: %5.8f, name: %s" % (delay, name))
        return ret.json()

###############################################################################


class KairosDBFinder(object):
    def __init__(self):
        self.mt = MetricType()
        self.mt.openConnection()
        #self.mt = GLOBAL_METRIC_TYPE

    def getLeafNode(self, mname, reader, avoidIntervals=False):
        ln = LeafNode(mname, reader)  #, avoidIntervals=avoidIntervals)
        return ln

    def getBranchNode(self, mname):
        bn = BranchNode(mname)
        return bn

    def find_nodes(self, query):
        timeStart = time.time()
        
        cacheKey = "find_node_qpList:%s" % query.pattern
        tupes = cache.get(cacheKey)
        if not tupes:
            tupes = self.mt.find_nodes(query.pattern)
            cache.set(cacheKey, tupes, 30*60)
         
        nodes = []
        try:
            for mname, nodeType in tupes:
                if nodeType == 'L':
                    reader  = KairosdbReader(KAIROSDB_URL, mname)
                    nodes.append(self.getLeafNode(mname, reader, avoidIntervals=True))
                elif nodeType == 'B':
                    nodes.append(BranchNode(mname))
                else:
                    assert False, "KairosDBFinder.find_nodes(): ERROR: got wrong node type back from nodeType: %s" % (nodeType)
        except Exception as e:
            tb = traceback.format_exc()
            log.info("finders.KairosDBFinder.find_nodes(%s) EXCEPTION: e: %s, %s, tupes: %s." % (query, e, tb, tupes))
        if 0:
            log.info("finders.KairosDBFinder.find_nodes(%s) saving data! %d nodes." % (query.pattern, len(nodes)))
            if 0:
                log.info("finders.KairosDBFinder.find_nodes(%s) nodes to save: %s" % (query.pattern, nodes))
        delay = time.time() - timeStart
        log.info("KairosDBFinder.find_nodes(): kdbFindNodesDelay: %05.08f #tupes: %s query: %s" % (delay, len(tupes), query))
        return nodes

###############################################################################

class KairosdbReader(object):
    __slots__ = ('kairosdb_uri', 'metric_name')
    supported = True

    def __init__(self, kairosdb_uri, metric_name):
        self.kairosdb_uri = kairosdb_uri
        self.metric_name = metric_name
        #log.info("KairosdbReader.__init__(): uri: %s, metricname: %s" % (kairosdb_uri, metric_name))

    def get_intervals(self):
        return IntervalSet([Interval(0, time.time())])

    def fetch(self, startTime, endTime):

        def get_data():
            utils = KairosdbUtils()
            post_data_obj = {
                'start_absolute' : utils.graphite_time_to_kairosdb_time(startTime) - 1000,
                'end_absolute'   : utils.graphite_time_to_kairosdb_time(endTime),
                'metrics': [{
                    'tags'          : {},
                    'name'          : self.metric_name,
                    'aggregators'   : [ 
                        #{
                        #'name'              : 'avg',
                        #'align_sampling'    : 'true',
                        #'sampling'          : {
                        #    'value' : '1',
                        #    'unit'  : 'minutes'
                        #    }
                        #}
                        ]
                    }]
                }
            post_data = json.dumps(post_data_obj)
            #log.info("KairosDBReader.fetch(): post data: %s" % (post_data))
            post_response = utils.post_kairosdb_url(self.kairosdb_uri, 'datapoints/query', data=post_data)
            if 'errors' in post_response.keys():
                log.info("KairosDBReader.fetc(): errors found: %s" % (str(post_response)))
                time_info = startTime, endTime, 1
                return (time_info, [])
            values = post_response['queries'][0]['results'][0]['values']
            values_length = len(values)
            if values_length == 0:
                time_info = (startTime, endTime, 1)
                datapoints = []
                return (time_info, datapoints)
            else:
                if values_length == 1:
                    time_info = (startTime, endTime, 1)
                    datapoints = [values[0].value]
                    return (time_info, datapoints)
                else:
                    # 1. Calculate step (in seconds)
                    #    Step will be lowest time delta between values or 1 (in case if delta is smaller)
                    step = 1
                    minDelta = None
                    for i in range(0, values_length - 2):
                        (timeI, valueI) = values[i]
                        (timeIplus1, valueIplus1) = values[i + 1]
                        timeI = utils.kairosdb_time_to_graphite_time(timeI)
                        timeIplus1 = utils.kairosdb_time_to_graphite_time(timeIplus1)
                        delta = timeIplus1 - timeI
                        if (minDelta == None) or (delta < minDelta):
                            minDelta = delta
                    if minDelta > step:
                        step = minDelta
                    # 2. Fill time info table
                    time_info = (startTime, endTime, step)
                    # 3. Create array of output points
                    number_points = int(math.ceil((endTime - startTime) / step))
                    datapoints = [None for i in range(number_points)]
                    # 4. Fill array of output points
                    cur_index = 0
                    cur_value = None
                    cur_time_stamp = None
                    cur_value_used = None
                    for i in range(0, number_points - 1):
                        data_point_time_stamp = startTime + i * step
                        (cur_time_stamp, cur_value) = values[cur_index]
                        cur_time_stamp = utils.kairosdb_time_to_graphite_time(cur_time_stamp)
                        while cur_index + 1 < values_length:
                            (next_time_stamp, next_value) = values[cur_index + 1]
                            next_time_stamp = utils.kairosdb_time_to_graphite_time(next_time_stamp)
                            if next_time_stamp > data_point_time_stamp:
                                break
                            (cur_value, cur_time_stamp, cur_value_used) = (next_value, next_time_stamp, False)
                            cur_index = cur_index + 1
                        data_point_value = None
                        if (not cur_value_used) and (cur_time_stamp <= data_point_time_stamp):
                            cur_value_used = True
                            data_point_value = cur_value
                        datapoints[i] =  data_point_value

                    return (time_info, datapoints)

        # above was all just one def-inside-a-def.  Dont' blame me, this is code-as-found-in-someone-else's-project.

        job = KAIROSDB_REQUEST_POOL.apply_async(get_data)
        return FetchInProgress(job.get)


###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################

