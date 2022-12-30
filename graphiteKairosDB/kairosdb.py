import sys, os
	
import re
import requests
import time
import math

import requests
from multiprocessing.pool import ThreadPool
from django.conf import settings

from graphite.intervals import Interval, IntervalSet
from graphite.node import LeafNode, BranchNode
from graphite.readers import FetchInProgress

import json

KAIROSDB_MAX_REQUESTS = 10
KAIROSDB_REQUEST_POOL = ThreadPool(KAIROSDB_MAX_REQUESTS)

class KairosNode(object):
    def __init__(self):
        self.child_nodes = []
    
    #Node is leaf, if it has no child nodes.
    def isLeaf(self):
        return len(self.child_nodes) == 0
    
    #Add child node to node.
    def addChildNode(self, node):
        self.child_nodes.append(node)
    
    #Get child node with specified name
    def getChild(self, name):
        for node in self.child_nodes:
            if node.name == name:
                return node
        return None
    
    def getChildren(self):
        return self.child_nodes

    
class KairosTree(KairosNode):
    pass
            

class KairosRegularNode(KairosNode):
    def __init__(self, name):
        KairosNode.__init__(self)
        self.name = name
    
    def getName(self):
        return self.name
 
                
class Utils(object):
    def kairosdb_time_to_graphite_time(self, time):
        return time / 1000;

    def graphite_time_to_kairosdb_time(self, time):
        return time * 1000;
    
    def get_kairosdb_url(self, kairosdb_uri, url):
        full_url = "%s/%s" % (kairosdb_uri, url)
        return requests.get(full_url).json()


    def post_kairosdb_url(self, kairosdb_uri, url, data):
        full_url = "%s/%s" % (kairosdb_uri, url)
        return requests.post(full_url, data).json()
    

class KairosdbReader(object):
    __slots__ = ('kairosdb_uri', 'metric_name')
    supported = True

    def __init__(self, kairosdb_uri, metric_name):
        self.kairosdb_uri = kairosdb_uri
        self.metric_name = metric_name

    def get_intervals(self):
        return IntervalSet([Interval(0, time.time())])

    def fetch(self, startTime, endTime):
        def get_data():
            
            utils = Utils()
            
            post_data_obj = {
                    'start_absolute': utils.graphite_time_to_kairosdb_time(startTime) - 1000,
                    'end_absolute': utils.graphite_time_to_kairosdb_time(endTime),
                    'metrics': [{
                                 'tags':{}, 
                                 'name': self.metric_name
                                }]
                    }
            
            post_data = json.dumps(post_data_obj)
            
            post_response = utils.post_kairosdb_url(self.kairosdb_uri, 'datapoints/query', data=post_data)

            if 'errors' in post_response.keys():
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
                        
                        if (minDelta == None or delta < minDelta):
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
                        
                        while (cur_index + 1 < values_length):
                            (next_time_stamp, next_value) = values[cur_index + 1]
                            next_time_stamp = utils.kairosdb_time_to_graphite_time(next_time_stamp)
                            if next_time_stamp > data_point_time_stamp:
                                break;
                            (cur_value, cur_time_stamp, cur_value_used) = (next_value, next_time_stamp, False)
                            cur_index = cur_index + 1
                            
                        data_point_value = None
                        if(not cur_value_used and cur_time_stamp <= data_point_time_stamp):
                            cur_value_used = True
                            data_point_value = cur_value
                        
                        datapoints[i] =  data_point_value
        
                    return (time_info, datapoints)

        job = KAIROSDB_REQUEST_POOL.apply_async(get_data)

        return FetchInProgress(job.get)
    
    
class KairosdbFinder(object):
    def __init__(self, kairosdb_uri=None):
        self.kairosdb_uri = settings.KAIROSDB_URL.rstrip('/')
        
    # Fills tree of metrics out from flat list
    # of metrics names, separated by dot value
    def _fill_kairo_tree(self, metric_names):
        tree = KairosTree()
        
        for metric_name in metric_names:
            name_parts = metric_name.split('.')
            
            cur_parent_node = tree
            cur_node = None
            
            for name_part in name_parts:
                cur_node = cur_parent_node.getChild(name_part)
                if cur_node == None:
                    cur_node = KairosRegularNode(name_part)
                    cur_parent_node.addChildNode(cur_node)
                cur_parent_node = cur_node
        
        return tree
    
    
    def _find_nodes_from_pattern(self, kairosdb_uri, pattern):
        query_parts = []
        for part in pattern.split('.'):
            part = part.replace('*', '.*')
            part = re.sub(
                r'{([^{]*)}',
                lambda x: "(%s)" % x.groups()[0].replace(',', '|'),
                part,
            )
            query_parts.append(part)
        
        utils = Utils()
        
        #Request for metrics
        get_metric_names_response = utils.get_kairosdb_url(kairosdb_uri, "metricnames")
        metric_names = get_metric_names_response["results"]
        
        #Form tree out of them
        metrics_tree = self._fill_kairo_tree(metric_names)    
        
        for node in self._find_kairosdb_nodes(kairosdb_uri, query_parts, metrics_tree):
            yield node
    
    def _find_kairosdb_nodes(self, kairosdb_uri, query_parts, current_branch, path=''):
        query_regex = re.compile(query_parts[0])
        for node, node_data, node_name, node_path in self._get_branch_nodes(kairosdb_uri, current_branch, path):
            dot_count = node_name.count('.')
    
            if dot_count:
                node_query_regex = re.compile(r'\.'.join(query_parts[:dot_count+1]))
            else:
                node_query_regex = query_regex
    
            if node_query_regex.match(node_name):
                if len(query_parts) == 1:
                    yield node
                elif not node.is_leaf:
                    for inner_node in self._find_kairosdb_nodes(
                        kairosdb_uri,
                        query_parts[dot_count+1:],
                        node_data,
                        node_path,
                    ):
                        yield inner_node
    
    
    def _get_branch_nodes(self, kairosdb_uri, input_branch, path):
        results = input_branch.getChildren()
        if results:
            if path:
                path += '.'
                
            branches = [];
            leaves = [];
            
            for item in results:
                if item.isLeaf():
                    leaves.append(item)
                else:
                    branches.append(item)
            
            if (len(branches) != 0):
                for branch in branches:
                    node_name = branch.getName()
                    node_path = path + node_name
                    yield BranchNode(node_path), branch, node_name, node_path
            if (len(leaves) != 0):
                for leaf in leaves:
                    node_name = leaf.getName()
                    node_path = path + node_name
                    reader = KairosdbReader(kairosdb_uri, node_path)
                    yield LeafNode(node_path, reader), leaf, node_name, node_path

    def find_nodes(self, query):
        for node in self._find_nodes_from_pattern(self.kairosdb_uri, query.pattern):
            yield node
