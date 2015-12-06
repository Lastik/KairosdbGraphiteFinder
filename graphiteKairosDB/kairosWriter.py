#!/usr/bin/env python2.6
################################################################################

import re
# import os
import time
# import random
import requests
# import traceback
import json
# from helpers import sanitize, isBadMetricPath
import logging
import logging.handlers
from optparse import OptionParser
from pprint import pprint, pformat
import pika
from string import maketrans

################################################################################

BAD_METRIC_CHARACTERS_REGEX = re.compile(r'[^a-zA-Z\-_0-9.:]')
BAD_CHARS = ' /*?[]:(),+'
BAD_CHAR_TRANSLATOR = maketrans(BAD_CHARS, '_' * len(BAD_CHARS))
DELETE_CHARS = '' # candidates: ^ & \
# KAIROS_VIP = "http://localhost:8080"


################################################################################

class AllDoneException(Exception):
    pass

################################################################################

class KairosWriter(object):

    def __init__(self, logger=None):
        self.log                    = logger
        self.logDir                 = "/tmp/kairosdb/logs"
        self.debug                  = False
        self.verbose                = False
        self.sendCounter            = 0
        self.errorCounter           = 0
        self.rabbitmqServer         = 'localhost'
        self.setupStats()

    def setupStats(self):
        statsRate    = '''socketLinesRece socketBytesRece socketConns'''.split()
        statsNonRate = '''numSites eventQueueSize dataQueueSize usertime systime mem_mb'''.split()
        #self.counterStats = CounterStats(names=statsRate + statsNonRate)
        #self.counterStats.setIsRateOnNames(statsNonRate, isRate=False)

    def getLogger(self, logger=None):
        try:
            logging.getLogger("requests").setLevel(logging.CRITICAL)
        except:
            pass
        if logger:
            self.log = logger
        else:
            self.log = logging.getLogger()   # needed, mult. process write to same logfile, this one coordinates.
            self.log.setLevel(logging.INFO)
            logfilename = self.logDir + "/kairosWriter.log"
            rfh = logging.handlers.WatchedFileHandler(logfilename)
            f = logging.Formatter('%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
            rfh.setFormatter(f)
            sth = logging.StreamHandler()
            sth.setFormatter(f)
            self.log.addHandler(sth)
            self.log.addHandler(rfh)
        return

    def getOptionParser(self):
        return OptionParser()

    def getOptions(self):
        parser = self.getOptionParser()
        parser.add_option("-d", "--debug",   action="store_true", dest="debug",   default=False, help="debug mode for this program, writes debug messages to logfile." )
        parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False, help="verbose mode for this program, prints a lot to stdout." )
        parser.add_option("-r", "--rmq", action="store", default="localhost", dest="rmqServer",  help="FQDN of rabbit mq server." )
        parser.add_option("-k", "--kdb", action="store", default="localhost", dest="kdbVip", help="FQDN of kairosdbvip for storing." )
        parser.add_option("-p", "--kdbPort", action="store", default=8080, dest="kdbPort", help="kairosdb port")
        (options, args)             = parser.parse_args()
        self.verbose                = options.verbose
        self.debug                  = options.debug
        self.rabbitmqServer         = options.rmqServer
        self.kairosdbVip            = options.kdbVip
        self.kairosdbPort           = options.kdbPort
        return

    # def callback(ch, method, properties, body):
    #     stime = time.time()
    #     sendDataToKairos(body)
    #     print " [x] Received %r" % (body,)

    def main(self):
        self.getLogger()
        self.getOptions()
        connection = pika.BlockingConnection(pika.ConnectionParameters(self.rabbitmqServer))
        channel = connection.channel()
        channel.queue_declare(queue='measurements')
        channel.basic_consume(self.callback, queue='measurements', no_ack=False)
        self.log.info(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()

    def callback(self, channel, method, properties, body):
        stime = time.time()
        try:
            if not body.startswith('['):
                body = '['+body+']'
            body = body.replace('None', '3')
            self.log.debug(" [x] BODY_RECEIVED: %s" % (pformat(body)))
            jdata = json.loads(body)
            firstbit = jdata[:100]
            self.log.debug(" [x] Received     json text %r" % (firstbit,))
            #if jdata:
                #self.log.debug("body is not blank: body: {body}".format(body=jdata))
            if not jdata:
                #self.log.error("jdata is blank")
                #self.log.error("body was: {body}".format(body=body) )
                raise Exception
        except Exception as e:
            self.log.info(" [x] Received non-json text, e: %s, text: %s" % (str(e), pformat(body)))
        retval = self.sendDataToKairos(jdata)
        if retval:
            channel.basic_ack(delivery_tag = method.delivery_tag)
        return

    def sanitize(self, word):
        "Remove illegal characters from a graphite metric path"
        return word.translate(BAD_CHAR_TRANSLATOR, DELETE_CHARS)

    def isBadMetricPath(self, metricname):
        if not metricname:
            return "Must provide metricname of nonzero length, got empty string."
        if not type(metricname) == str:
            return "metricname must be of type string, instead got type: %s" % (str(type(metricname)))
        sp = metricname.split('.')
        for elem in sp:
            if not elem:
                return "must not have empty section in metricname, got '%s', split into: '%s'" % (metricname, sp)
        if not sp[0]:
            return "must not have leading dot in metricname, got '%s'" % (metricname)
        if len(sp) < 2:
            return "metricname must have at least two elements separated by a dot, got '%s'" % (metricname)
        if ' ' in metricname:
            return "metricname must not contain embedded spaces, got '%s'" % (metricname)
        bad_chars = BAD_METRIC_CHARACTERS_REGEX.findall(metricname)
        if bad_chars:
            return "metricname contains invalid characters (%s), got '%s'" % (bad_chars, metricname)
        return False

    def makeKairosObject(self, inPath, val, timestamp):
        try:
            metricPath = self.sanitize(inPath)
            err = self.isBadMetricPath(metricPath)
            if err:
                self.log.error("bad metric path: %s, err: %s" % (inPath, err))
                return None
            if not metricPath:
                self.log.error("MetricPath is None, inpath: %s" % (inPath))
                return None
            timestamp = timestamp * 1000  # must give MILLISECONDS FROM EPOCH.

            json_obj = {
                'timestamp'     : timestamp,
                'name'          : metricPath,
                'value'         : val,
                'tags'          : {"receiver": "receiv"}
            }
            try:
                import socket
                kwhost = str(socket.getfqdn)
                kwhost = kwhost.replace(".","_")
                json_obj['tags'].update({"kw_writer": kwhost})
            except Exception as e:
                self.log.error("excetion on fqdn: {e}".format(e=e))
            if 0:
                try:
                    tags = metricPath.split('.')
                    for i in tags:
                        tag_name = "t{index}".format(index=str(tags.index(i)))
                        tag = str(i)
                        json_obj['tags'].update({tag_name: tag})
                except Exception as e:
                    self.log.info("printing exception on tags: {e}".format(e=e))
        except Exception as e:
            self.log.info("printing exception: {e}".format(e=e))
            json_obj = None
        return json_obj

    def logStatsError(self):
        self.errorCounter += 1
        if self.sendCounter % 1000 == 0:
            self.log.info("Have had %s k errors") % (int(self.errorCounter /1000))

    def logStats(self):
        self.sendCounter += 1
        if self.sendCounter % 1000 == 0:
            self.log.info("Have handled %s k requests." % (int(self.sendCounter / 1000)))
            pass

    def sendDataToKairos(self, jdata):
        try:
            name = None
            printEvery = 400
            self.logStats()
            # app.logger.info("sendDataToKairos(): called.")
            if isinstance(jdata, dict):
                jdata = [jdata]
            assert isinstance(jdata, list), "jdata is not list, should be, type is %s" % (type(jdata))
            kairosObjects = []
            # lastName = "NONE"
            numItems = len(jdata)
            if self.verbose or self.debug or (self.sendCounter % printEvery == 0):
                self.log.info("sendDataToKairos(): ITEM_COUNT=%s" % (numItems))
            counter = 0
            for item in jdata:
                counter += 1
                if self.debug or self.verbose or (self.sendCounter % printEvery == 0):
                    self.log.info("sendDataToKairos(): item %5.5d of %5.5d: %s" % (counter, numItems, item))
                try:
                    if item.get("eventType", None) != 'measurement':
                        # ignore errors, heartbeats, etc., for now.  Not even coded as possible, so log it.  later, ignore.
                        self.log.info("BAD_DATA:  Non-measurement received, event: %s" % (item))
                        continue
                    if item.get('metricType', None) != 'raw':
                        self.log.info("BAD_DATA:  Non-Raw measurement received, ignoring for now: %s." % (item))
                        continue
                    name = item.get('metric', '')
                    name = name.encode('utf-8')
                    assert len(name), "BAD_DATA:  Metric name not found, event: %s" % (item)
                    assert isinstance(name, str), "BAD_DATA:  Name is not a string, name: %s, type: %s" % (pformat(name), type(name))
                    assert name.count('.') > 2, "BAD_DATA:  Name must have at least 2 dots, did not, name: %s." % (name)
                    val = float(item.get('value', None))
                    timestamp = int(float(item.get('timestamp', None)))  # only care about seconds.
                    nobj = self.makeKairosObject(name, val, timestamp)
                    if nobj is None:
                        self.logStatsError()
                        self.log.info("sendDataToKairos(): BAD_DATA:  nobj is none.")
                        continue
                    kairosObjects.append(nobj)
                except Exception as e:
                    self.log.info("sendDataToKairos(): EXCEPTION: %s, bad data, return True so remove from queue by ack'ing and continuing." % (str(e)))
            # self.log.info("sendDataToKairos(): kairosObjects: %s" % (pformat(kairosObjects)))
            # data = data.replace(" ", "")
            # print json.dumps(kairosObjects)
            if not kairosObjects:
                self.logStatsError()
                self.log.info("sendDataToKairos(): no kairosObjects, original data: %s, returning." % (str(jdata)))
                return True
            KAIROS_VIP = "http://" + self.kairosdbVip + ":" + self.kairosdbPort
            url = KAIROS_VIP + "/api/v1/datapoints"
            headers = { "Content-Type": "application/json" }
            tstart = time.time()
            obj = kairosObjects
            jobj = json.dumps(obj)
            # self.log.info("sendDataToKairos(): obj: %s, jobj: %s" % (pformat(obj), pformat(jobj)))
            nr = requests.post(url, data=json.dumps(obj), headers=headers, timeout=2.0)
            delay = time.time() - tstart
            scode = nr.status_code
            if not scode == 204:
                return False
            if self.verbose or self.debug or (self.sendCounter % printEvery == 0):
                self.log.info("sendDataToKairos():  (printline every %s events as sample) POST_STATUS: %s, delay: %08.08f name: %s" % (printEvery, scode, delay, name))
            try:
                nr.close()   # close http connection to help socket counts.
            except Exception as e:
                self.log.info("Cannot close, e: %s" % (e))
        except Exception as e:
            self.log.error("Exception during sendDataToKairos(): %s, sendCounter: %s" % (e, self.sendCounter))
        return True

################################################

if __name__ == '__main__':
    def runit():
        kwriter = KairosWriter()
        kwriter.main()
    runit()

###############################################################################




