#!/usr/bin/env python2.6
###############################################################################

import sys
import os
import time
import traceback
import re
import fnmatch
from pprint import pformat

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ValueSequence
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import dict_factory

from mockLogger import MockLogger

###############################################################################


class BadMetricNameException(Exception):
    pass

###############################################################################


class MetricType(object):
    '''
    This is a utility class and does not represent one specific Metric object.
    '''
    def __init__(self, connectionObject=None, logger=None):
        self.log = logger or MockLogger()
        self.session    = None
        self.cache_max  = 10 * 1000 * 1000
        self._cache     = {} # Do not access this directly, use addDocToCache()
        self._qpCache   = {}
        self._qpCacheExpirySeconds = 60 * 60
        self.badCharPattern = re.compile(r'[^a-zA-Z\-_0-9.:]')
        self.lastCheckUpdateRetval = 0
        self.checkUpdateRetvalEvery = 1000

    def __del__(self):
        self.closeConnection()

    def addDocToCache(self, name, val):
        self._cache[name] = val
        if self.cache_max:
            while len(self._cache) > self.cache_max:
                self._cache.popitem()

    def invalidateCache(self, name):
        self._cache[name] = None
                
    def closeConnection(self):
        if self.session:
            self.log.debug("metricType.closeConnection(): trying to close conection...")
            try:
                self.session.close()
            except Exception as e:
                self.log.info("metricType.closeConnection(): Exception, e: %s." % (str(e)))
                # disconnected now
                pass
        self.log.info("metricType.closeConnection(): Closed.")
        
    def openConnection(self, session=None):
        #self.log.debug("metricType.openConnection(): server:%s port:%s dbname:%s." % (mongo_server, mongo_port, dbname))
        if session != None:
            self.log.info("MetricType.openConnection(): have connection object, re-using it.")
            self.session = session
            self.log.info("MetricType object using passed-in connection object.")
        else:
            cluster = Cluster(['10.235.67.65'])
            self.session = cluster.connect()  # aka session 
            self.log.info("Instantiated new connection for metricType: %s" % (self.session))
            keyspace = 'metricnamecache'
            self.session.set_keyspace(keyspace)
            self.session.row_factory = dict_factory    # always return results as a dict.
            self.log.info("set keyspace to %s" % (keyspace))
        self.log.info("metricType.openConnection(): Connection established, have session.")
        return

    def getAllMetricTypeRecords(self):
        self.log.debug("Querying metrictype for all metric names:  START:" )
        q = '''select * from metrictype'''
        rows = self.session.execute(q)
        counter = 0
        for r in rows:
            #self.log.debug("Row: %s" % (pformat(r)))
            counter += 1
            self.log.info("Row #%9.9d: %s" % (counter, r))
        self.log.debug("Querying metrictype for all metric names,  END." )
   
    def getMetricTypeRec(self, metricname):
        rows = self.session.execute('select * from metrictype where metricname = %s', [metricname])
        if not rows:
            return None
        assert len(rows) == 1, "should have one row, have more, metricname: %s, result: %s" % (metricname, rows)
        return rows[0]

    def getByName(self, metricname, saveNew=True):
        if not metricname:
            return None
        doc = self._cache.get(metricname)
        if doc:
            self.log.info("getByName: %s found name in cache, returning %s." % (metricname, self.strMtDoc(doc)))
            return doc
        self.log.debug("metricType.getByName(): Missed cache, querying, metricname=%s" % (metricname))
        doc = self.getMetricTypeRec(metricname)
        self.log.debug("metricType.getByName(): Query found doc: %s" % (self.strMtDoc(doc)))
        if doc:
            self.addDocToCache(metricname, doc)
            return doc
        ##Link2MetricName:  might be a link intervening.  find sublists of a.b.c.d --> [a, a.b, a.b.c, a.b.c.d]
        ##Link2MetricName:  possLinks = set()
        ##Link2MetricName: for c in range(0, metricname.count('.')+1): 
        ##Link2MetricName:     possLinks.add('.'.join(metricname.split('.')[0:c+1]))
        ##Link2MetricName: possDocs = self.getByNames(possLinks)
        ##Link2MetricName: if possDocs:
        ##Link2MetricName:     possDocs = sorted(possDocs, key=lambda x: len(x['metricname']))
        ##Link2MetricName:     possLinkDoc  = possDocs[-1]
        ##Link2MetricName:     search_mname = metricname.replace(possLinkDoc['metricname'], possLinkDoc['linktometricname'])
        ##Link2MetricName:     ret = self.getByName(search_mname, saveNew=saveNew)
        ##Link2MetricName:     if ret:
        ##Link2MetricName:         self.addDocToCache(metricname, ret)
        ##Link2MetricName:     return ret
        if saveNew:
            st = time.time()
            doc = self.createMetricTypeDoc(metricname)
            self.log.debug("Delay creating mtdoc is %s" % (time.time() - st))
        if doc:
            self.addDocToCache(metricname, doc)
        return doc

    def _getByNamesList(self, metricnames):
        query = 'SELECT * FROM metrictype WHERE metricname IN %s'
        rows = self.session.execute(query, parameters=[ValueSequence(metricnames)])
        return rows

    def getByNames(self, queryList):
        gotAll = True
        retList = []
        for metricName in queryList:
            rec  = self._cache.get(metricName)
            if rec:
                # self.log.debug("getByName: %s found name in cache, returning %s." % (metricname, self.strMtDoc(doc)))
                retList.append(rec)
            else:
                gotAll = False
        if gotAll:
            #self.log.info("getByNames: queryList: (%s) all found in cache, returning." % (queryList))
            return retList
        cur = self._getByNamesList(queryList)
        for rec in cur:
            mname = rec.get('metricname', None)
            if not mname:  # had this happen once, test for it now.
                self.log.warning("Found Metrictype record with no metricname: %s" % (rec))
                continue
            retList.append(rec)
            self._cache[mname] = rec  # populate cache 
        return retList

    def invalidateCacheForName(self, metricname):
        try:
            del self._cache[metricname]
        except:
            pass

    ###Obs: def getByName(self, metricname, saveNew=True):
    ###Obs:     # Will occassionally suffer from race condition for brand-new records.
    ###Obs:     # * 2+ concurrent processes trying to create parent, parent-of-parent, etc.
    ###Obs:     # * solve this by just trying again;
    ###Obs:     # * if a.b.c.d.e.f.g1 and a.b.c.d.e.f.g2 race, trying to create parents, alternate failures, will TB 3 times.
    ###Obs:     numTries = 100  # Note, will do 1 more time than this
    ###Obs:     while numTries > 0:
    ###Obs:         try:
    ###Obs:             return self._getByName(metricname, saveNew)
    ###Obs:         except pymongo.errors.DuplicateKeyError:
    ###Obs:             numTries -= 1
    ###Obs:     # last time, if fails, will return traceback up the stack.
    ###Obs:     return self._getByName(metricname, saveNew)

    def strMtDoc(self, doc):
        if not doc:
            return "No doc given."
        if not type(doc)  == dict:
            return "doc not a dict, doc: %s" % (pformat(doc))
        kids = doc.get('children') 
        if kids is None:
            outkids = "MISSING"
        elif len(kids) > 10:
            outkids = "%d children" % (len(kids))
        else:
            outkids = kids
        out = "<MetricType metricname: %s num kids: %s" % (doc.get('metricname'), outkids)
        return out 

    ###ConvAlready: def _getByName(self, metricname, saveNew=True):
    ###ConvAlready:     if not metricname:
    ###ConvAlready:         return None
    ###ConvAlready:     doc = self._cache.get(metricname)
    ###ConvAlready:     if doc:
    ###ConvAlready:         # self.log.debug("getByName: %s found name in cache, returning %s." % (metricname, self.strMtDoc(doc)))
    ###ConvAlready:         return doc
    ###ConvAlready:     # self.log.debug("metricType.getByName(): Missed cache, querying, metricname=%s" % (metricname))
    ###ConvAlready:     doc = self.db.metricType.find_one({'metricname' : metricname})
    ###ConvAlready:     # self.log.debug("metricType.getByName(): Query found doc: %s" % (self.strMtDoc(doc)))
    ###ConvAlready:     if doc:
    ###ConvAlready:         self.addDocToCache(metricname, doc)
    ###ConvAlready:         return doc
    ###ConvAlready: 
    ###ConvAlready:     # might be a link intervening.  find sublists of a.b.c.d --> [a, a.b, a.b.c, a.b.c.d]
    ###ConvAlready:     possLinks = set()
    ###ConvAlready:     for c in range(0, metricname.count('.')+1): 
    ###ConvAlready:         possLinks.add('.'.join(metricname.split('.')[0:c+1]))
    ###ConvAlready:     possDocs = self.db.metricType.find({ 'linktometricname' : { '$ne' : None }, 'metricname' : { '$in' : list(possLinks) } } )
    ###ConvAlready:     possDocs = list(possDocs)
    ###ConvAlready: 
    ###ConvAlready:     if possDocs:
    ###ConvAlready:         possDocs = sorted(possDocs, lambda x: len(x['metricname']))
    ###ConvAlready:         possLinkDoc  = possDocs[-1]
    ###ConvAlready:         search_mname = metricname.replace(possLinkDoc['metricname'], possLinkDoc['linktometricname'])
    ###ConvAlready: 
    ###ConvAlready:         ret = self.getByName(search_mname, saveNew=saveNew)
    ###ConvAlready:         if ret:
    ###ConvAlready:             self.addDocToCache(metricname, ret)
    ###ConvAlready:         return ret
    ###ConvAlready:     
    ###ConvAlready:     if not doc and saveNew:
    ###ConvAlready:         st = time.time()
    ###ConvAlready:         doc = self.createMetricTypeDoc(metricname)
    ###ConvAlready:         self.log.debug("Delay creating mtdoc is %s" % (time.time() - st))
    ###ConvAlready:     if doc:
    ###ConvAlready:         self.addDocToCache(metricname, doc)
    ###ConvAlready:     return doc

    def linkExists(self, realMetricName, linkName):
        mtdoc = self.getByName(realMetricName, saveNew=False)
        if not mtdoc:
            self.log.error("MetricType.linkExists(): No such metricName: %s" % (realMetricName))
            return False
        newLinkDoc  = self.getByName(linkName, saveNew=False)
        return bool(newLinkDoc)

    #   Just like calling linux ln -s command.
    #   if existing one is a.b, call as createMetrictTypeLink('a.b', 'x.y')
    def createLink(self, realMetricName, linkName):
        #if not linkName.startswith("obu.sites"):
        #    self.log.error("createLink(): ERROR:  createLink that doesn't start with obu.sites, called with linkname: %s, realname: %s" % (linkName, realMetricName))
        #    return False
        mtdocReal = self.getByName(realMetricName, saveNew=False)
        if not mtdocReal:
            self.log.error("MetricType.createLink(): No such metricName: %s" % (realMetricName))
            return False
        self.log.info("MetricType.createLink(): CONFIRMED.  realMetricName doc exists, name: %s, doc: %s" % (realMetricName, mtdocReal))
        mtdocLink = self.getByName(linkName, saveNew=False)
        self.log.info("MetricType.createLink(): getByName on linkName: %s returns doc: %s" % (linkName, mtdocLink))
        if mtdocLink:
            existingLink = mtdocLink.get('linktometricname', '')
            if existingLink == realMetricName:
                self.log.info("MetricType.createLink(): Already linked correctly, link %s points to real %s." % (linkName, realMetricName))
                return mtdocLink
        else:
            self.log.info("MetricType.createLink(): Creating link name: %s" % (linkName))
            self.getByName(linkName, saveNew=True)
            mtdocLink = self.db.metricType.find_one({ 'metricname' : linkName })
            if not mtdocLink:
                self.log.error("MetricType.createLink(): ERROR:  Add of metricName failed, linkname: %s" % (linkName))
                return False
            self.log.info("Created MT rec that will have link field set, not set yet.")
        # now have rec, can update it to have linktometricname field correctly set.
        wc = 1   # always want link creation to be verified, it is important.
        retval = self.db.metricType.update({ 'metricname' : linkName }, { '$set' : { 'linktometricname' : realMetricName, 'children' : mtdocReal.get('children') } }, w=wc)
        if (retval.get('n', 0) == 0):
            self.log.error("MetricType.createLink(): WARNING: ERROR!  Update of metrictype to add linktometricname failed.  realname=%s, linkname=%s." % (realMetricName, linkName))
            return None
        self.log.info("MetricType.createLink():  SUCCESS, retval OKAY from MT.update() call: %s" % (retval))
        self.invalidateCache(linkName)
        mtdocLink = self.db.metricType.find_one({ 'metricname' : linkName })
        self.log.info("MetricType.createLink(): verifying, after update, find_one now yields: %s" % (mtdocLink))
        self.fixChildren(self.getParentMetricName(linkName))
        return mtdocLink 

    ###Obsolete: def getAllDocs(self):
    ###Obsolete:     cur = self.db.metricType.find({})
    ###Obsolete:     for rec in cur:
    ###Obsolete:         mname = rec.get('metricname', None)
    ###Obsolete:         self.addDocToCache(mname, rec)
    ###Obsolete:         yield rec

    def getByRegex(self, metricNameRegex, limitNum=None):
        # NOTE:  Cannot use cache, might be incomplete.
        # { name : { $regex : 'acme.*corp', $options: 'i' } }
        # self.log.info("metricType.getByRegex(): querying regex=%s" % (metricNameRegex))
        if not limitNum:
            docs = self.db.metricType.find({ 'metricname' : { '$regex': metricNameRegex, '$options': 'i'}})
        else:
            docs = self.db.metricType.find({ 'metricname' : { '$regex': metricNameRegex, '$options': 'i'}}).limit(limitNum)
        doclist = list(docs)
        # self.log.info("metricType.getByRegex(): found doclist array w/ len = %s" % (len(doclistarray)))
        for doc in doclist:             
            metricname = doc['metricname']
            self.addDocToCache(metricname, doc)
        return doclist

    def isMetricNameCached(self, mname):
        return self._cache.get(mname)

    def getRootDocs(self):
        res = self.getByParentName(None)
        for doc in res:
            metricname = doc['metricname']
            self.addDocToCache(metricname, doc)
        return res

    def regexOrGlobCharsInString(self, instr):
        for c in ' * [ ] ? '.split():
            if c in instr:
                return True
        return False

    def getNamesGivenGlob(self, qp):
        pat = fnmatch.translate(qp)
        fcursor = self.db.metricType.find({'metricname': { '$regex' : pat } })
        ret = []
        for mtobj in fcursor:
            name = mtobj.get('metricname', None)
            if not name: continue
            ret.append(name)
        return ret        

    def childrenForMetrictype(self, metricTypeDoc):
        pass

    def find_mnames(self, qp):
        ''' given string, return array of un-aliased strings'''
        if (not qp)   : return []
        cacheData = self.getQpFromCache(qp)
        if cacheData:
            return cacheData
        first, star, last = self.splitOnStars(qp)
        #self.log.info("find_mnames(): qp: %s, fsl: f=%s, s=%s, l=%s" % (qp, first, star, last))
        if not first:   # starts with a wildcard, get root docs.
            rdocs = self.getRootDocs()
            rdocNames = [x.get('metricname', None) for x in rdocs if x is not None]
            tlist = []
            for name in rdocNames:
                pat = fnmatch.translate(star)  # get regex pattern
                gotMatch = bool(re.match(pat, name))
                #self.log.info("find_mnames(): First: %s, Star: '%s', last: '%s', regex: '%s', gotMatch: %s" % (first, star, last, pat, gotMatch))
                if not gotMatch:
                    continue
                fname = '%s.%s' % (name, last)
                if not last:
                    fname = name
                if gotMatch:
                    tlist.extend(self.find_mnames(fname))
            return tlist
        realName, linkpart, realpart = self.translateLinkedPath(first)
        #self.log.info("find_mnames(): tlp returned rn: %s, lp: %s, rp: %s." % (realName, linkpart, realpart))
        realMt = self.getByName(realName, saveNew=False)
        #self.log.info("find_mnames(): realname returned: %s" % (realMt))
        if not realMt:
            #self.log.info("find_mnames(): non, returning empty.")
            return []
        if not star:
            # all of it is literal, just return self.
            #self.log.info("find_mnames(): no star, all literal, returning first.")
            return [first]
        # have star.  
        tlist = []
        children = realMt['children']
        linkedKids = []
        pat = fnmatch.translate(star)  # get regex pattern
        pat = r'%s.%s' % (first, pat)
        for childName in children:
            realName = realMt.get('metricname', "")
            if (not linkpart):
                cname = childName
            else:
                cname = childName.replace(linkpart, realpart)
            gotMatch = bool(re.match(pat, cname))
            #self.log.info("find_mnames(): CHILD: %s, rn: %s, cname: %s, gotMatch: %s" % (childName, realName, cname, gotMatch))
            if gotMatch:
                linkedKids.append(cname)
        # self.log.info("find_mnames(): created linkedKids list: %s" % (linkedKids))
        ret = []
        for n in linkedKids:
            if last:
                ret.extend(self.find_mnames("%s.%s" % (n, last)))
            else:
                ret.extend(self.find_mnames(n))
        #self.log.info("find_mnames(): returning: %s" % (ret))
        return ret

    def find_nodes(self, queryPattern): 
        # return array of [ (mtrec1, 'B'), (mtrec2, 'B'), ...]   for branches or 'L' for leaves.
        if not queryPattern:
            return []
        # 4 basic cases: 1: top level, 2: a.b.* navigating tree, 3: a.b.c exact, 4: everything else.
        mnames = self.find_mnames(queryPattern)
        #self.log.info("find_nodes(): have mnames: %s" % (mnames))
        ret = []
        for mn in mnames:
            realName, linkpart, realpart = self.translateLinkedPath(mn)
            mtobj = self.getByName(realName, saveNew=False)
            #self.log.info("find_nodes(): getByName of mn: '%s', real is '%s' obj : %s" % (mn, realName, self.strMtDoc(mtobj)))
            if (not mtobj) or (not mtobj.has_key('children')):  # protect against missing/malformed mtobjects.
                continue
            if mtobj.get('children', []):
                node = (mn, 'B')
            else:
                node = (mn, 'L')
            ret.append(node)
        return ret

    def nowTime(self):
        return time.time()

    def getQpFromCache(self, qp):
        rdict = self._qpCache.get(qp, None)
        if not rdict:
            return None
        expiry = rdict.get('expiry', None)
        now = self.nowTime()
        if (not expiry) or (expiry < now):
            del self._qpCache[qp]
            return None
        val = rdict.get('val', None)
        return val

    def addQpToCache(self, qp, result):
        now = self.nowTime() 
        expiry = now + self._qpCacheExpirySeconds
        # print "Nowtime: %s + %s seconds = %s expiry." % (now, self._qpCacheExpirySeconds, expiry)
        adict = { 'expiry' : expiry, 'val' : result }
        self._qpCache[qp] = adict

    def translateLinkedPath(self, inPath):
        #OPTIMIZATION:  
        if not u'sites' in inPath:
            return (inPath, None, None)
        #OPTIMIZATION
        #
        #self.log.info("metricType.translatePath(): called w/ path: %s" % (inPath))
        subnames = self.getPossibleSubnames(inPath)
        #self.log.info("metricType.translateLinkedPath(): subnames of %s are: %s" % (inPath, subnames))
        recs = [x for x in self.getByNames(subnames)]
        recNames = [x.get('metricname', "NONE") for x in recs]
        #self.log.info("metricType.translateLinkedPath(): recs returned for list: %s" % (recNames))
        recs.sort(key=lambda x: len(x.get('metricname', '')), reverse=True)
        #self.log.info("metricType.translateLinkedPath(): sorted subnames list: %s" % (recs))
        haveLink = False
        linkDest = None
        for rec in recs:
            #self.log.info("metricType.translateLinkedPath(): examining rec: %s" % (rec))
            linkDest = rec.get('linktometricname', '')
            if rec.get('metricname', '') == inPath and (not linkDest):
                # exact match.
                #self.log.info("metricType.translateLinkedPath(): Exact Match, have rec, no link, name: %s" % (inPath))
                return (inPath, None, None)
                break
            if linkDest:
                break
        if not linkDest:
            #self.log.info("metricType.translateLinkedPath(): No links in middle of inPath %s.  Returning self." % (inPath))
            return (inPath, None, None)
        linkPart = rec.get('metricname', '')
        newPath = inPath.replace(linkPart, linkDest, 1)
        # return: realName, linkpart, realpart 
        realPart = linkDest # destination is the real metric path.
        return (newPath, linkDest, linkPart)

    def getPossibleSubnames(self, qp):
        qpArr = qp.split('.')
        if (not qp) or (not qpArr):
            return []
        ret = []
        for n in range(len(qpArr), 0, -1):
            name = '.'.join(qpArr[0:n])
            ret.append(name)
        #self.log.info("getPossibleSubnames(): qp: %s, returning %s." % (qp, ret))
        return ret

    def getSubNodesSimple(self, queryPattern):
        assert not '*' in queryPattern, "getSubNodesSimple() cannot handle glob/regex patterns."
        self.log.debug("metricType.getSubNodesSimple(): qp = %s" % (qp))
        rec = self.getByName(queryPattern)
        if not rec:
            self.log.debug("metricType.getSubNodesSimple(): RETURNED NONE, qp = %s" % (qp))
            return []
        namelist = rec['children'] 
        self.log.debug("metricType.getSubNodesSimple(): child id list: %s" % (namelist))
        doclist = self.getByNames(namelist)
        self.log.debug("metricType.getSubNodesSimple(): Returning, result of query '%s', docs: %s" % (qp, [x.get('mname', "NOName!") for x in doclist]))
        return doclist

    def splitOnStars(self, queryPattern):
        qpSplit = queryPattern.split('.')
        starNum = None
        for (num, elem) in enumerate(qpSplit):
            if self.regexOrGlobCharsInString(elem):
                starNum = num
                break
        if starNum is None:
            return queryPattern, None, None
        first = '.'.join(qpSplit[0:starNum])
        star  = qpSplit[starNum]
        last  = '.'.join(qpSplit[starNum+1:])
        return first, star, last

    def getSubNodesNonRegex(self, queryPattern):
        self.log.debug("getSubNodesNonRegex(): Getting subnodes with query pattern: %s" %queryPattern)
        first, star, last = self.splitOnStars(queryPattern)
        firstNodes = self.getSubNodesSimple(first)
        star = star.replace('*', ".*")
        star = star.replace('?', ".")
        reStar = re.compile(star)
        fnList = []
        for n in firstNodes:
            if reStar.match(n['metricname'], 1):
                fnList.append(n)
        retList = []
        specificMnames = []
        for n in fnList:
            qstring = n['metricname'] 
            if last:
                qstring += '.' + last
            if '*' in last:
                retList.extend(self.getSubNodesNonRegex(qstring))
            else:
                specificMnames.append(qstring)
        subs = self.getByNames(specificNnames)
        retList.extend(subs)
        return retList
        
    def getParentMetricName(self, metricName):
        assert metricName, "must supply metricname"
        sp = metricName.split('.')
        if (len(sp) == 1):
            return None
        return '.'.join(sp[0:-1])

    def writeConcernThisTime(self):
        self.lastCheckUpdateRetval  += 1
        retval = 0
        if self.lastCheckUpdateRetval > self.checkUpdateRetvalEvery:
            self.lastCheckUpdateRetval = 0
            retval = 1
        return retval


    def getByParentName(self, parentname):
        if not parentname:
            parentname = 'root'
        rows = self.session.execute('select * from metrictype where parentname = %s', [parentname])
        if not rows:
            return []
        return rows

    def setChildrenForMetricName(self, mname, children):
        sql = 'update metrictype set children = %s where metricname = %s'
        self.log.debug("setChildrenForMetricName(): sql: %s" % (sql))
        retval = self.session.execute(sql, [children, mname])
        self.log.debug("setChildrenForMetricName(): update retval: %s." % (retval))
        return

    def fixChildren(self, metricName, verbose=False):
        # self.log.debug("metricType.fixChildren(): fixing children of metricName: %s." % (metricName))
        mtrec = self.getByName(metricName, saveNew=False)  # cannot saveNew or will recurse.
        if not mtrec:
            self.log.warning("WARNING: could not find MT rec for metric name=%s." % (metricName))
            return False
        try:
            mname   = mtrec.get('metricname')
            curList = mtrec.get('children', [])
            curSet  = set(curList)
            parentname = self.getParentMetricName(mname)
            if not parentname:
                parentname = 'root'
            docs    = self.getByParentName(mname)
            self.log.info("fixChildren(): Name: %s found recs with parent name %s of %s" % (mname, parentname, docs))
            childNamesSet = set()
            for d in docs: 
                name = d.get('metricname')
                childNamesSet.add(name)
            self.log.info("fixChildren(): Name: %s, children current: %s, shouldbe: %s" % (mname, curSet, childNamesSet)) 
            if (curSet != childNamesSet):
                self.setChildrenForMetricName(mname, childNamesSet)
                self.invalidateCache(mname)   # important
                justUpdated = self.getByName(mname, saveNew=True)
                self.addDocToCache(mname, justUpdated)
        except:
            self.log.warning("EXCEPTION: processing childlist for metrictype rec: %s, ERROR: %s" % (mtrec, traceback.format_exc()))
        return False

    def updateParentsChildren(self, metricname):
        parentName = self.getParentMetricName(metricname)
        self.fixChildren(parentName)
        return

    def isBadMetricName(self, metricname):
        if not metricname:
           return "Must provide metricname of nonzero length, got empty string."
        sp = metricname.split('.')
        for elem in sp:
            if not elem:
                return "must not have empty section in metricname, got '%s', split into: '%s'" % (metricname, sp)
        if not sp[0]:
            return "must not have leading dot in metricname, got '%s'" % (metricname)
        if len(sp) < 3:
            return "metricname must have at least 3 elements separated by a dot, got '%s'" % (metricname)
        if ' ' in metricname:
            return "metricname must not contain embedded spaces, got '%s'" % (metricname)
        bad_chars = self.badCharPattern.findall(metricname)
        if bad_chars:
            return "metricname contains invalid characters (%s), got '%s'" % (bad_chars, metricname)
        return None

    def createMetricTypeDoc(self, metricname, okay2createRoot=False):
        #self.log.debug("metricType.createMetricTypeDoc(): called with metricname='%s'" % (metricname))
        if not okay2createRoot:
            errMsg = self.isBadMetricName(metricname)
            if errMsg:
                raise BadMetricNameException(errMsg)
        try:
        
            found = self.getByName(metricname, saveNew=False)
            if found: # already exists, don't duplicate.
                return found
            parentMetricName = self.getParentMetricName(metricname)
            self.log.debug("parent of name %s is %s" % (metricname, parentMetricName))
            if parentMetricName:  # not root
                parentMetricTypeDoc = self.createMetricTypeDoc(parentMetricName, okay2createRoot=True)
            else:
                parentMetricName = 'root'
            newDoc = {}
            newDoc['metricname'         ] = metricname
            newDoc['parentname'         ] = parentMetricName
            newDoc['children'           ] = []
            newDoc['linktometricname'   ] = None
            newDoc['expiry'             ] = None
            self.log.info("CreateMetricTypeDoc(): doc created: %s" % (newDoc))
            self.insertMetricTypeDocument(newDoc)
            self.log.info("CreateMetricTypeDoc(): inserted doc w/ name: %s" % (metricname))
            self.addDocToCache(metricname, newDoc)
            self.fixChildren(parentMetricName)
        except Exception as e:
            tb = traceback.format_exc()
            self.log.error("createMetricTypeDoc(): exception:  e: %s, tb: %s" % (e, tb))
            newDoc = None
        return newDoc

    def insertMetricTypeDocument(self, doc):
        query = '''INSERT INTO metrictype (metricname, parentname) VALUES (%(metricname)s, %(parentname)s )'''
        self.log.info("insertMetricTypeDocument(): query: %s, doc: %s" % (query, doc))
        retval = self.session.execute(query, doc)
        self.log.info("retval: %s" % (retval))

    def deleteMetricTypeDocuments(self, mnames):
        query = '''DELETE FROM metrictype WHERE metricname IN %s'''        
        self.log.info("deleteMetricTypeDocuments(): query: %s, list: %s" % (query, mnames))
        retval = self.session.execute(query, parameters=[ValueSequence(mnames)])
        self.log.info("deleteMetricTypeDocuments(): retval: %s" % (retval))
        return
        
        

###############################################################################
###############################################################################
###############################################################################
###############################################################################
