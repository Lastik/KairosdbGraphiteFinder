#!/usr/bin/env python2.6
################################################################################

# Class for self-reporting metrics.  Handles conversion to
# rate unless disabled via set_rate("metric_name", False)

import time
from pprint import pprint, pformat

###############################################################################

class CounterStats(object):

    def __init__(self, names=[], *nameArgs):
        self._t0 = self.getNow()
        self._names = list(nameArgs)
        self._names.extend(names)
        self._is_rate = {}
        for name in self._names:
            setattr(self, name, 0)
            self._is_rate[name] = True

    def setAllNonRate(self):
        for name in self._names:
            self._is_rate[name] = False
        return

    def setIsRateOnNames(self, namesList, isRate=False):
        for name in namesList:
            if name not in self._names:
                continue
            self._is_rate[name] = isRate
        return

    def addStatName(self, name, is_rate=True):
        self._names.append(name)
        if getattr(self, name, None) is None:
            setattr(self, name, 0)
            self._is_rate[name] = is_rate
        return

    def addStatNames(self, statNames, is_rate=True):
        for name in statNames:
            self.addStatName(name, is_rate=is_rate)
        return

    def setRate(self, name, val=True):
        return self.set_rate(name, val=val)

    def set_rate(self, name, val=True):
        self._is_rate[name] = val

    def is_rate(self, name):
        return self._is_rate[name]

    def getNow(self):
        ''' overridden for unit tests. '''
        return time.time()
        
    def __iter__(self):
        '''  Returns a iterable of (name, value) pairs '''
        now = self.getNow()
        dt = now - self._t0
        for name in self._names:
            val = getattr(self, name)
            if self._is_rate[name]:
                if dt == 0: # no div by zero. Time always passes, but check.
                    continue
                val = val / dt
                name += 'PerSec'
            yield (name, val)
                
    def reset(self):
        '''  All rate counters are reset to 0.  others left alone.'''
        self._t0 = self.getNow()
        for name in self._names:
            if self._is_rate[name]:
                setattr(self, name, 0)

    def resetNonRate(self):
        '''  All non-rate counters are reset to 0. others left alone.'''
        self._t0 = self.getNow()
        for name in self._names:
            if not self._is_rate[name]:
                setattr(self, name, 0)

    def __str__(self):
        return str([x for x in self])

    def get_names_as_string(self):
        return ", ".join(self._names)

    def get_names(self):
        return self._names

###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
