# -*- coding: utf-8 -*-
"""
Created on Sat Mar 29 22:15:39 2014

@author: jfelipe, Steven F. Lott
Special credits (and thanks) to Steven F. Lott for the inspiration to create
a simple (but effective) structure for fan-out/fan-in using the
multiprocessing module in Python. Original sources and examples can be found
on the following blog posts:
http://slott-softwarearchitect.blogspot.com.es/2012/02/
multiprocessing-goodness-part-1-use.html
http://slott-softwarearchitect.blogspot.com.es/2012/02/
multiprocessing-goodness-part-2-class.html
"""

import multiprocessing as mp
from page import Page
from revision import Revision
# from logitem import LogItem
# from user import User


class Producer(mp.Process):
    """
    Produces items into a Queue.

    The "target" must be a generator function which yields
    pickable items.

    The example has been modified to support two output queues, one for
    page and another one for revision elements
    """
    def __init__(self, group=None, target=None, name=None, args=None,
                 kwargs=None, out_page_queue=None,
                 out_rev_queue=None, out_logitem_queue=None,
                 out_user_queue=None,
                 page_consumers=0, rev_consumers=0, logitem_consumers=0,
                 user_consumers=0):

        super(Producer, self).__init__(name=name)
        self.target = target
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.out_page_queue = out_page_queue
        self.out_rev_queue = out_rev_queue
        self.out_logitem_queue = out_logitem_queue
        self.out_user_queue = out_user_queue
        self.page_consumers = page_consumers
        self.rev_consumers = rev_consumers
        self.logitem_consumers = logitem_consumers
        self.user_consumers = user_consumers

    def run(self):
        target = self.target
        for item in target(*self.args, **self.kwargs):
            # Classify outcome elements in their corresponding queue
            # for later processing
            if item['item_type'] == 'page':
                if self.out_page_queue is not None:
                    self.out_page_queue.put(item)

            elif item['item_type'] == 'revision':
                if self.out_rev_queue is not None:
                    self.out_rev_queue.put(item)

#            elif isinstance(item, LogItem):
#                if self.out_logitem_queue is not None:
#                    self.out_logitem_queue.put(item)
#
#            elif isinstance(item, User):
#                if self.out_user_queue is not None:
#                    self.output_user_queue.put(item)

        # Introduce poison pills for every consumer in each active queue
        if self.page_consumers > 0:
            for x in range(self.page_consumers):
                self.out_page_queue.put(None)
            self.out_page_queue.close()

        if self.rev_consumers > 0:
            for x in range(self.rev_consumers):
                self.out_rev_queue.put(None)
            self.out_rev_queue.close()

#        if self.logitem_consumers > 0:
#            for x in range(self.logitem_consumers):
#                self.out_logitem_queue.put(None)
#            self.out_logitem_queue.close()
#
#        if self.user_consumers > 0:
#            for x in range(self.user_consumers):
#                self.output_user_queue.put(None)
#            self.output_user_queue.close()


class Consumer(mp.Process):
    """
    Consumes items from a Queue.

    The "target" must be a function which expects an iterable as it's
    only argument.  Therefore, the args value is not used here.
    """
    def __init__(self, group=None, target=None, name=None, args=None,
                 kwargs=None, input_queue=None, producers=0):

        super(Consumer, self).__init__(name=name)
        self.target = target
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.input_queue = input_queue
        self.producers = producers

    def items(self):
        while self.producers != 0:
            for item in iter(self.input_queue.get, None):
                yield item
                self.input_queue.task_done()
            self.input_queue.task_done()
            self.producers -= 1

    def run(self):
        target = self.target
        target(self.items(), **self.kwargs)


class Processor(mp.Process):
    """
    Consumes items from a Queue and yield processed items to another Queue

    The "target" must be a generator function which yields
    pickable items derived from DataItems and which expects an iterable as its
    only argument.  Therefore, the args value is not used here.
    """
    def __init__(self, group=None, target=None, name=None, args=None,
                 kwargs=None, input_queue=None, producers=0,
                 output_queue=None, consumers=0):
        super(Processor, self).__init__(name=name)
        self.target = target  # String with method name, not method itself
        #self.args= args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.input_queue = input_queue
        self.producers = producers
        self.output_queue = output_queue
        self.consumers = consumers

    def items(self):
        while self.producers != 0:
            for item in iter(self.input_queue.get, None):
                yield item
                self.input_queue.task_done()

            self.input_queue.task_done()
            self.producers -= 1

    def run(self):
        target = self.target

        for item in target(self.items(), **self.kwargs):
            self.output_queue.put(item)

        for x in range(self.consumers):
            self.output_queue.put(None)

        self.output_queue.close()
