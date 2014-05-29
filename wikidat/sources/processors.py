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

import time
import multiprocessing as mp
import zmq
from wikidat.utils.comutils import send_ujson, recv_ujson
# from page import Page
# from revision import Revision
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
                 kwargs=None, page_consumers=0, rev_consumers=0,
                 logitem_consumers=0, user_consumers=0,
                 push_pages_port=None, push_revs_port=None):

        super(Producer, self).__init__(name=name)
        self.target = target
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.page_consumers = page_consumers
        self.rev_consumers = rev_consumers
        self.logitem_consumers = logitem_consumers
        self.user_consumers = user_consumers
        self.push_pages_port = push_pages_port
        self.push_revs_port = push_revs_port

    def run(self):
        target = self.target

        context = zmq.Context()
        # Set up sending channel for page elements
        channel_pages_send = context.socket(zmq.PUSH)
        channel_pages_send.bind("tcp://127.0.0.1:" + str(self.push_pages_port))

        # Set up sending channel for revision elements
        channel_revs_send = context.socket(zmq.PUSH)
        channel_revs_send.bind("tcp://127.0.0.1:" + str(self.push_revs_port))

        # Wait a second to wake up and connect
        time.sleep(1)

        for item in target(*self.args, **self.kwargs):
            # Classify outcome elements in their corresponding queue
            # for later processing
            if item['item_type'] == 'page':
                send_ujson(channel_pages_send, item)

            elif item['item_type'] == 'revision':
                send_ujson(channel_revs_send, item)

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
                # Send poison pills to page workers
                send_ujson(channel_pages_send, None)
            # Close pages downstream channel
            channel_pages_send.close()

        if self.rev_consumers > 0:
            for x in range(self.rev_consumers):
                # Send poison pills to revision workers
                send_ujson(channel_revs_send, None)
            # Close revisions downstream channel
            channel_revs_send.close()


class Consumer(mp.Process):
    """
    Consumes items from a Queue.

    The "target" must be a function which expects an iterable as it's
    only argument.  Therefore, the args value is not used here.
    """
    def __init__(self, group=None, target=None, name=None, args=None,
                 kwargs=None, producers=0, pull_port=None):

        super(Consumer, self).__init__(name=name)
        self.target = target
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.producers = producers
        self.pull_port = pull_port

    def items(self):
        context = zmq.Context()
        channel_receiver = context.socket(zmq.PULL)
        channel_receiver.bind("tcp://127.0.0.1:"+str(self.pull_port))

        # Wait a second to wake up and connect
        time.sleep(1)

        while self.producers != 0:
            item = recv_ujson(channel_receiver)
            while item is not None:
                yield item
                item = recv_ujson(channel_receiver)
            self.producers -= 1

        channel_receiver.close()

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
                 kwargs=None, producers=0, consumers=0,
                 pull_port=None, push_port=None):
        super(Processor, self).__init__(name=name)
        self.target = target  # String with method name, not method itself
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.producers = producers
        self.consumers = consumers
        self.pull_port = pull_port
        self.push_port = push_port

    def items(self):
        context = zmq.Context()
        channel_receiver = context.socket(zmq.PULL)
        channel_receiver.connect("tcp://127.0.0.1:"+str(self.pull_port))

        # Wait a second to wake up and connect
        time.sleep(1)

        while self.producers != 0:
            item = recv_ujson(channel_receiver)
            while item is not None:
                yield item
                item = recv_ujson(channel_receiver)
            self.producers -= 1

        channel_receiver.close()

    def run(self):
        target = self.target
        context = zmq.Context()
        channel_send = context.socket(zmq.PUSH)
        channel_send.connect("tcp://127.0.0.1:" + str(self.push_port))

        # Wait a second to wake up and connect
        time.sleep(1)

        for item in target(self.items(), **self.kwargs):
            send_ujson(channel_send, item)

        for x in range(self.consumers):
            send_ujson(channel_send, None)

        channel_send.close()
