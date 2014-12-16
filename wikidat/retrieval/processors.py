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

A gentle introduction to ZeroMQ elements and communication patterns is:
http://nichol.as/zeromq-an-introduction

Example code for ZeroMQ patterns can be found in the official guide:
http://zguide.zeromq.org/page:all
"""

import time
import multiprocessing as mp
import zmq
from wikidat.utils.comutils import send_ujson, recv_ujson
from page import Page
from revision import Revision
from logitem import LogItem
# from user import User


class Producer(mp.Process):
    """
    Produces items to be sent downstream to a ZMQ pipeline.
    http://nichol.as/zeromq-an-introduction

    The "target" must be a generator function which yields
    pickable items.

    The example has been modified to support two output queues, one for
    page and another one for revision elements
    """
    def __init__(self, group=None, target=None, name=None, args=None,
                 kwargs=None, consumers=0, push_pages_port=None,
                 push_revs_port=None, push_logs_port=None,
                 control_port=None):

        super(Producer, self).__init__(name=name)
        self.target = target
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.consumers = consumers
        self.push_pages_port = push_pages_port
        self.push_revs_port = push_revs_port
        self.push_logs_port = push_logs_port
        self.control_port = control_port

    def run(self):
        target = self.target

        # Set up sending ZMQ data and control channels
        context = zmq.Context()

        if (self.push_pages_port):
            channel_pages_send = context.socket(zmq.PUSH)
            channel_pages_send.bind("tcp://127.0.0.1:%s" %
                                    self.push_pages_port)

        if (self.push_revs_port):
            channel_revs_send = context.socket(zmq.PUSH)
            channel_revs_send.bind("tcp://127.0.0.1:%s" %
                                   self.push_revs_port)

        if (self.push_logs_port):
            channel_logs_send = context.socket(zmq.PUSH)
            channel_logs_send.bind("tcp://127.0.0.1:%s" %
                                   self.push_logs_port)

        channel_control = context.socket(zmq.PUB)
        channel_control.bind("tcp://127.0.0.1:%s" %
                             self.control_port)

        # Wait a second to wake up and connect
        time.sleep(1)

        for item in target(*self.args, **self.kwargs):
            # Classify outcome elements in their corresponding queue
            # for later processing
            if isinstance(item, Page):
                send_ujson(channel_pages_send, item)

            elif isinstance(item, Revision):
                send_ujson(channel_revs_send, item)

            elif isinstance(item, LogItem):
                send_ujson(channel_logs_send, item)

        # Wait few seconds to let workers empty data pipeline
        time.sleep(20)
        #channel_pages_send.close()
        #channel_revs_send.close()

        # Send STOP message to all workers and quit
        if self.consumers > 0:
            channel_control.send('STOP')

        time.sleep(5)
        #channel_control.close()


class Consumer(mp.Process):
    """
    Consumes items from ZMQ pipeline, coming from Processors.
    http://nichol.as/zeromq-an-introduction

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
        data_recv = context.socket(zmq.PULL)
        data_recv.bind("tcp://127.0.0.1:"+str(self.pull_port))

        # Wait a second to wake up and connect
        time.sleep(1)

        while self.producers > 0:
            while True:
                item = recv_ujson(data_recv)
                if item == 'STOP':
                    break
                yield item
            self.producers -= 1

        time.sleep(1)
        #data_recv.close()

    def run(self):
        target = self.target
        target(self.items(), **self.kwargs)


class Processor(mp.Process):
    """
    Consumes items from a ZMQ pipeline, coming from a Producer and sent them
    downstream to a Consumer.

    The "target" must be a generator function which yields
    pickable items derived from DataItems and which expects an iterable as its
    only argument.  Therefore, the args value is not used here.
    """
    def __init__(self, group=None, target=None, name=None, args=None,
                 kwargs=None, producers=0, consumers=0,
                 pull_port=None, push_port=None, control_port=None):
        super(Processor, self).__init__(name=name)
        self.target = target  # String with method name, not method itself
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.producers = producers
        self.consumers = consumers
        self.pull_port = pull_port
        self.push_port = push_port
        self.control_port = control_port

    def items(self):
        context = zmq.Context()
        data_recv = context.socket(zmq.PULL)
        data_recv.connect("tcp://127.0.0.1:%s" % self.pull_port)

        control_sub = context.socket(zmq.SUB)
        control_sub.connect("tcp://127.0.0.1:%s" % self.control_port)
        control_sub.setsockopt(zmq.SUBSCRIBE, "STOP")

        # Wait a second to wake up and connect
        time.sleep(1)

        # Initialize poll set
        poller = zmq.Poller()
        poller.register(data_recv, zmq.POLLIN)
        poller.register(control_sub, zmq.POLLIN)

        while self.producers > 0:
            # Work on requests from pipelining and control channel
            while True:
                socks = dict(poller.poll())
                if data_recv in socks and socks[data_recv] == zmq.POLLIN:
                    yield(recv_ujson(data_recv))

                if control_sub in socks and socks[control_sub] == zmq.POLLIN:
                    message = control_sub.recv()
                    if message == "STOP":
                        print "Received STOP %s" % self.name
                        break  # Exit poll loop

            self.producers -= 1

        time.sleep(1)
        #data_recv.close()
        #control_sub.close()

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
            send_ujson(channel_send, 'STOP')

        time.sleep(1)
        #channel_send.close()
