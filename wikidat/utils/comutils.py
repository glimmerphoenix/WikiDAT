# -*- coding: utf-8 -*-
"""
Created on Thu May 29 00:33:50 2014

@author: jfelipe
"""
import ujson


def send_ujson(socket, obj, flags=0):
    """Serialize object using ultra-fast ujson"""
    return socket.send(ujson.dumps(obj), flags=flags)


def recv_ujson(socket, flags=0):
    """Load object from ujson serialization"""
    obj = socket.recv(flags)
    return ujson.loads(obj)