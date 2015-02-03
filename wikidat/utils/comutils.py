# -*- coding: utf-8 -*-
"""
Created on Thu May 29 00:33:50 2014

@author: jfelipe
"""
import ujson
import zlib


def send_ujson(socket, obj, flags=0):
    """Serialize object using ultra-fast ujson"""
    m = ujson.dumps(obj)
    z = zlib.compress(m.encode())
    return socket.send(z, flags=flags)


def recv_ujson(socket, flags=0):
    """Load object from ujson serialization"""
    z = socket.recv(flags)
    m = zlib.decompress(z).decode()
    return ujson.loads(m)
