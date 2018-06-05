#!/usr/bin/env python

def get_user_password(host, port):
    """Given the (host, port) of the admin endpoint, returns a tuple (user, password)."""
    return ("admin", "admin")

def get_host_ports():
    """Return a list of (host, port) ProxySQL admin endpoints."""
    return [
        ('127.0.0.1', 6032),
        ('127.0.0.1', 6033),
    ]
