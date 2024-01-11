#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2009 Christopher Lenz
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.

"""
DEPRECATED
"""

from couchdb import exceptions


__all__ = ['ResourceNotFound', 'ResourceConflict']


class ResourceNotFound(exceptions.MissingDatabase, exceptions.MissingDocument):
    """Exception raised when a 404 HTTP error is received in response to a
    request.
    """


class ResourceConflict(exceptions.UpdateConflict):
    """Exception raised when a 409 HTTP error is received in response to a
    request.
    """
