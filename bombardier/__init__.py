#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  bombardier.py
#
#  Copyright 2018 Jelle Smet <development@smetj.net>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#
#


from gevent import monkey

monkey.patch_all()

import gipc
from gevent import pool, sleep, queue
from itertools import cycle
import requests
import logging
import sys
import argparse


class Poison:
    pass


class Bombardier:
    """
    A framework to parallelize Gevent functions over multiple processes.

    Args:
        processes (int): The number of parallel processes.
        greenthreads (int): The number of greenthreads per process.

    """

    def __init__(self, processes=1, greenthreads=1):

        self.greenthreads = greenthreads
        self.processes = processes
        self.comms = []
        self.pool = pool.Pool()

        self.__setupProcesses()
        self.cycle = cycle(self.comms)

    def stop(self):

        for _, writer in self.comms:
            writer.put(Poison())
        self.pool.join()

    def submit(self, func, func_args):
        _, writer = next(self.cycle)
        writer.put((func, func_args))

    def sessionLauncher(self, reader, size):

        p = pool.Pool(size)
        logger = self.__setupLogging()
        while True:
            item = reader.get()
            if isinstance(item, Poison):
                # log message that poison pill is rec
                break
            else:
                func, func_args = item
                result = p.spawn(func, logger, *func_args)

    def processLauncher(self):

        with gipc.pipe() as (r, w):
            self.comms.append((r, w))
            p = gipc.start_process(
                target=self.sessionLauncher, args=((r, self.greenthreads))
            )
            p.join()
            # log message that process x has ended

    def __setupLogging(self):
        b = logging.getLogger("bombardier")
        b.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - pid[%(process)d] - %(funcName)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        b.addHandler(handler)
        return b

    def __setupProcesses(self):

        for _ in range(0, self.processes):
            self.pool.spawn(self.processLauncher)
            sleep(1)
