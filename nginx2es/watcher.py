from time import time
import logging
import os

import click

from inotify_simple import INotify, flags


def yield_until_eof(f):
    stat = os.stat(f.fileno())
    while True:
        pos = f.tell()
        if pos == stat.st_size:
            break
        line = f.readline()
        if line:
            # only a part of the line is written to file
            if line[-1] != '\n':
                # restore previous position (before readline)
                f.seek(pos)
                # exit
                break
            yield line


class Watcher(object):

    def __init__(self, filename, from_start=False):
        self.filename = filename
        self.from_start = from_start

    def __iter__(self):
        while True:
            for i in self.watch():
                yield i

    def watch(self):

        f = click.open_file(self.filename, errors='replace')

        # rewind to end of the file if not asked to start from begin
        if not self.from_start:
            f.seek(0, 2)

        try:
            with INotify() as inotify:
                inotify.add_watch(self.filename, flags.MODIFY | flags.MOVE_SELF)
                for i in yield_until_eof(f):
                    yield i
                for i in self.watch_until_closed(f, inotify):
                    yield i
        except:
            logging.error("exception in watcher, current file position: %d",
                          f.tell(), exc_info=True)
            raise

        f.close()

    def watch_until_closed(self, f, inotify):
        moved = False
        wait_teardown = None
        while True:
            if moved:
                if wait_teardown is None:
                    wait_teardown = time() + 10.
                else:
                    if wait_teardown > time():
                        break
            events = inotify.read(1000)
            for event in events:
                if event.mask & flags.MOVE_SELF:
                    logging.info('file has been moved')
                    moved = True
                elif event.mask & flags.MODIFY:
                    for i in yield_until_eof(f):
                        yield i
                else:
                    raise Exception("Shouldn't happen")
