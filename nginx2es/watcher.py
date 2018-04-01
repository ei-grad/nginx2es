from time import sleep
import logging
import os

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

    def __init__(self, filename, from_start=False, teardown_timeout=10.):
        self.filename = filename
        self.from_start = from_start
        self.teardown_timeout = teardown_timeout
        self.inotify = INotify()
        self.inotify.add_watch(self.filename, flags.MODIFY | flags.MOVE_SELF)

    def __iter__(self):
        while True:
            with open(self.filename, errors='replace') as f:
                try:
                    for i in self.watch(f):
                        yield i
                except:
                    logging.error("exception in watcher, current file position: %d",
                                  f.tell(), exc_info=True)
                    raise

    def watch(self, f):
        # rewind to end of the file if needed
        if not self.from_start:
            f.seek(0, os.SEEK_END)
            # new files should always be readed from start
            self.from_start = True
        # read current contents of file
        for i in yield_until_eof(f):
            yield i
        for i in self.yield_until_moved(f):
            yield i
        # wait some time for nginx processes to flush logs to current file
        sleep(self.teardown_timeout)
        for i in yield_until_eof(f):
            yield i

    def yield_until_moved(self, f):
        moved = False
        while not moved:
            events = self.inotify.read()
            for event in events:
                if event.mask & flags.MOVE_SELF:
                    logging.info('file has been moved')
                    moved = True
                elif event.mask & flags.MODIFY:
                    for i in yield_until_eof(f):
                        yield i
                else:
                    raise Exception("Shouldn't happen")
