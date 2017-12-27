import logging
import os

from inotify_simple import INotify, flags


def yield_until_eof(f):
    stat = os.stat(f.fileno())
    while True:
        pos = f.tell()
        if pos == stat.st_size:
            break
        # exit if only a part of the line is written to file
        line = f.readline()
        if line[-1] != '\n':
            f.seek(pos)
            break
        yield stat.st_ino, pos, line


class Watcher(object):

    def __init__(self, filename, last_inode=None, last_pos=None):
        self.filename = filename
        self.last_inode = last_inode
        self.last_pos = last_pos

    def __iter__(self):
        while True:
            for i in self.watch():
                yield i

    def watch(self):

        logging.debug("starting watch")

        f = open(self.filename)

        inode = os.stat(f.fileno()).st_ino
        if inode == self.last_inode:
            f.seek(self.last_pos)
        else:
            self.last_inode = inode

        for i in yield_until_eof(f):
            yield i

        with INotify() as inotify:
            inotify.add_watch(self.filename, flags.MODIFY | flags.CLOSE_WRITE)
            for i in self._watch_until_closed(f, inotify):
                yield i

        self.last_pos = f.tell()

    def _watch_until_closed(self, f, inotify):
        closed = False
        while not closed:
            events = inotify.read()
            for event in events:
                if event.mask & flags.CLOSE_WRITE:
                    logging.debug('got CLOSE_WRITE')
                    closed = True
                elif event.mask & flags.MODIFY:
                    logging.debug('got MODIFY')
                    for i in yield_until_eof(f):
                        yield i
                else:
                    raise Exception("Shouldn't happen")
