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
            yield stat.st_ino, pos, line


class Watcher(object):

    def __init__(self, filename, from_start=False):
        self.filename = filename
        self.from_start = from_start
        self.last_inode = None
        self.last_pos = None

    def __iter__(self):
        while True:
            for i in self.watch():
                yield i

    def watch(self):

        f = open(self.filename)
        inode = os.stat(f.fileno()).st_ino

        # if this is not the first watch pass
        if self.last_inode is not None:
            # seek to last position if file is not changed
            if inode == self.last_inode:
                f.seek(self.last_pos)
        # if it is the first watch pass
        else:
            # rewind to end of the file if not asked to start from begin
            if not self.from_start:
                f.seek(0, 2)

        with INotify() as inotify:
            logging.info("starting watch on %s (inode %d)", self.filename, inode)
            inotify.add_watch(self.filename, flags.MODIFY | flags.CLOSE_WRITE)
            for i in yield_until_eof(f):
                yield i
            for i in self._watch_until_closed(f, inotify):
                yield i
            logging.info("finished watch on %s (inode %d)", self.filename, inode)

        self.last_inode = inode
        self.last_pos = f.tell()

        f.close()

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
