from time import sleep
import logging
import os

from inotify_simple import INotify, flags


class Watcher(object):

    def __init__(self, filename, from_start=False, teardown_timeout=10.):
        self.filename = filename
        self.from_start = from_start
        self.teardown_timeout = teardown_timeout
        self.remainder = ''

    def __iter__(self):
        self.remainder = ''
        while True:
            with open(self.filename, errors='replace') as f:
                with INotify() as inotify:
                    inotify.add_watch(self.filename, flags.MODIFY | flags.MOVE_SELF)
                    yield from self.watch(f, inotify)

    def watch(self, f, inotify):

        if self.from_start:
            # read current contents of file
            yield from self.yield_until_eof(f)
        else:
            # rewind to end of the file if needed
            f.seek(0, os.SEEK_END)
            # new files should always be readed from start
            self.from_start = True

        # loop over inotify events until file would be moved
        yield from self.yield_until_moved(f, inotify)

        # wait some time for nginx processes to flush logs to current file
        sleep(self.teardown_timeout)

        # read logs written by nginx processes after the file has been moved
        yield from self.yield_until_eof(f)

    def yield_until_eof(self, f):

        if self.remainder:
            self.remainder += f.readline()
            if self.remainder[-1] == '\n':
                line, self.remainder = self.remainder, ''
                yield line
            else:
                raise StopIteration()

        while True:
            line = f.readline()
            if not line:
                # got to the end of file
                break
            elif line[-1] != '\n':
                # got to the end of file, but the last line is truncated
                self.remainder = line
                break
            else:
                yield line

    def yield_until_moved(self, f, inotify):
        moved = False
        while not moved:
            events = inotify.read()
            for event in events:
                if event.mask & flags.MOVE_SELF:
                    logging.info('file has been moved %s', event)
                    moved = True
                elif event.mask & flags.MODIFY:
                    yield from self.yield_until_eof(f)
                else:
                    raise Exception("Shouldn't happen")
