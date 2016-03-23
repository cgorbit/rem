import os
import time
import codecs
import thread
import rem.osspec
import logging
import logging.handlers

logger = logging.getLogger('rem_server')

if rem.osspec.gettid():
    gettid = rem.osspec.gettid
else:
    gettid = thread.get_ident


class LogFormatter(logging.Formatter):
    def format(self, record):
        record.thread_id = gettid()
        return logging.Formatter.format(self, record)


class StableRotateFileHandler(logging.handlers.TimedRotatingFileHandler):
    REOPEN_TM = 60

    def __init__(self, filename, **kws):
        logging.handlers.TimedRotatingFileHandler.__init__(self, filename, **kws)
        self.lastReopen = time.time()

    def shouldRollover(self, record):
        if time.time() - self.lastReopen > self.REOPEN_TM:
            self.stream.close()
            if self.encoding:
                self.stream = codecs.open(self.baseFilename, "a", self.encoding)
            else:
                self.stream = open(self.baseFilename, "a")
            lastReopen = time.time()
        return logging.handlers.TimedRotatingFileHandler.shouldRollover(self, record)


def _create_logger(ctx, log_to_file):
    log_level = getattr(logging, ctx.log_warn_level.upper())

    logger = logging.getLogger('rem_server')
    logger.handlers = [] # oops
    logger.propagate = False

    if log_to_file:
        logHandler = StableRotateFileHandler(
            os.path.join(ctx.log_directory, ctx.log_filename),
            when="midnight", backupCount=ctx.log_backup_count)
        logHandler.setFormatter(LogFormatter("%(asctime)s %(thread_id)s %(levelname)-8s %(module)s:\t%(message)s"))
        logger.addHandler(logHandler)
    else:
        logger.addHandler(logging.StreamHandler())

    logger.setLevel(log_level)

    return logger

def reinit_logger(ctx, log_to_file=True):
    global logger
    logger = _create_logger(ctx, log_to_file)