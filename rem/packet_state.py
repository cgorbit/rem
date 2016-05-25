
class PacketState(object):
    UNINITIALIZED = 'IS_UNINITIALIZED' # jobs, files and queue not set
    TAGS_WAIT     = 'IS_TAGS_WAIT'
    PREV_EXECUTOR_STOP_WAIT = 'IS_PREV_EXECUTOR_STOP_WAIT'
    PENDING       = 'IS_PENDING'
    RUNNING       = 'IS_RUNNING'
    PAUSED        = 'IS_PAUSED'
    PAUSING       = 'IS_PAUSING'
    TIME_WAIT     = 'IS_TIME_WAIT'
    SUCCESSFULL   = 'IS_SUCCESSFULL'
    ERROR         = 'IS_ERROR'
    BROKEN        = 'IS_BROKEN'
    DESTROYING    = 'IS_DESTROYING'
    HISTORIED     = 'IS_HISTORIED'
