
class ReprState(object):
    CREATED = "CREATED"
    WORKABLE = "WORKABLE"               # working packet without pending jobs
    PENDING = "PENDING"                 # working packet with pending jobs (may have running jobs)
    SUSPENDED = "SUSPENDED"             # waiting for tags or manually suspended (may have running jobs)
    ERROR = "ERROR"                     # unresolved error exists
    SUCCESSFULL = "SUCCESSFULL"         # successfully done packet
    HISTORIED = "HISTORIED"             # temporary state before removal
    WAITING = "WAITING"                 # wait timeout for retry failed jobs (may have running jobs)
    NONINITIALIZED = "NONINITIALIZED"
    # src -> dst
    #allowed = {
        #CREATED: (WORKABLE, SUSPENDED),
        #WORKABLE: (SUSPENDED, ERROR, SUCCESSFULL, PENDING, WAITING, NONINITIALIZED),
        #PENDING: (SUSPENDED, WORKABLE, ERROR, WAITING, NONINITIALIZED),
        #SUSPENDED: (WORKABLE, HISTORIED, WAITING, ERROR, NONINITIALIZED),
        #WAITING: (PENDING, SUSPENDED, ERROR, NONINITIALIZED),
        #ERROR: (SUSPENDED, HISTORIED, NONINITIALIZED),
        #SUCCESSFULL: (HISTORIED, NONINITIALIZED),
        #NONINITIALIZED: (CREATED,),
        #HISTORIED: ()}


