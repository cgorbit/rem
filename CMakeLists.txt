OWNER(trofimenkov)

RECURSE(
    lib
    client
)

IF (LINUX)
    RECURSE(
        pgrpguard
    )
ENDIF ()
