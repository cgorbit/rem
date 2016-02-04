OWNER(trofimenkov)

RECURSE(
    lib
)

IF (LINUX)
    RECURSE(
        pgrpguard
    )
ENDIF ()
