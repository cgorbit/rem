#! /usr/bin/env python

import rem_server

if __name__ == "__main__":
    #rem_server.main()
    import rem.sandbox

    sbx = rem.sandbox.Client('http://localhost:34567/api/v1.0/', debug=True, timeout=10.0)

    sbx.list_task_resources(119)

