## The Leader Election

Leader election is the process of designating a single process as the organizer of some task distributed among several computers (nodes).

## Multithreading

- One node has 2 roles :
  a server (wait for predecessor to connect - accept())
  a client (connect to successor - connect())

## How to run the program?

Open 3 terminals and run each command in one of them

- Terminal 1
  python3 myleprocess.py config1.txt

- Terminal 2
  python3 myleprocess.py config2.txt

- Terminal 3
  python3 myleprocess.py config3.txt

**Output**
Each node prints its own UUID on startup.
Each node writes a full log of every message it sends/receives/ignores to <node_uuid>.txt in the current directory.
Once the leader is determined, every node logs the leader's UUID and exits.
