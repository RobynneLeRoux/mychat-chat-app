# MyChat: Real-Time Chat Application

A client–server chat app in **Python** with direct and group messaging, typing indicators and peer-to-peer file transfer, built on a **custom text-based application-layer protocol** and a terminal UI.

## Demo
[▶ Watch my prototype in action](demo/mychat_prototype_demo.mp4) (5 min): my threaded, protocol-first prototype running a server and several clients side by side: registering, logging in, `PING`/`PONG` heartbeats and messaging between users.

## Features
- Register, log in, create, join and leave public group chats
- Direct and group messaging in real time over **TCP**
- Lightweight "is typing…" indicators over **UDP** (packet loss doesn't matter here, so UDP's speed wins)
- **Peer-to-peer** file transfer: the sender opens a temporary TCP server and the recipient downloads directly, so files never pass through the chat server
- Terminal user interface built with **Textual**; concurrency with **asyncio** on both client and server

## The protocol
Every message has a category, an action, headers and an optional body:

```
<CATEGORY> <ACTION>\r\n
<header> <value>\r\n
ContentLength <n>\r\n\r\n
<body>
```

| Category | Meaning |
|---|---|
| `CMD` | A client asks the server to do something (`LOGIN`, `CREATE`, `JOIN`, `LEAVE`) |
| `CTL` | The server's direct reply to a `CMD` (success flag, data or error) |
| `INF` | The server informs other clients that something happened |
| `DATA` | Chat content (`SEND` for text, `MEDIA` for file offers) |

Example, joining a group: the client sends `CMD JOIN`, the server replies `CTL JOIN` with the current members, then sends `INF JOIN` to everyone already in the group.

**Framing:** TCP is a byte stream with no message boundaries, so every message ends its headers with a blank line and declares a `ContentLength`; the reader reads exactly that many body bytes (`packet.py`).

**Request/response matching:** commands that need a reply carry a `futureId`. The client parks an asyncio `Future` under that ID and the background listener resolves it when the matching `CTL` arrives, so the UI never blocks while waiting.

## Run it
Requires Python 3.11+.

```bash
pip install -r requirements.txt
python3 server.py 8888            # start the server
python3 client.py 127.0.0.1 8888  # start a client (open several terminals)
```

## Files
| File | Purpose |
|---|---|
| `server.py` | asyncio TCP server + UDP typing relay; auth, routing and group management |
| `client.py` | Textual UI, message listener, typing indicators and P2P file transfer |
| `packet.py` | Protocol encoding/decoding and one helper per message type |
| `helpers.py` | `User` and `Chat` data classes |

## My role
- Drafted the full protocol specification
- Built a protocol-first prototype with threads, a `PING`/`PONG` heartbeat and send/print locks that fixed corrupted message framing under concurrent sends
- In the final version, implemented and documented every protocol message type, rebuilt message parsing on asyncio `StreamReader` (`readuntil` / `readexactly`) and added error handling for edge cases across the protocol
- Wrote the design, performance, reliability and security evaluation in the final report

## Limitations and what I'd do next
- Accounts and chats live in server memory and passwords are sent and stored in plain text. Next steps: TLS and hashed passwords in a database.
- P2P transfer assumes both clients are reachable on `127.0.0.1`; across real networks it would need the sender's public address and NAT traversal.

## Team
Robynne Le Roux, Ashley and Laith. The Textual UI framework started from an AI-generated scaffold, declared as required by the course. Built for CSC3002F Networks, University of Cape Town (2026).
