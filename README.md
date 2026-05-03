# Proxy Server With Cache

A multithreaded HTTP proxy server written in C/C++ with an in-memory LRU cache for faster response delivery.

---

## Overview

This project implements a threaded HTTP proxy server that:

* Accepts client connections on a user-defined port
* Parses incoming HTTP requests
* Forwards valid requests to the origin server
* Relays responses back to the client

To improve performance, the proxy maintains an in-memory cache using a **Least Recently Used (LRU)** eviction policy, allowing repeated requests to be served without contacting the remote server.

---

## Features

* Multithreaded client handling
* Supports HTTP `GET` requests
* Compatible with HTTP `1.0` and `1.1`
* In-memory caching with size limits
* LRU-based cache eviction
* Standard HTTP error handling for invalid requests

---

## Project Structure

* `proxy_server_with_cache.c` — Main proxy server, request forwarding, and cache logic
* `proxy_parse.c`, `proxy_parse.h` — HTTP request parsing utilities
* `Makefile` — Build, clean, and packaging commands
* `notes.txt` — Background notes on HTTP and socket concepts

---

## Build Instructions

Compile the project using:

```bash
make
```

This generates the executable:

```bash
proxy
```

### Clean build files

```bash
make clean
```

### Create submission archive

```bash
make tar
```

---

## Usage

Run the proxy server by specifying a port:

```bash
./proxy <port>
```

### Example

```bash
./proxy 8080
```

The proxy will start listening on the given port and log connection activity to the console.

---

## Testing the Proxy

You can test the proxy using tools like `curl`:

```bash
curl -x http://127.0.0.1:8080 http://example.com/
```

Or configure your browser to use:

```
localhost:<port>
```

as the HTTP proxy.

---

## Future Improvements

* Support additional HTTP methods (`POST`, `PUT`, etc.)
* Add HTTPS support via `CONNECT` tunneling
* Improve thread-safe cache synchronization
* Introduce configurable cache size and TTL-based expiration
* Enhance logging and debugging capabilities
* Add configuration support (thread limits, upstream proxies, etc.)
* Implement unit and integration tests

---

## Limitations

* Currently supports only `GET` requests
* No HTTPS support yet
* Cache is in-memory only (not persistent)
* Limited configurability

---

## Notes

This project is intended for learning purposes and demonstrates core concepts in:

* Socket programming
* Multithreading
* HTTP request parsing
* Caching strategies

---
