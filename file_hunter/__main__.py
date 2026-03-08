import argparse
import logging
import os
import socket
import uvicorn
from file_hunter.config import load_config


def find_free_port(host, start_port, max_attempts=50):
    for p in range(start_port, start_port + max_attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind((host, p))
                return p
            except OSError:
                continue
    raise RuntimeError(
        f"No free port found in range {start_port}–{start_port + max_attempts - 1}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="File Hunter")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="populate the database with test data on first run",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="host to bind to (default: config or 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="port to listen on (default: config or 8000)",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="enable auto-reload on code changes (development only)",
    )
    args = parser.parse_args()

    if args.demo:
        os.environ["FILE_HUNTER_DEMO"] = "1"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    config = load_config()
    host = args.host or config.get("host", "127.0.0.1")
    port = args.port or config.get("port", 8000)

    port = find_free_port(host, port)
    print(f"File Hunter starting on http://{host}:{port}")

    uvicorn.run(
        "file_hunter.app:app",
        host=host,
        port=port,
        reload=args.reload,
        log_level="info",
        ws_ping_interval=None,
        ws_ping_timeout=None,
    )
