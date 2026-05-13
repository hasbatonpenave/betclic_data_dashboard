"""
server.py — Entry point. Starts the uvicorn server.

Usage:
    python server.py
    BETCLIC_PORT=5001 python server.py
"""

import logging
import uvicorn
from config import settings

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(name)-28s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

if __name__ == "__main__":
    uvicorn.run(
        "server.app:app",
        host="0.0.0.0",
        port=settings.port,
        reload=False,
        log_level=settings.log_level.lower(),
    )
