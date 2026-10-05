"""Thin application-owned bridge; no conversation data is sent implicitly."""
import asyncio
import json
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class Client:
    def __init__(self, connection_file: str, session_id: str):
        config = json.loads(Path(connection_file).read_text())
        self._url = config["url"]
        self._token = config["token"]
        self._session_id = session_id

    async def execute(self, code: str) -> dict:
        """Submit once to the shared namespace; disconnect does not undo execution."""
        def request():
            body = json.dumps({"session_id": self._session_id, "code": code}).encode()
            command = Request(self._url, data=body, method="POST", headers={
                "Authorization": "Bearer " + self._token,
                "Content-Type": "application/json",
            })
            try:
                with urlopen(command, timeout=70) as response:
                    return json.load(response)
            except HTTPError as error:
                raise RuntimeError(f"Application kernel rejected execution ({error.code}); no automatic retry.") from None
        return await asyncio.to_thread(request)

    async def catalog(self) -> dict:
        """Read the latest function catalog; refresh before adding/replacing functions."""
        def request():
            query = urlencode({"session_id": self._session_id})
            command = Request(self._url.rstrip("/") + "/catalog?" + query, headers={
                "Authorization": "Bearer " + self._token,
            })
            try:
                with urlopen(command, timeout=15) as response:
                    return json.load(response)
            except HTTPError as error:
                raise RuntimeError(f"Application kernel catalog unavailable ({error.code}).") from None
        return await asyncio.to_thread(request)


def connect(connection_file: str, session_id: str) -> Client:
    return Client(connection_file, session_id)
