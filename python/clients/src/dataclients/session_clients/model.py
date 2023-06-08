""" jrdodson
2023
"""
from typing import Iterable, Iterator, Optional, List
from dataclasses import dataclass
from enum import Enum

class Method(Enum):
    """ Valid HTTP methods for this API
    """
    GET = "GET"
    POST = "POST"
    DELETE = "DELETE"
          
@dataclass
class Session:
    """ Object representation for an API session JSON object
    
    A session is a single active connection to the API.
    """
    session_id: int
    proxy_user: str
    kind: str
    state: str

    @classmethod
    def from_json(cls, data: dict) -> "Session":
        return cls(data["id"],data["proxyUser"],data["kind"],data["state"])

@dataclass
class Output:
    """ Object representation for an API output JSON object
    
    Outputs are any valid output returned from an API call
    """
    status: str
    text: Optional[str]
    json: Optional[dict]
    ename: Optional[str]
    evalue: Optional[str]
    traceback: Optional[List[str]]

    @classmethod
    def from_json(cls, data: dict) -> "Output":
        return cls(data["status"],
            data.get("data", {}).get("text/plain"),
            data.get("data", {}).get("application/json"),
            data.get("ename"),
            data.get("evalue"),
            data.get("traceback"))

    def raise_for_status(self) -> None:
        if self.status ==  "error":
            raise RuntimeError(f"Error collecting output: {ename} {evalue}")
            
@dataclass
class Job:
    """ Object representation for an API job JSON object
    
    A Spark job is a remote process executed as an asynchronous batch workload.
    """
    job_id: int
    app_id: Optional[str]
    app_info: Optional[dict]
    log: List[str]
    state: str

    @classmethod
    def from_json(cls, data: dict) -> "Job":
        return cls(data["id"],data.get("appId"),data.get("appInfo"),data.get("log", []),data["state"])
    
@dataclass
class Statement:
    """ Object representation for an API statement JSON object
    
    A statement is a submitted query to the API. It could be either a Hive query or valid Spark code.
    """
    session_id: int
    statement_id: int
    state: str
    code: str
    output: Optional[Output]
    progress: Optional[float]

    @classmethod
    def from_json(cls, session_id: int, data: dict) -> "Statement":
        if data["output"] is None:
            output = None
        else:
            output = Output.from_json(data["output"])
        return cls(session_id,data["id"],data["state"],data["code"],output,data.get("progress"))
    
    
def poll(start: Iterable[float] = [0.1, 0.2, 0.3, 0.5], rest: float = 1.0, end: float = 100.0) -> Iterator[float]:
    def yield_interval():
        yield from start
        while True:
            yield rest
    total = 0.0
    for interval in yield_interval():
        total += interval
        if total > end:
            break
        yield interval
