""" jrdodson
2023
"""
from requests.auth import HTTPBasicAuth
from abc import ABC, abstractmethod
from .model import *
import pandas as pd
import requests
import time
        
class WarehouseSession(ABC):
    """Abstract base class for managing remote API sessions within the Azure data warehouse.
    
    :param auth: HTTP authentication object containing username, password data (user must have admin credentials)
    :param session_id: unique identitifier for the session
    :param http_session: requests.Session object for making HTTP POST, GET, and DELETE requests; contains header information
    :param kernel: the type of session - pyspark or hive
    """
    def __init__(self, auth: HTTPBasicAuth, url: str, session_id: int, http_session: requests.Session, kernel: str):
        self._url = url
        self._kernel = kernel
        self._client = WarehouseClient(auth, url, http_session, self._kernel)
        self._session_id = session_id
    
    def __enter__(self) -> "WarehouseSession":
        self._wait_for_connection()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
        
    def close(self) -> None:
        """ Tear down the current API session and drop connection.
        """
        self._client.remove_session(self._session_id)
        self._client.close()
        
    def _perform_query_op(self, query: str) -> Output:
        query_statement = self._client.statement(query, self._session_id)
        def await_query(statement):
            in_progress = statement.state in ["waiting","running"]
            is_available = statement.state == "available"
            return in_progress or (is_available and statement.output is None)
        intervals = poll()
        while await_query(query_statement):
            time.sleep(next(intervals))
            query_statement = self._client.get_statement_status(query_statement.session_id, query_statement.statement_id)
        if query_statement.output is None:
            raise RuntimeError("statement had no output")
        query_statement.output.raise_for_status()
        if query_statement.output.text is None and query_statement.output.json is None:
            raise RuntimeError("statement had no output content")
        return query_statement.output
        
    @abstractmethod
    def query(self, query: str) -> pd.DataFrame:
        """Submit a query to the warehouse and return results as a native pandas DataFrame
        
        :param query: Code to execute against the warehouse
        """
        pass
    
    @abstractmethod
    def _deserialize(self, query_output) -> pd.DataFrame:
        pass
    
    @property
    def state(self) -> str:
        """Returns the current state of the session - isalive, stopped, etc.
        """
        try:
            return self._client.get_session(self._session_id).state
        except:
            raise RuntimeError(f"No session found for id {self._session_id}")
    
    def _wait_for_connection(self) -> None:
        intervals = poll()
        while self.state in ["not_started", "starting"]:
            time.sleep(next(intervals))
            
            
class SparkSession(WarehouseSession):
    """A WarehouseSession implementation specifically for communicating with Apache Spark
    """
    
    @classmethod
    def newSparkSession(cls, username: str, password: str, url: str, kernel: str = "spark"):
        """Create a new Apache Spark API session
        
        :param username: the username (must have administrative rights to the warehouse)
        :param password: the corresponding password
        :param url: the warehouse API url
        """
        def build_http_connection(user):
            conn = requests.Session()
            conn.headers.update({"X-Requested-By": user})
            return conn
        
        auth = HTTPBasicAuth(username, password)
        http_connection = build_http_connection(username)
        client = WarehouseClient(auth, url, http_connection, kernel)
        session = client.establish_session()
        client.close()
        return cls(auth, url, session.session_id, http_connection, kernel) 
    
    def _deserialize(self, query_output: str) -> pd.DataFrame:
        import json 
        rows = []
        for line in query_output.split("\n"):
            if line:
                rows.append(json.loads(line))
        return pd.DataFrame.from_records(rows)

    def submit(self, executable_path: str, cluster_config: dict = dict(), spark_config: dict = dict()) -> int:
        """ Use the SparkSession to submit an Apache Spark executable file to the warehouse.
        
        :param executable_path: the path to the Spark job executable (should be accessible remotely from the cluster)
        :param cluster_config: key-value mapping between cluster configurations and their values 
        :param spark_config: key-value mapping between Spark configurations and their values 
        """
        job = self._client.submit_job(executable_path,
                                      cluster_config.get('driver_memory'),
                                      cluster_config.get('driver_cores'),
                                      cluster_config.get('executor_memory'),
                                      cluster_config.get('executor_cores'),
                                      cluster_config.get('num_executors'),
                                      spark_config)
        return job.job_id
    
    def check_job_status(self, job_id: int) -> str:
        """ Check the current status of a running Spark job
        
        :param job_id: the unique identifier of the running job
        """
        job = self._client.get_job_status(job_id)
        return job.state
    
    def query(self, query: str) -> pd.DataFrame:
        output = self._perform_query_op(query)
        return self._deserialize(output.text)
    
class HiveSession(WarehouseSession):
    """A WarehouseSession implementation specifically for communicating with Hive tables
    """
    @classmethod
    def newHiveSession(cls, username: str, password: str, url: str, kernel: str = "sql"):
        """Create a new Hive API session
        
        :param username: the username (must have administrative rights to the warehouse)
        :param password: the corresponding password
        :param url: the warehouse API url
        """
        def build_http_connection(user):
            conn = requests.Session()
            conn.headers.update({"X-Requested-By": user})
            return conn
        
        auth = HTTPBasicAuth(username, password)
        http_connection = build_http_connection(username)
        client = WarehouseClient(auth, url, http_connection, kernel)
        session = client.establish_session()
        client.close()
        return cls(auth, url, session.session_id, http_connection, kernel) 
    
    def _deserialize(self, query_output: dict) -> pd.DataFrame:
        try:
            fields = query_output["schema"]["fields"]
            columns = [field["name"] for field in fields]
            data = query_output["data"]
        except KeyError:
            raise ValueError(f"Invalid json: {json}")
        return pd.DataFrame(data, columns=columns)
    
    def query(self, query: str) -> pd.DataFrame:
        output = self._perform_query_op(query)
        return self._deserialize(output.json)
            
class WarehouseClient:
    """An HTTP client for making POST, GET, and DELETE requests against the data warehouse API
    
    :param auth: HTTP authentication object containing username, password data (user must have admin credentials)
    :param url: the warehouse API url
    :param session: requests.Session object for making HTTP POST, GET, and DELETE requests; contains header information
    :param kernel: the type of session - pyspark or hive
    """
    def __init__(self, auth: HTTPBasicAuth, url: str, session: requests.Session, kernel: str):
        self._auth = auth
        self._url = url
        self._session = session
        self._kernel = kernel
        
    def close(self) -> None:
        self._session.close()

    def get(self, endpoint: str = "", params: dict = None) -> dict:
        return self._make_request(Method.GET, endpoint, params=params)

    def post(self, endpoint: str, data: dict = None) -> dict:
        return self._make_request(Method.POST, endpoint, data)

    def delete(self, endpoint: str = "") -> dict:
        return self._make_request(Method.DELETE, endpoint)
    
    def _make_request(self, method: Method, endpoint: str, data: dict = None, params: dict = None) -> dict:
        url = self._url.rstrip("/") + endpoint
        response = self._session.request(method.value, url, auth=self._auth, json=data, params=params)
        response.raise_for_status()
        return response.json()
    
    def get_statement_status(self, session_id: int, statement_id: int) -> Optional[Statement]:
        response = self.get(f"/sessions/{session_id}/statements/{statement_id}")
        return Statement.from_json(session_id, response)
    
    def submit_job(self, 
                   file: str, 
                   spark_driver_memory: str = None,
                   spark_driver_cores: int = None,
                   spark_executor_memory: str = None,
                   spark_executor_cores: int = None,
                   spark_num_executors: int = None,
                   spark_config: dict = None) -> Job:
        data = {"file": file}
        if spark_driver_memory is not None:
            data["driverMemory"] = spark_driver_memory
        if spark_driver_cores is not None:
            data["driverCores"] = spark_driver_cores
        if spark_executor_memory is not None:
            data["executorMemory"] = spark_executor_memory
        if spark_executor_cores is not None:
            data["executorCores"] = spark_executor_cores
        if spark_num_executors is not None:
            data["numExecutors"] = spark_num_executors
        if spark_config is not None:
            data['conf'] = spark_config
            
        json_resp = self.post("/batches", data=data)
        return Job.from_json(json_resp)
    
    def get_job_status(self, job_id: int) -> Optional[Job]:
        try:
            return Job.from_json(self.get(f"/batches/{job_id}"))
        except requests.HTTPError as err:
            if err.response.status_code == 404:
                return None
            else:
                raise
                
    def statement(self, query: str, session_id: int) -> Statement:
        json_resp = self.post(f"/sessions/{session_id}/statements", data={"code": query, "kind": self._kernel})
        return Statement.from_json(session_id, json_resp)
                                   
    def establish_session(self) -> Session:
        json_resp = self.post("/sessions", data={"kind": self._kernel})
        return Session.from_json(json_resp)
    
    def get_session(self, session_id: int) -> Optional[Session]:
        try:
            return Session.from_json(self.get(f"/sessions/{session_id}"))
        except requests.HTTPError as err:
            if err.response.status_code == 404:
                return None
            else:
                raise
                   
    def remove_session(self, session_id: int) -> int:
        self.delete(f"/sessions/{session_id}")
        return session_id
    
