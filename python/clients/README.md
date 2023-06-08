### Python Interface for Data Warehouse APIs ###

A Pythonic interface wrapping Spark, Hive, and Atlas APIs.


## installation instructions ##

prerequisite python packages:
 - apache-atlas

for testing:
 - pytest


```
python3 -m pip install .
```

## Usage ##

```
from dataclients import AtlasSession
session = AtlasSession(endpoint = "<endpoint>", 
                       auth = HTTPBasicAuth("<username", "<password>")) 

```
