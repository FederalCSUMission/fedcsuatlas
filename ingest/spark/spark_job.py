""" jrdodson
2023
"""
from dataclients import SparkSession
import configparser
import argparse
import time
import base64

parser = argparse.ArgumentParser()
parser.add_argument("--config", type=str, help="Path to configuration file", required=True)
args, unks = parser.parse_known_args()


def main():
    config = configparser.ConfigParser()
    config.read(args.config)
    
    try:
        credentials = dict(config['connection'])
    except KeyError as err:
        raise RuntimeError(f"Error in config: {err}")
    
    pw = base64.b64decode(credentials['b64_encoded_password']).decode()
    with SparkSession.newSparkSession(credentials['username'], pw, credentials['url'], kernel="pyspark") as spark:
        job_id = spark.submit(config['job']['executable'])
        while True:
            time.sleep(15.0)
            job_status = spark.check_job_status(job_id)
            print(f"Job status: {job_status}")
            if job_status == "success" or job_status == "dead":
                exit(1)


if __name__ == "__main__":
    main()