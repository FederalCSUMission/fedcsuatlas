""" jrdodson
2023
"""
from dataclients import SparkSession
import configparser
import argparse
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
    with SparkSession.newSparkSession(credentials['username'], pw, credentials['url']) as spark:
        df = spark.query(f"""
                          spark.sql("SELECT * FROM {config['interactive']['table']}").
                                limit(10).
                                toJSON.
                                collect.
                                foreach(println)
                          """)
    print(type(df))
    print(df)

if __name__ == "__main__":
    main()

