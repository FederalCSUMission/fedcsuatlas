""" jrdodson
2023
"""
from dataclients import HiveSession
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
    with HiveSession.newHiveSession(credentials['username'], pw, credentials['url']) as hive:
        df = hive.query("SELECT * FROM warehouse.jukebox_bin_metadata LIMIT 10")
        print(df)
        
        df = hive.query(f"""
                         SELECT * FROM warehouse.jukebox_sample_temp LIMIT 10
                         """)
        print(df)
        
if __name__ == "__main__":
    main()
