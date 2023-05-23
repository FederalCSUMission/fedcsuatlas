from dataclients import AtlasSession
from requests.auth import HTTPBasicAuth
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--type-name", type=str, help="The name of the new type", required=True)
parser.add_argument("--atlas-endpoint", type=str, help="An endpoint for the Atlas API", required=True)
parser.add_argument("--atlas-admin", type=str, help="An administrative user for Atlas", required=True)
parser.add_argument("--atlas-password", type=str, help="An administrative user password for Atlas", required=True)

QUERY_ON_DATASET_TYPE = "group = RF_data" 
QUERY_ON_BIN_FILENAME = "name like 'Jukebox_Streaming_Data_20220630_22.32.59.598177873_GMT_04387_20220630_23.34.27.018380230_GMT.bin'"
QUERY_ON_NUM_PACKETS =  "from bip_bin where packets_processed > 134925"

def main(args):
    session = AtlasSession(endpoint = args.atlas_endpoint, 
                           auth = HTTPBasicAuth(args.atlas_admin, args.atlas_password)) 
    
    df = session.quick_search(type_name = args.type_name,
                              query = QUERY_ON_DATASET_TYPE)
    print(df[["name", "owner", "packets_processed", "bip_version", "fileSize", "path"]])
    print()
    
    df = session.quick_search(type_name = args.type_name,
                              query = QUERY_ON_BIN_FILENAME)
    print(df[["name", "owner", "packets_processed", "bip_version", "fileSize", "path"]])
    print()
    
    df = session.search(type_name = args.type_name,
                        query = QUERY_ON_NUM_PACKETS)
    print(df[["name", "owner", "packets_processed", "bip_version", "fileSize", "path"]])
    print()

if __name__ == "__main__":
    args, unks = parser.parse_known_args()
    main(args)