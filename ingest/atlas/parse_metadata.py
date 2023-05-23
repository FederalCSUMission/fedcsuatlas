from dataclients import AtlasSession, create_entity
from requests.auth import HTTPBasicAuth
import argparse
import json
import glob

parser = argparse.ArgumentParser()
parser.add_argument("--type-name", type=str, help="The name of the new type", required=True)
parser.add_argument("--json-root", type=str, help="Local directory containing metadata files", required=True)

parser.add_argument("--hdfs-root", 
                    type=str, 
                    help="HDFS root directory", 
                    required=False, 
                    default="abfss://warehouse@mc2il5dedadlsstg01.blob.core.usgovcloudapi.net/")
parser.add_argument("--cluster-name", type=str, help="The cluster which contains the binary data", default="mc2-hive-il5-ded")
parser.add_argument("--dataset-group", type=str, help="Dataset category to which these data files belong", default="RF_data")
parser.add_argument("--file-sizes", type=int, help="Size of the files (if uniform)", default=1073741824)

parser.add_argument("--atlas-endpoint", type=str, help="An endpoint for the Atlas API", required=True)
parser.add_argument("--atlas-admin", type=str, help="An administrative user for Atlas", required=True)
parser.add_argument("--atlas-password", type=str, help="An administrative user password for Atlas", required=True)

def parse_to_attributes(metadata_json: dict, 
                        owner: str, 
                        hdfs_root: str, 
                        dataset_group: str, 
                        file_size: int, 
                        cluster_name: str) -> dict:
    return {
        "isFile": True,
        "owner": owner,
        "name": metadata_json['file'],
        "qualifiedName": metadata_json['file'],
        "bip_version": metadata_json['parser']['Version'],
        "packets_processed": metadata_json['packets_processed'],
        "path": f"{hdfs_root}/{metadata_json['file']}",
        "clusterName": cluster_name,
        "group": dataset_group,
        "fileSize": file_size
    }

def main(args):
    session = AtlasSession(endpoint = args.atlas_endpoint, 
                           auth = HTTPBasicAuth(args.atlas_admin, args.atlas_password)) 
    
    if not session.type_exists(args.type_name):
        print(f"{args.type_name} does not exist, exiting.")
        exit(0)
        
    for metadata in glob.glob(f"{args.json_root}/**/*.json"):
        with open(metadata, 'r') as handle:
            obj = json.load(handle)
            
        attributes = parse_to_attributes(obj, args.atlas_admin, args.hdfs_root)
        entity= create_entity(args.type_name, attributes)
    
        resp = session.upload_entity(entity)
    
    res = session.quick_search(type_name = args.type_name, query="group = 'RF_data'")
    print(res)
    
if __name__ == "__main__":
    args, unks = parser.parse_known_args()
    main(args)