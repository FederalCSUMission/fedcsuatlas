from dataclients import AtlasSession, create_type
from requests.auth import HTTPBasicAuth
import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument("--type-name", type=str, help="The name of the new type", required=True)
parser.add_argument("--inherit-from", nargs='+', help="The name of the superType to inherit from", required=False)
parser.add_argument("--attributes", nargs='+', help="Attributes to associate with the new type", required=False)
parser.add_argument("--description",  type=str, help="Type description", required=False)
parser.add_argument("--version",  type=int, help="Type version", required=False, default=1)

parser.add_argument("--atlas-endpoint", type=str, help="An endpoint for the Atlas API", required=True)
parser.add_argument("--atlas-admin", type=str, help  ="An administrative user for Atlas", required=True)
parser.add_argument("--atlas-password", type=str, help="An administrative user password for Atlas", required=True)

def main(args):
    session = AtlasSession(endpoint = args.atlas_endpoint, 
                           auth = HTTPBasicAuth(args.atlas_admin, args.atlas_password)) 
    
    typeDef = create_type(name=args.type_name, 
                          category= "ENTITY",
                          createdBy = args.atlas_admin, 
                          updatedBy = args.atlas_admin,  
                          description = args.description, 
                          version = args.version,
                          superTypes = args.inherit_from, 
                          attributes = args.attributes)
    print(f"Creating type: {typeDef}")
    
    resp = session.create_type(typeDef)
    print(f"Atlas type created: {resp}")  
    
if __name__ == "__main__":
    args, unks = parser.parse_known_args()
    main(args)