from dataclients import AtlasSession, create_type, create_entity
from requests.auth import HTTPBasicAuth
import pytest

AUTH = HTTPBasicAuth("admin", "admin")
ENDPOINT = "https://mc2il5dedatlas.azurewebsites.us/"

def test_create_type():
    typeDef = create_type(name="entity-type", 
                          category= "ENTITY", 
                          attributes = ['test-attribute'])
    assert typeDef['category'] == 'ENTITY' 
    assert typeDef['name'] == 'entity-type'
    
def test_create_entity():
    attributes = {"attr1": "val1", "attr2": "val2"}
    entity= create_entity(type_name = "entity-type", 
                          entity_attributes = attributes)
    
    assert entity['entity']['typeName'] == 'entity-type'
    assert len(entity['entity']['attributes']) == 2
    
@pytest.mark.parametrize('type_name', ["hdfs_path", "bip_bin"])
def test_fetch_types(type_name):
    session = AtlasSession(ENDPOINT, AUTH)
    
    type_exists = session.type_exists(type_name)
    assert type_exists == True
    
    attr_df = session.get_type_attributes(type_name)
    assert len(attr_df) > 0
    
@pytest.mark.parametrize('type_name', ["test_type", "test_type_2"])
def test_create_delete_type(type_name):
    session = AtlasSession(ENDPOINT, AUTH)
    typeDef = create_type(name=type_name, 
                          category= "ENTITY", 
                          attributes = ['test-attribute'])
    
    session.create_type(typeDef)
    type_exists = session.type_exists(type_name)
    assert type_exists == True
    
    session.delete_type(type_name)
    type_exists = session.type_exists(type_name)
    assert type_exists == False
    