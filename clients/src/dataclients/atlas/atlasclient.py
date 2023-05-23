from apache_atlas.client.base_client import AtlasClient
from apache_atlas.model.instance import AtlasEntity, AtlasEntityWithExtInfo, AtlasEntitiesWithExtInfo
from apache_atlas.model.typedef import AtlasConstraintDef, AtlasEntityDef, AtlasTypesDef, AtlasAttributeDef
from requests.auth import HTTPBasicAuth
import pandas as pd

class AtlasSession(object):
    def __init__(self, endpoint: str, auth: HTTPBasicAuth):
        self._endpoint = endpoint
        self._auth = auth

        self._client = AtlasClient(host=self._endpoint, auth = (self._auth.username, self._auth.password))

    @property
    def endpoint(self) -> str:
        return self._endpoint

    @property
    def auth(self) -> HTTPBasicAuth:
        return self._auth

    def upload_entity(self, entity: AtlasEntity) -> dict:
        '''
        Add a single AtlasEntity object to the registry
        '''
        try:
            return self._client.entity.create_entity(entity)
        except Exception as err:
            raise RuntimeError(f"Unable to upload entities: {str(err)}")
            
    def upload_entities(self, entities: list) -> dict:
        '''
        Bulk upload a list of AtlasEntity objects
        '''
        try:
            assert entities is not None and len(entities) > 0
            atlas_entities = AtlasEntitiesWithExtInfo()
            atlas_entities.entities = entities
            return self._client.entity.create_entities(atlas_entities)
        except Exception as err:
            raise RuntimeError(f"Unable to upload entities: {str(err)}")

    def search(self,type_name: str, query: str) -> pd.DataFrame:
        '''
        Query the Atlas Discovery endpoint to retrieve a list of entities based on some filter predicate using the Atlas DSL 
        
        The Atlas DSL specification is detailed here: https://atlas.apache.org/1.1.0/Search-Advanced.html
        
        DSL queries are typically formatted with a `from <type>` clause followed by some predicate, i.e. `where <attribute> = <value`
        
        :param type_name: the type name
        :param query: an Atlas DSL query string, i.e. `from bip_bin where name = <name>` 
        '''
        try:
            search_json = self._client.discovery.dsl_search(query)
            valid_entities = []
            for entity in search_json['entities']:
                if entity['typeName'] == type_name:
                    valid_entities += [entity]
            if len(valid_entities) == 0:
                return None
            guids = [entity['guid'] for entity in valid_entities]
            entity_json =  self._client.entity.get_entities_by_guids(guids)
            attrs = [entity['attributes'] for entity in entity_json['entities']]
            return pd.DataFrame.from_dict(attrs)
        except Exception as err:
            raise RuntimeError(f"Unable to query entities: {str(err)}")
            
    def quick_search(self, type_name: str, query: str, limit: int = None, exclude_deleted_entities: bool = True) -> pd.DataFrame:
        '''
        Query the Atlas Discovery endpoint to retrieve a list of entities based on some filter predicate
        '''
        try:
            search_json = self._client.discovery.quick_search(query, 
                                                              type_name = type_name, 
                                                              exclude_deleted_entities = True, 
                                                              limit = limit)
            result_count = search_json['searchResults']['approximateCount']
            if result_count == 0:
                return None
            guids = [entity['guid'] for entity in search_json['searchResults']['entities']]
            entity_json =  self._client.entity.get_entities_by_guids(guids)
            attrs = [entity['attributes'] for entity in entity_json['entities']]
            return pd.DataFrame.from_dict(attrs)
        except Exception as err:
            raise RuntimeError(f"Unable to query entities: {str(err)}")

    def get_entity_types(self, type_name: str) -> dict:
        '''
        Return a JSON dict containing type information from Atlas, optionally accepting parameters to filter by type values.
        '''
        try:
            constraint = AtlasConstraintDef({"params": {"typeName": type_name}})
            return self._client.typedef.get_entitydef_by_name(type_name)
        except Exception as err:
            raise RuntimeError(f"Unable to fetch type {type_name}, {str(err)}")

    def get_type_attributes(self, type_name: str) -> pd.DataFrame:
        '''
        Return a JSON dict containing attributes for a given type
        '''
        try:
            json = self.get_entity_types(type_name = type_name)
            return pd.read_json(str(json['attributeDefs']))
        except Exception as err:
            raise RuntimeError(f"Unable to fetch Atlas attributes for type {type_name}: {str(err)}")

    def type_exists(self, name: str) -> bool:
        '''
        Check if a type exists in the registry
        '''
        try:
            return self._client.typedef.type_with_name_exists(name)
        except:
            raise ValueError(f"Error searching for type by name: {name}")

    def create_type(self, typedef: AtlasEntityDef):
        '''
        Create a new type given a JSON typedef object
        '''
        defWrapper = AtlasTypesDef()
        defWrapper.entityDefs = [typedef]
        return self._client.typedef.create_atlas_typedefs(defWrapper)

    def delete_type(self, type_name: str):
        '''
        Remove an existing type from the registry
        '''
        return self._client.typedef.delete_type_by_name(type_name)


def create_entity(type_name: str, entity_attributes: dict) -> AtlasEntityWithExtInfo:
    '''
    Create an AtlasEntity object given an entity type and corresponding set of attributes to register.
    :param type_name: the Atlas type to implement
    :param entity_attributes: a dictionary of key:value pairs to register as part of the entity
    '''
    entity = AtlasEntity({'typeName': type_name})
    entity.attributes = entity_attributes
    upload = AtlasEntityWithExtInfo()
    upload.entity = entity
    return upload

def create_attribute(name: str, 
                     typeName: str = "string",  
                     isOptional: str = True, 
                     cardinality: str = "SINGLE", 
                     valuesMinCount: int = -1, 
                     valuesMaxCount: int = 1, 
                     isUnique: int = False, 
                     isIndexable: int = True, 
                     includeInNotification: int = False, 
                     searchWeight: int = 1):
    attribute_args = {
        "name": name, 
        "typeName": typeName,  
        "isOptional": isOptional, 
        "cardinality": cardinality, 
        "valuesMinCount": valuesMinCount, 
        "valuesMaxCount": valuesMaxCount, 
        "isUnique": isUnique, 
        "isIndexable": isIndexable, 
        "includeInNotification": includeInNotification, 
        "searchWeight": searchWeight
    }
    return AtlasAttributeDef(attribute_args)

def create_type(name: str,
                category: str, 
                createdBy: str = "admin", 
                updatedBy: str = "admin",  
                description: str = "", 
                version: str = "1",
                superTypes: list = [],
                attributes: list = [],
                **kwargs) -> AtlasEntityDef:
    type_args = {
        "name": name,
        "category": category,
        "createdBy": createdBy,
        "updatedBy": updatedBy,
        "description": description,
        "version": version,
        "superTypes": superTypes,
    }
    type_args.update(kwargs)
    
    typeDef = AtlasEntityDef(type_args)
    attributeDefs = [create_attribute(name = attr, 
                                      typeName = "string",  
                                      isOptional = True, 
                                      cardinality = "SINGLE", 
                                      valuesMinCount = -1, 
                                      valuesMaxCount = 1, 
                                      isUnique = False, 
                                      isIndexable = True, 
                                      includeInNotification = False, 
                                      searchWeight = 1) for attr in attributes]
    typeDef.attributeDefs = attributeDefs
    return typeDef
