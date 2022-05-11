
from typing import Any, ClassVar, Dict, List, Mapping, MutableMapping, Optional, Set, Tuple, Union
import jsonref
import os
import pkgutil
import json
import importlib


def _resolve_ref_links(obj: Any,current_depth, max_depth) -> Union[Dict[str, Any], List[Any]]:
    """
    Scan resolved schema and convert jsonref.JsonRef object to JSON serializable dict.

    :param obj - jsonschema object with ref field resolved.
    :return JSON serializable object with references without external dependencies.
    """
    if current_depth==max_depth:
        return obj
    if isinstance(obj, jsonref.JsonRef):
        print(obj)
        obj = _resolve_ref_links(obj.__subject__,current_depth+1,max_depth)
        # Omit existing definitions for external resource since
        # we dont need it anymore.
        obj.pop("definitions", None)
        return obj
    elif isinstance(obj, dict):
        return {k: _resolve_ref_links(v,current_depth+1,max_depth) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_resolve_ref_links(item,current_depth+1,max_depth) for item in obj]
    else:
        return obj


def resolve_ref_links(obj: Any, max_depth=5) -> Union[Dict[str, Any], List[Any]]:
    return _resolve_ref_links(obj,0,max_depth)


class JsonFileLoader:
    """
    Custom json file loader to resolve references to resources located in "shared" directory.
    We need this for compatability with existing schemas cause all of them have references
    pointing to shared_schema.json file instead of shared/shared_schema.json
    """

    def __init__(self, uri_base: str, shared: str):
        self.shared = shared
        self.uri_base = uri_base

    def __call__(self, uri: str) -> Dict[str, Any]:
        uri = uri.replace(self.uri_base, f"{self.uri_base}/{self.shared}/")
        return json.load(open(uri))


class ResourceLoader:
    def __init__(self, package_name: str):
        self.package_name = package_name

    def get_schema(self, name: str, max_depth:int =5) -> dict:
        """
        This method retrieves a JSON schema from the schemas/ folder.


        The expected file structure is to have all top-level schemas (corresponding to streams) in the "schemas/" folder, with any shared $refs
        living inside the "schemas/shared/" folder. For example:

        schemas/shared/<shared_definition>.json
        schemas/<name>.json # contains a $ref to shared_definition
        schemas/<name2>.json # contains a $ref to shared_definition
        """

        schema_filename = f"schemas/{name}.json"
        raw_file = pkgutil.get_data(self.package_name, schema_filename)
        if not raw_file:
            raise IOError(f"Cannot find file {schema_filename}")
        try:
            raw_schema = json.loads(raw_file)
        except ValueError as err:
            raise RuntimeError(f"Invalid JSON file format for file {schema_filename}") from err

        return self.__resolve_schema_references(raw_schema,max_depth=max_depth)

    def __resolve_schema_references(self, raw_schema: dict, max_depth:int=5) -> dict:
        """
        Resolve links to external references and move it to local "definitions" map.

        :param raw_schema jsonschema to lookup for external links.
        :param max_depth max_depth when resolve the reference link
        :return JSON serializable object with references without external dependencies.
        """

        package = importlib.import_module(self.package_name)
        base = os.path.dirname(package.__file__) + "/"
        resolved = jsonref.JsonRef.replace_refs(raw_schema, loader=JsonFileLoader(base, "schemas/shared"), base_uri=base)
        resolved = resolve_ref_links(resolved, max_depth=max_depth)
        return resolved

    def get_graphql_gql(self, name:str)-> str:
        gql_filename = f"gql/{name}.gql"
        raw_file = pkgutil.get_data(self.package_name, gql_filename)
        if not raw_file:
            raise IOError(f"Cannot find file {gql_filename}")
        return raw_file

