import hashlib
import time
import json
from typing import Dict, Any, List, Optional, Tuple, Union
import jsonref
import os
import importlib
import pkgutil


# https://open.shipout.com/portal/zh/documentation/introduction.html#%E9%89%B4%E6%9D%83


def get_sign(path, version, timestamp, app_secret):
    data = {'path': path, 'version': version, 'timestamp': timestamp}
    list_data = list(data.keys())
    string_sign = ""
    sort_list = sorted(list_data)
    for item in sort_list:
        string_sign += str(item)
        string_sign += str(data[item])
    string_sign += app_secret
    sign = hashlib.md5(string_sign.encode('utf-8')).hexdigest().upper()
    return sign


def resolve_ref_links(obj: Any) -> Union[Dict[str, Any], List[Any]]:
    """
    Scan resolved schema and convert jsonref.JsonRef object to JSON serializable dict.

    :param obj - jsonschema object with ref field resolved.
    :return JSON serializable object with references without external dependencies.
    """
    if isinstance(obj, jsonref.JsonRef):
        obj = resolve_ref_links(obj.__subject__)
        # Omit existing definitions for external resource since
        # we dont need it anymore.
        obj.pop("definitions", None)
        return obj
    elif isinstance(obj, dict):
        return {k: resolve_ref_links(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [resolve_ref_links(item) for item in obj]
    else:
        return obj

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


class ResourceSchemaLoader:
    """JSONSchema loader from package resources"""

    def __init__(self, package_name: str):
        self.package_name = package_name

    def get_schema(self, name: str) -> dict:
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
        return self.__resolve_schema_references(raw_schema)

    def __resolve_schema_references(self, raw_schema: dict) -> dict:
        """
        Resolve links to external references and move it to local "definitions" map.

        :param raw_schema jsonschema to lookup for external links.
        :return JSON serializable object with references without external dependencies.
        """
        resolved = jsonref.JsonRef.replace_refs(raw_schema)
        resolved = resolve_ref_links(resolved)
        return resolved
