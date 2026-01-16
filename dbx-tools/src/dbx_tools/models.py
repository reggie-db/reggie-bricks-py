"""
Pydantic model extensions for JSON schema generation and OpenAI response formats.

Provides a base model class with utilities for generating JSON schemas with
key exclusion and OpenAI-compatible response format specifications.
"""

from dbx_core import objects, strs
from pydantic import BaseModel


class SchemaModel(BaseModel):
    """
    Base Pydantic model with JSON schema generation utilities.

    Extends Pydantic's BaseModel with methods for generating JSON schemas that
    can exclude specific keys and for creating OpenAI-compatible response format
    specifications.
    """

    @classmethod
    def model_json_schema(cls, *args, exclude_keys: list[str] | None = None, **kwargs):
        """
        Generate JSON schema for the model with optional key exclusion.

        Generates a standard Pydantic JSON schema and then recursively removes
        the specified keys from the schema structure.

        Args:
            *args: Positional arguments passed to parent model_json_schema
            exclude_keys: List of keys to remove from the generated schema
            **kwargs: Keyword arguments passed to parent model_json_schema

        Returns:
            Dictionary containing the JSON schema with excluded keys removed
        """
        schema = super().model_json_schema(*args, **kwargs)
        if exclude_keys:
            objects.remove_keys(schema, *exclude_keys)
        return schema

    @classmethod
    def model_response_format(
        cls, *args, exclude_keys: list[str] | None = None, **kwargs
    ):
        """
        Generate OpenAI-compatible response format specification.

        Creates a response format dictionary suitable for use with OpenAI API
        calls. The format includes a JSON schema generated from the model with
        optional key exclusion. The schema name is derived from the class name
        by tokenizing and joining with underscores.

        Args:
            *args: Positional arguments passed to model_json_schema
            exclude_keys: List of keys to exclude from the schema
            **kwargs: Keyword arguments passed to model_json_schema

        Returns:
            Dictionary with "type" and "json_schema" keys formatted for OpenAI API
        """
        name = "_".join(strs.tokenize(cls.__name__))
        schema = cls.model_json_schema(*args, exclude_keys=exclude_keys, **kwargs)
        response_format = {
            "type": "json_schema",
            "json_schema": {"name": name, "schema": schema},
        }
        return response_format
