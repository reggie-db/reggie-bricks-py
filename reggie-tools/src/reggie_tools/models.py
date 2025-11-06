from pydantic import BaseModel
from reggie_core import objects, strs


class SchemaModel(BaseModel):
    @classmethod
    def model_json_schema(cls, *args, exclude_keys: list[str] = [], **kwargs):
        schema = super().model_json_schema(*args, **kwargs)
        objects.remove_keys(schema, *exclude_keys)
        return schema

    @classmethod
    def model_response_format(cls, *args, exclude_keys: list[str] = [], **kwargs):
        name = "_".join(strs.tokenize(cls.__name__))
        schema = cls.model_json_schema(*args, exclude_keys=exclude_keys, **kwargs)
        response_format = {
            "type": "json_schema",
            "json_schema": {"name": name, "schema": schema},
        }
        return response_format
