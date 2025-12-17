"""Pydantic models for BigQuery schema generation."""

from datetime import date, datetime
from types import UnionType
from typing import Iterable, Union, get_args, get_origin

from google.cloud.bigquery import SchemaField
from pydantic import BaseModel, ConfigDict
from pydantic.fields import FieldInfo

from .types import T


def listify(func):
    """Decorator to convert generator to list."""

    def wrapper(*args, **kwargs):
        return list(func(*args, **kwargs))

    return wrapper


class BQBaseModel(BaseModel):
    """Base model for BigQuery tables with automatic schema generation."""

    model_config = ConfigDict(extra='ignore')

    @classmethod
    def get_field_type(cls, field_info: FieldInfo) -> T:
        """Determine BigQuery field type from Pydantic field annotation."""
        annotation = field_info.annotation

        if isinstance(annotation, UnionType):
            raise TypeError('Use Optional[X] or Union[X, None] instead of X | None syntax for field annotations')

        if get_origin(annotation) is Union:
            # Dealing with Optional fields
            annotations = get_args(annotation)
            assert annotations[1] is type(None)
            annotation = get_args(annotation)[0]

        if get_origin(annotation) is list:
            annotation = get_args(annotation)[0]

        if annotation is str:
            return T.STR
        elif annotation is int:
            return T.INT
        elif annotation is float:
            return T.FLOAT
        elif annotation is bool:
            return T.BOOL
        elif annotation is datetime:
            return T.TS
        elif annotation is date:
            return T.DATE
        else:
            # Assuming Enums here but difficult to check
            return T.STR

    @classmethod
    def get_field_mode(cls, field_info: FieldInfo) -> str:
        """Determine BigQuery field mode from Pydantic field annotation."""
        is_optional = False
        annotation = field_info.annotation
        if get_origin(annotation) is Union:
            # Dealing with Optional fields
            annotations = get_args(annotation)
            assert annotations[1] is type(None)
            annotation = get_args(annotation)[0]
            is_optional = True
        if get_origin(annotation) is list:
            return 'REPEATED'
        elif is_optional:
            return 'NULLABLE'
        else:
            return 'REQUIRED'

    @classmethod
    @listify
    def bq_schema(cls) -> Iterable[SchemaField]:
        """Generate BigQuery schema from Pydantic model fields."""
        for field_name, field_info in cls.model_fields.items():
            field_info: FieldInfo
            yield SchemaField(
                field_name,
                cls.get_field_type(field_info),
                mode=cls.get_field_mode(field_info),
                description=field_info.description,
            )

    def model_dump(self, *args, **kwargs) -> dict:
        """Dump model to dict with ISO formatted dates."""
        data = super().model_dump(*args, **kwargs)
        for f, v in data.items():
            if isinstance(v, date) or isinstance(v, datetime):
                data[f] = v.isoformat()
        return data

    class Meta:
        table_id: str = NotImplemented
        table_description: str = ''
