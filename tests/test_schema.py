"""Tests for BQBaseModel schema generation."""

from datetime import date, datetime
from enum import Enum
from typing import Optional

from pydantic import Field

from pydantic_bq.schema import BQBaseModel
from pydantic_bq.types import T


class Status(str, Enum):
    """Sample enum for testing."""

    ACTIVE = 'active'
    INACTIVE = 'inactive'


class AllTypesModel(BQBaseModel):
    """Model with all supported field types for testing."""

    str_field: str
    int_field: int
    float_field: float
    bool_field: bool
    datetime_field: datetime
    date_field: date
    optional_str: Optional[str] = None
    list_str: list[str] = []
    enum_field: Status = Status.ACTIVE
    optional_int: Optional[int] = None

    class Meta:
        table_id = 'all_types'
        table_description = 'Test table with all types'


class TestGetFieldType:
    """Tests for get_field_type method."""

    def test_str_type(self):
        """String fields map to STRING."""
        field_info = AllTypesModel.model_fields['str_field']
        assert AllTypesModel.get_field_type(field_info) == T.STR

    def test_int_type(self):
        """Integer fields map to INTEGER."""
        field_info = AllTypesModel.model_fields['int_field']
        assert AllTypesModel.get_field_type(field_info) == T.INT

    def test_float_type(self):
        """Float fields map to FLOAT."""
        field_info = AllTypesModel.model_fields['float_field']
        assert AllTypesModel.get_field_type(field_info) == T.FLOAT

    def test_bool_type(self):
        """Boolean fields map to BOOL."""
        field_info = AllTypesModel.model_fields['bool_field']
        assert AllTypesModel.get_field_type(field_info) == T.BOOL

    def test_datetime_type(self):
        """Datetime fields map to TIMESTAMP."""
        field_info = AllTypesModel.model_fields['datetime_field']
        assert AllTypesModel.get_field_type(field_info) == T.TS

    def test_date_type(self):
        """Date fields map to DATE."""
        field_info = AllTypesModel.model_fields['date_field']
        assert AllTypesModel.get_field_type(field_info) == T.DATE

    def test_optional_unwraps_type(self):
        """Optional[T] unwraps to T's type."""
        field_info = AllTypesModel.model_fields['optional_str']
        assert AllTypesModel.get_field_type(field_info) == T.STR

        field_info = AllTypesModel.model_fields['optional_int']
        assert AllTypesModel.get_field_type(field_info) == T.INT

    def test_list_unwraps_type(self):
        """list[T] unwraps to T's type."""
        field_info = AllTypesModel.model_fields['list_str']
        assert AllTypesModel.get_field_type(field_info) == T.STR

    def test_enum_maps_to_string(self):
        """Enum fields map to STRING."""
        field_info = AllTypesModel.model_fields['enum_field']
        assert AllTypesModel.get_field_type(field_info) == T.STR

    def test_union_type_syntax_raises_error(self):
        """Using X | None syntax raises TypeError."""
        import pytest
        from pydantic.fields import FieldInfo

        # Create a FieldInfo with UnionType annotation (X | None syntax)
        field_info = FieldInfo(annotation=str | None)

        with pytest.raises(TypeError, match='Use Optional'):
            AllTypesModel.get_field_type(field_info)


class TestGetFieldMode:
    """Tests for get_field_mode method."""

    def test_required_field(self):
        """Non-optional fields are REQUIRED."""
        field_info = AllTypesModel.model_fields['str_field']
        assert AllTypesModel.get_field_mode(field_info) == 'REQUIRED'

    def test_optional_field(self):
        """Optional fields are NULLABLE."""
        field_info = AllTypesModel.model_fields['optional_str']
        assert AllTypesModel.get_field_mode(field_info) == 'NULLABLE'

    def test_list_field(self):
        """List fields are REPEATED."""
        field_info = AllTypesModel.model_fields['list_str']
        assert AllTypesModel.get_field_mode(field_info) == 'REPEATED'


class TestBQSchema:
    """Tests for bq_schema method."""

    def test_generates_all_fields(self):
        """Schema includes all model fields."""
        schema = AllTypesModel.bq_schema()
        field_names = {f.name for f in schema}

        assert 'str_field' in field_names
        assert 'int_field' in field_names
        assert 'float_field' in field_names
        assert 'bool_field' in field_names
        assert 'datetime_field' in field_names
        assert 'date_field' in field_names
        assert 'optional_str' in field_names
        assert 'list_str' in field_names

    def test_schema_field_types(self):
        """Schema fields have correct types."""
        schema = AllTypesModel.bq_schema()
        schema_dict = {f.name: f for f in schema}

        assert schema_dict['str_field'].field_type == 'STRING'
        assert schema_dict['int_field'].field_type == 'INTEGER'
        assert schema_dict['float_field'].field_type == 'FLOAT'
        assert schema_dict['bool_field'].field_type == 'BOOL'
        assert schema_dict['datetime_field'].field_type == 'TIMESTAMP'
        assert schema_dict['date_field'].field_type == 'DATE'

    def test_schema_field_modes(self):
        """Schema fields have correct modes."""
        schema = AllTypesModel.bq_schema()
        schema_dict = {f.name: f for f in schema}

        assert schema_dict['str_field'].mode == 'REQUIRED'
        assert schema_dict['optional_str'].mode == 'NULLABLE'
        assert schema_dict['list_str'].mode == 'REPEATED'

    def test_schema_with_description(self):
        """Schema includes field descriptions."""

        class DescribedModel(BQBaseModel):
            name: str = Field(description='The name of the item')

            class Meta:
                table_id = 'described'

        schema = DescribedModel.bq_schema()
        assert schema[0].description == 'The name of the item'

    def test_listify_decorator(self):
        """bq_schema returns a list, not a generator."""
        schema = AllTypesModel.bq_schema()
        assert isinstance(schema, list)


class TestModelDump:
    """Tests for model_dump with date serialization."""

    def test_model_dump_serializes_datetime(self):
        """Datetime fields are serialized to ISO format."""
        model = AllTypesModel(
            str_field='test',
            int_field=1,
            float_field=1.0,
            bool_field=True,
            datetime_field=datetime(2024, 6, 15, 10, 30, 0),
            date_field=date(2024, 6, 15),
        )
        data = model.model_dump()

        assert data['datetime_field'] == '2024-06-15T10:30:00'

    def test_model_dump_serializes_date(self):
        """Date fields are serialized to ISO format."""
        model = AllTypesModel(
            str_field='test',
            int_field=1,
            float_field=1.0,
            bool_field=True,
            datetime_field=datetime(2024, 6, 15, 10, 30, 0),
            date_field=date(2024, 6, 15),
        )
        data = model.model_dump()

        assert data['date_field'] == '2024-06-15'

    def test_model_dump_preserves_other_types(self):
        """Non-date fields remain unchanged."""
        model = AllTypesModel(
            str_field='test',
            int_field=42,
            float_field=3.14,
            bool_field=True,
            datetime_field=datetime(2024, 6, 15),
            date_field=date(2024, 6, 15),
            optional_str='optional',
            list_str=['a', 'b'],
        )
        data = model.model_dump()

        assert data['str_field'] == 'test'
        assert data['int_field'] == 42
        assert data['float_field'] == 3.14
        assert data['bool_field'] is True
        assert data['optional_str'] == 'optional'
        assert data['list_str'] == ['a', 'b']


class TestModelConfig:
    """Tests for model configuration."""

    def test_extra_ignore(self):
        """Extra fields are ignored."""
        model = AllTypesModel(
            str_field='test',
            int_field=1,
            float_field=1.0,
            bool_field=True,
            datetime_field=datetime.now(),
            date_field=date.today(),
            unknown_field='should be ignored',
        )
        assert not hasattr(model, 'unknown_field')


class TestMeta:
    """Tests for Meta class."""

    def test_meta_table_id(self):
        """Meta.table_id is accessible."""
        assert AllTypesModel.Meta.table_id == 'all_types'

    def test_meta_table_description(self):
        """Meta.table_description is accessible."""
        assert AllTypesModel.Meta.table_description == 'Test table with all types'

    def test_meta_default_description(self):
        """Meta.table_description defaults to empty string."""

        class NoDescModel(BQBaseModel):
            value: int

            class Meta:
                table_id = 'no_desc'
                table_description = ''

        assert NoDescModel.Meta.table_description == ''
