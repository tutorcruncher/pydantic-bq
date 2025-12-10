"""Tests for types module utilities."""

from decimal import Decimal

from pydantic_bq.types import T, to_str


class TestTEnum:
    """Tests for T enum."""

    def test_int_value(self):
        """INT maps to INTEGER."""
        assert T.INT == 'INTEGER'
        assert T.INT.value == 'INTEGER'

    def test_num_value(self):
        """NUM maps to NUMERIC."""
        assert T.NUM == 'NUMERIC'

    def test_float_value(self):
        """FLOAT maps to FLOAT."""
        assert T.FLOAT == 'FLOAT'

    def test_str_value(self):
        """STR maps to STRING."""
        assert T.STR == 'STRING'

    def test_ts_value(self):
        """TS maps to TIMESTAMP."""
        assert T.TS == 'TIMESTAMP'

    def test_date_value(self):
        """DATE maps to DATE."""
        assert T.DATE == 'DATE'

    def test_bool_value(self):
        """BOOL maps to BOOL."""
        assert T.BOOL == 'BOOL'


class TestToStr:
    """Tests for to_str function."""

    def test_decimal_formats_two_decimals(self):
        """Decimal values are formatted with 2 decimal places."""
        result = to_str(Decimal('123.456'))
        assert result == '123.46'

        result = to_str(Decimal('100'))
        assert result == '100.00'

        result = to_str(Decimal('0.1'))
        assert result == '0.10'

    def test_bool_lowercase(self):
        """Boolean values are converted to lowercase strings."""
        assert to_str(True) == 'true'
        assert to_str(False) == 'false'

    def test_string_passthrough(self):
        """String values pass through unchanged."""
        assert to_str('hello') == 'hello'
        assert to_str('') == ''

    def test_int_converts(self):
        """Integer values are converted to strings."""
        assert to_str(42) == '42'
        assert to_str(0) == '0'
        assert to_str(-10) == '-10'

    def test_float_converts(self):
        """Float values are converted to strings."""
        assert to_str(3.14) == '3.14'
        assert to_str(0.0) == '0.0'

    def test_none_converts(self):
        """None converts to 'None' string."""
        assert to_str(None) == 'None'
