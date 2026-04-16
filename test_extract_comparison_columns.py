from test_determine_comparison import ColumnNamesComparisonTypes as TestData
from classes.class_comparison import Comparison as cc
import pytest


@pytest.mark.parametrize("GesuchteMenge, AlleSpaltennamen", TestData.ExtractionCases)
def test_determine_comparison_type(GesuchteMenge, AlleSpaltennamen):
    assert cc.extract_comparison_columns(AlleSpaltennamen, cc.default_column_names) == GesuchteMenge