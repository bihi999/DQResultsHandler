

from test_determine_comparison import ColumnNamesComparisonTypes
from classes.class_comparison import Comparison as cc

import pytest


@pytest.mark.parametrize("Abgleichstyp, Spaltennamen", ColumnNamesComparisonTypes.TestCases)
def test_determine_comparison_type(Abgleichstyp, Spaltennamen):
    assert cc.detect_comparison_type(Spaltennamen, cc.contact_fields) == Abgleichstyp


