import pytest

from classes.class_comparison import Comparison as cc
from classes.class_comparison import ComparisonType, ComparisonTypeDetectionError


def test_detect_comparison_type_firmenabgleich():
    comparison_columns = {"Firmenname"}
    all_columns = {"Nr.", "Firmenname", "WebFirmenID", "firmentupel_apollo"}

    assert cc.detect_comparison_type(comparison_columns, all_columns) == ComparisonType.FIRMENABGLEICH


def test_detect_comparison_type_removes_stars_from_all_columns():
    comparison_columns = {"Firmenname"}
    all_columns = {"Nr.", "Firmenname", "*WebFirmenID*", "*firmentupel_apollo*"}

    assert cc.detect_comparison_type(comparison_columns, all_columns) == ComparisonType.FIRMENABGLEICH


def test_detect_comparison_type_kontaktabgleich():
    comparison_columns = {"Vorname", "Nachname"}
    all_columns = {"Nr.", "Vorname", "Nachname", "WebID", "ApolloMemberID"}

    assert cc.detect_comparison_type(comparison_columns, all_columns) == ComparisonType.KONTAKTABGLEICH


@pytest.mark.parametrize(
    "comparison_columns, all_columns",
    [
        (["Firmenname"], {"Nr.", "Firmenname"}),
        ({"Firmenname"}, ["Nr.", "Firmenname"]),
        ({"Firmenname", 1}, {"Nr.", "Firmenname"}),
        ({"Firmenname"}, {"Nr.", "Firmenname", 1}),
        ({"Firmenname"}, {"Nr.", "Firmenname"}),
    ],
)
def test_detect_comparison_type_raises_for_invalid_or_unknown_columns(comparison_columns, all_columns):
    with pytest.raises(ComparisonTypeDetectionError):
        cc.detect_comparison_type(comparison_columns, all_columns)
