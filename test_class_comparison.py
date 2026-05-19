# Testen der Funktionen für classes\class_comparison.py und ihrer Klassen und Methoden.

import pandas as pd
import pytest
from pathlib import Path

from classes.class_comparison import Comparison as cc, ComparisonColumnSet, ComparisonType


testdaten = {}

testtabellen = { "08092025_Firmenabgleich_domain.xlsx" : "Firmenabgleich_domain"}
                
skript_pfad = Path(__file__).parent
datei_pfad = skript_pfad / "daten" / "meine_datei.txt"

for key, value in testtabellen.items():
    dateipfad = skript_pfad / "test_determine_comparison" /  key
    testdaten[key] = {"dataframe": "", "comparison_type" : ""}
    testdaten[key]["dataframe"] = pd.read_excel(dateipfad)
    testdaten[key]["comparison_type"] = value


# test 1: Übernimmt einen Index und macht daraus eine Menge an Spaltennamen
    
@pytest.mark.parametrize("urdaten,abgleichstyp", [(case["dataframe"], case["comparison_type"]) for case in testdaten.values()], ids=list(testdaten.keys()),)
def test_datentyp_rueckgabe(urdaten, abgleichstyp):
    assert isinstance(cc.extract_comparison_columns(urdaten.columns), set)

# Wieviele Tests lassen sich hier einreihen

# test 2: Es wird der Abgleichstyp korrekt ermittelt
# test 3: ....


def _prepare_for_comparison(dataframe):
    for column_name in ComparisonColumnSet.DEFAULT_COLUMN_NAMES.value:
        if column_name not in dataframe.columns:
            dataframe[column_name] = "x"
    return cc.prepare_raw_dataframe(dataframe)


def test_prepare_raw_dataframe_normalizes_missing_values_and_types():
    dataframe = pd.DataFrame(
        {
            "Nr.": [1.0, 2.0],
            "löschen": ["---", ""],
            "%": ["90%", "---"],
            "UnvollständigerDatensatz": ["", "nein"],
            "Tabelle": ["Lead", "Firma"],
            "*WebFirmenID*": [123.0, "---"],
            "Firmenname": ["A GmbH", None],
        }
    )

    prepared_dataframe = cc.prepare_raw_dataframe(dataframe)

    assert prepared_dataframe is not None
    assert "WebFirmenID" in prepared_dataframe.columns
    assert str(prepared_dataframe["Nr."].dtype) == "Int64"
    assert str(prepared_dataframe["WebFirmenID"].dtype) == "Int64"
    assert str(prepared_dataframe["Firmenname"].dtype) == "string"
    assert pd.isna(prepared_dataframe.loc[1, "WebFirmenID"])
    assert pd.isna(prepared_dataframe.loc[0, "löschen"])


def test_extract_comparison_columns_ignores_prepared_stammdaten_columns():
    dataframe = pd.DataFrame(
        {
            "Nr.": [1],
            "löschen": [""],
            "%": ["90%"],
            "UnvollständigerDatensatz": [""],
            "Tabelle": ["Firma"],
            "Firmenname": ["A GmbH"],
            "*WebFirmenID*": [123],
        }
    )

    prepared_dataframe = cc.prepare_raw_dataframe(dataframe)

    assert cc.extract_comparison_columns(prepared_dataframe) == {"Firmenname"}


def test_comparison_extracts_stammdaten_and_normalizes_column_names_on_init():
    dataframe = pd.DataFrame(
        {
            "Nr.": [1],
            "Firmenname": ["Test GmbH"],
            "*WebFirmenID*": [123],
            "*(Nicht aendern) Firma*": ["abc"],
        }
    )

    prepared_dataframe = _prepare_for_comparison(dataframe)
    instance = cc(
        ComparisonType.FIRMENABGLEICH,
        {"Firmenname"},
        prepared_dataframe,
        "test.xlsx",
    )

    assert instance.stammdaten_spalten == {"WebFirmenID", "(Nicht aendern) Firma"}
    assert {"Nr.", "Firmenname", "WebFirmenID", "(Nicht aendern) Firma"}.issubset(instance.comparison_data.columns)


def test_comparison_does_not_evaluate_stammdaten_dataframes_on_init():
    dataframe = pd.DataFrame(
        {
            "Nr.": [1, 2, 3, 3],
            "Firmenname": ["A", "B", "C", "C"],
            "*WebFirmenID*": [123, None, "", ""],
            "*firmentupel_apollo*": ["apollo-1", "apollo-2", None, None],
            "*LeereSpalte*": [None, None, None, None],
        }
    )

    prepared_dataframe = _prepare_for_comparison(dataframe)
    instance = cc(
        ComparisonType.FIRMENABGLEICH,
        {"Firmenname"},
        prepared_dataframe,
        "test.xlsx",
    )

    assert not hasattr(instance, "stammdaten")
    assert instance.comparison_data is prepared_dataframe
