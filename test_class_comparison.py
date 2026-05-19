# Testen der Funktionen für classes\class_comparison.py und ihrer Klassen und Methoden.

import pandas as pd
import pytest
from pathlib import Path

from classes.class_comparison import Comparison as cc, ComparisonType


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


def test_comparison_extracts_stammdaten_and_normalizes_column_names_on_init():
    dataframe = pd.DataFrame(
        {
            "Nr.": [1],
            "Firmenname": ["Test GmbH"],
            "*WebFirmenID*": [123],
            "*(Nicht aendern) Firma*": ["abc"],
        }
    )

    instance = cc(
        ComparisonType.FIRMENABGLEICH,
        {"Firmenname"},
        dataframe,
        "test.xlsx",
    )

    assert instance.stammdaten_spalten == {"WebFirmenID", "(Nicht aendern) Firma"}
    assert set(instance.comparison_data.columns) == {"Nr.", "Firmenname", "WebFirmenID", "(Nicht aendern) Firma"}


def test_comparison_evaluates_stammdaten_dataframes_on_init():
    dataframe = pd.DataFrame(
        {
            "Nr.": [1, 2, 3, 3],
            "Firmenname": ["A", "B", "C", "C"],
            "*WebFirmenID*": [123, None, "", ""],
            "*firmentupel_apollo*": ["apollo-1", "apollo-2", None, None],
            "*LeereSpalte*": [None, None, None, None],
        }
    )

    instance = cc(
        ComparisonType.FIRMENABGLEICH,
        {"Firmenname"},
        dataframe,
        "test.xlsx",
    )

    assert set(instance.stammdaten) == {"WebFirmenID", "firmentupel_apollo"}
    assert len(instance.stammdaten["WebFirmenID"]) == 1
    assert len(instance.stammdaten["firmentupel_apollo"]) == 2
    assert "Firmenname" not in instance.stammdaten["WebFirmenID"].columns
    assert "LeereSpalte" not in instance.stammdaten["WebFirmenID"].columns
