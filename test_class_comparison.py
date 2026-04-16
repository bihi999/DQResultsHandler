# Testen der Funktionen für classes\class_comparison.py und ihrer Klassen und Methoden.

import pandas as pd
import pytest
from pathlib import Path

from classes.class_comparison import Comparison as cc


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
    assert isinstance(cc.extract_comparison_columns(urdaten.columns, cc.default_column_names), set)

# Wieviele Tests lassen sich hier einreihen

# test 2: Es wird der Abgleichstyp korrekt ermittelt
# test 3: ....