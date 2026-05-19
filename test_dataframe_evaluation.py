import pandas as pd

from helper_functions.dataframe_evaluation import evaluate_stammdaten_dataframes, enrich_dataframe_with_stammdaten


def test_enrich_dataframe_with_stammdaten_left_joins_without_duplicate_id():
    target_dataframe = pd.DataFrame(
        {
            "WebFirmenID": [1, 2, 3],
            "Wert": ["a", "b", "c"],
        }
    )
    stammdaten = {
        "WebFirmenID": pd.DataFrame(
            {
                "WebFirmenID": [1, 2],
                "Firmenname": ["A GmbH", "B GmbH"],
                "Ort": ["Berlin", "Hamburg"],
            }
        )
    }

    enriched_dataframe = enrich_dataframe_with_stammdaten(stammdaten, target_dataframe)

    assert list(target_dataframe.columns) == ["WebFirmenID", "Wert"]
    assert "WebFirmenID" in enriched_dataframe.columns
    assert "WebFirmenID_WebFirmenID" not in enriched_dataframe.columns
    assert "WebFirmenID_Firmenname" in enriched_dataframe.columns
    assert "WebFirmenID_Ort" in enriched_dataframe.columns
    assert len(enriched_dataframe) == 3
    assert enriched_dataframe.loc[0, "WebFirmenID_Firmenname"] == "A GmbH"
    assert pd.isna(enriched_dataframe.loc[2, "WebFirmenID_Firmenname"])


def test_evaluate_stammdaten_dataframes_keeps_dict_structure_and_deduplicates():
    dataframe = pd.DataFrame(
        {
            "WebFirmenID": [1, 2, 2, None],
            "WebID": [None, None, 10, 11],
            "Firmenname": ["A GmbH", "B GmbH", "B GmbH", "C GmbH"],
            "Abgleichsspalte": ["x", "y", "y", "z"],
            "LeereSpalte": [None, None, None, None],
        }
    )

    stammdaten = evaluate_stammdaten_dataframes(dataframe, {"Abgleichsspalte"})

    assert set(stammdaten) == {"WebFirmenID", "WebID"}
    assert len(stammdaten["WebFirmenID"]) == 2
    assert len(stammdaten["WebID"]) == 2
    assert "Abgleichsspalte" not in stammdaten["WebFirmenID"].columns
    assert "LeereSpalte" not in stammdaten["WebID"].columns
