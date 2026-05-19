import pandas as pd

from helper_functions.dataframe_evaluation import combine_stammdaten_from_comparisons, enrich_dataframe_with_stammdaten


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


def test_combine_stammdaten_from_comparisons_keeps_dict_structure_and_deduplicates():
    class ComparisonStub:
        def __init__(self, stammdaten):
            self.stammdaten = stammdaten

    comparisons = [
        ComparisonStub(
            {
                "WebFirmenID": pd.DataFrame(
                    {
                        "WebFirmenID": [1, 2],
                        "Firmenname": ["A GmbH", "B GmbH"],
                    }
                )
            }
        ),
        ComparisonStub(
            {
                "WebFirmenID": pd.DataFrame(
                    {
                        "WebFirmenID": [2, 3],
                        "Firmenname": ["B GmbH", "C GmbH"],
                    }
                ),
                "WebID": pd.DataFrame(
                    {
                        "WebID": [10],
                        "Name": ["Max"],
                    }
                ),
            }
        ),
    ]

    combined = combine_stammdaten_from_comparisons(comparisons)

    assert set(combined) == {"WebFirmenID", "WebID"}
    assert len(combined["WebFirmenID"]) == 3
    assert len(combined["WebID"]) == 1
