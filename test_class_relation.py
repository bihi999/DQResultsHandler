import logging

import pandas as pd

from classes import class_relation as cr


def test_relation_pipeline_uses_firmentupel_apollo_schema():
    logger = logging.getLogger(__name__)
    relations = pd.DataFrame(
        {
            "firmentupel_apollo": ["apollo-1", "apollo-1", "apollo-2"],
            "WebFirmenID": [100, 100, 200],
            "Relation": ["Firmenname", "domain", "Firmenname"],
            "Relation_staerke": [90, 80, 95],
        }
    )

    valid_dataframes = cr.prepare_dataframes([relations], logger)
    instances = cr.create_instances(valid_dataframes, logger)
    grouped = cr.reorganize_instances(instances, logger)
    result = cr.build_relation_dataframe(grouped, logger)

    assert "firmentupel_apollo" in result.columns
    assert "WebFirmenID" in result.columns
    assert "ApolloFirmenID" not in result.columns
    assert "Ort" not in result.columns
    assert "Firmenname" not in result.columns
    assert len(result) == 2
