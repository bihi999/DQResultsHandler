import pandas as pd
import logging
from enum import Enum


class IdColumn(Enum):
    APOLLO_KONTAKT_ID = "ApolloKontaktID"
    WEB_FIRMEN_ID = "WebFirmenID"
    WEB_ID = "WebID"
    FIRMENTUPEL_APOLLO = "firmentupel_apollo"


ID_COLUMNS = tuple(id_column.value for id_column in IdColumn)


def evaluate_stammdaten_dataframes(dataframe, columns_to_delete=None, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)

    working_dataframe = dataframe.copy()

    if columns_to_delete:
        columns_to_drop = [column_name for column_name in columns_to_delete if column_name in working_dataframe.columns]
        if columns_to_drop:
            working_dataframe = working_dataframe.drop(columns=columns_to_drop)
            logger.info("Spalten vor Stammdaten-Auswertung geloescht: %s", columns_to_drop)

    stammdaten = {}

    for id_column in ID_COLUMNS:
        if id_column not in working_dataframe.columns:
            logger.info("ID-Spalte '%s' nicht im DataFrame vorhanden.", id_column)
            continue

        id_dataframe = working_dataframe.copy()
        id_series = id_dataframe[id_column]
        has_value_mask = id_series.notna() & (id_series.astype("string").str.strip() != "")
        id_dataframe = id_dataframe[has_value_mask].copy()

        if id_dataframe.empty:
            logger.info("ID-Spalte '%s' vorhanden, aber ohne befuellte Zeilen.", id_column)
            continue

        id_dataframe = id_dataframe.replace(r"^\s*$", pd.NA, regex=True)
        id_dataframe = id_dataframe.dropna(axis=1, how="all")
        id_dataframe = id_dataframe.drop_duplicates()

        if not id_dataframe.empty:
            stammdaten[id_column] = id_dataframe
            logger.info("Stammdaten-DataFrame fuer '%s' erstellt: %s Zeilen, %s Spalten.", id_column, len(id_dataframe), len(id_dataframe.columns))

    logger.info("Stammdaten-Dictionary enthaelt %s Eintraege.", len(stammdaten))

    return stammdaten


def enrich_dataframe_with_stammdaten(stammdaten, dataframe, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)

    enriched_dataframe = dataframe.copy()

    if not isinstance(stammdaten, dict):
        logger.error("stammdaten muss ein Dict sein.")
        return enriched_dataframe

    for id_column in ID_COLUMNS:
        if id_column not in stammdaten:
            logger.info("Keine Stammdaten fuer ID-Spalte '%s' vorhanden.", id_column)
            continue

        if id_column not in enriched_dataframe.columns:
            logger.info("ID-Spalte '%s' nicht im Ziel-DataFrame vorhanden.", id_column)
            continue

        reference_dataframe = stammdaten[id_column]
        if not isinstance(reference_dataframe, pd.DataFrame):
            logger.error("Stammdaten fuer '%s' sind kein DataFrame.", id_column)
            continue

        if id_column not in reference_dataframe.columns:
            logger.error("Stammdaten-DataFrame fuer '%s' enthaelt die ID-Spalte nicht.", id_column)
            continue

        value_columns = [column_name for column_name in reference_dataframe.columns if column_name != id_column]
        if not value_columns:
            logger.info("Stammdaten-DataFrame fuer '%s' enthaelt keine anzureichernden Spalten.", id_column)
            continue

        renamed_columns = {
            column_name: f"{id_column}_{column_name}"
            for column_name in value_columns
        }
        join_dataframe = reference_dataframe[[id_column] + value_columns].drop_duplicates(subset=[id_column]).rename(columns=renamed_columns)

        before_columns = set(enriched_dataframe.columns)
        enriched_dataframe = enriched_dataframe.merge(join_dataframe, on=id_column, how="left")
        added_columns = [column_name for column_name in enriched_dataframe.columns if column_name not in before_columns]

        logger.info(
            "Ziel-DataFrame ueber '%s' angereichert: %s neue Spalten, %s Referenzzeilen.",
            id_column,
            len(added_columns),
            len(join_dataframe),
        )

    logger.info("Anreicherung abgeschlossen: %s Zeilen, %s Spalten.", len(enriched_dataframe), len(enriched_dataframe.columns))
    return enriched_dataframe


def combine_stammdaten_from_comparisons(comparisons, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)

    combined_stammdaten = {}

    for comparison_index, comparison in enumerate(comparisons, start=1):
        stammdaten = getattr(comparison, "stammdaten", None)
        if not isinstance(stammdaten, dict):
            logger.info("Comparison %s enthaelt kein Stammdaten-Dict.", comparison_index)
            continue

        for id_column in ID_COLUMNS:
            if id_column not in stammdaten:
                continue

            dataframe = stammdaten[id_column]
            if not isinstance(dataframe, pd.DataFrame):
                logger.error("Comparison %s: Stammdaten fuer '%s' sind kein DataFrame.", comparison_index, id_column)
                continue

            if dataframe.empty:
                logger.info("Comparison %s: Stammdaten fuer '%s' sind leer.", comparison_index, id_column)
                continue

            combined_stammdaten.setdefault(id_column, []).append(dataframe.copy())
            logger.info("Comparison %s: Stammdaten fuer '%s' zum Zusammenfuehren vorgemerkt.", comparison_index, id_column)

    for id_column, dataframes in list(combined_stammdaten.items()):
        combined_dataframe = pd.concat(dataframes, ignore_index=True)
        combined_dataframe = combined_dataframe.drop_duplicates()
        combined_dataframe = combined_dataframe.dropna(axis=1, how="all")
        combined_stammdaten[id_column] = combined_dataframe
        logger.info(
            "Stammdaten fuer '%s' zusammengefuehrt: %s DataFrames, %s Zeilen, %s Spalten.",
            id_column,
            len(dataframes),
            len(combined_dataframe),
            len(combined_dataframe.columns),
        )

    logger.info("Zusammengefuehrtes Stammdaten-Dict enthaelt %s Eintraege.", len(combined_stammdaten))
    return combined_stammdaten
