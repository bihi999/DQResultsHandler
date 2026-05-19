import pandas as pd
import logging


ID_COLUMNS = ("ApolloKontaktID", "WebFirmenID", "WebID", "firmentupel_apollo")


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
