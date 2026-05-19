import logging
import pytest
import pprint
from pathlib import Path
from datetime import datetime
import pandas as pd
import openpyxl


from classes import class_comparison as cc
from classes import class_relation as cr
from classes.abandoned_classes import ComparisonCompany_ID_wise

from excel_functions import excel_classes as ec

from helper_functions.pandas_functions import export_dataframe_to_excel


"""
    Abgleichsergebnisse aus dem DQ-Tool liegen als Excel-Tabellen vor. Unterschiedliche Abgleiche für gleiche Firmen und Kontakte sollen
    zusammengeführt werden. Die zusammengeführten Abgleiche müssen einheitlich mit ihren Stammdaten angereichert und als zu entscheidende
    Listen ausgegeben werden.


        DataFrames (dict) - Dateipfade sind die Schlüssel - die eingelesenen Frames die Werte
        DQFileFolder - IndividualFolderExcel-Objekt
        DQComparisonInstanzen (list) - Je Data Frame ein Comparison - Objekt - organisiert als Liste 


"""

DataFrames = {}
DQComparisonInstanzen = []
found_relations = []
found_relations_valid = []

skript_pfad = Path(__file__).parent
logfile_pfad = skript_pfad / "logfile_DQResultshandler.txt"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(module)s - %(message)s', datefmt="%Y-%m-%d %H:%M", filename = logfile_pfad, filemode = "w")
logger = logging.getLogger(__name__)
logger.info("\n" * 3)
logger.info("=== Skript gestartet am %s ===", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


ExportEqualsImport = False
ExportPath = ""

ImportPath = "C:\\Users\\BirgerHildenbrandt\\OneDrive - Quadriga Hochschule Berlin GmbH\\Desktop\\abgleich_temp"

if ExportEqualsImport or not ExportPath:
    logger.info("Exportpfad gleich Importpfad weil keine Info oder vorgegeben.")
    ExportPath = ImportPath

if ec.IndividualFolderExcel.is_readable_directory(ImportPath):
    DQFileFolder = ec.IndividualFolderExcel(ImportPath)
    DQFileFolder.file_list = DQFileFolder.list_visible_files()
    DQFileFolder.extract_filenames()
    DQFileFolder.get_excel_files()
    DQFileFolder.extract_filenames(True)
    DQFileFolder.filter_excel_files_by_columns(["Nr."], False)
    DQFileFolder.extract_filenames(True)
else:
    logger.info("Verzeichnis kann nicht instanziiert werden.")


if DQFileFolder.excel_file_list:
    DataFrames = DQFileFolder.load_all_excel_files_as_dataframes(logger)


for quelldatei, _DataFrame in DataFrames.items():
    
    menge_abgleichsspalten = set()
    menge_abgleichsspalten = cc.Comparison.extract_comparison_columns(_DataFrame.columns, cc.Comparison.default_column_names, logger)
    
    
    if menge_abgleichsspalten:
        string_abgleichstyp = cc.Comparison.detect_comparison_type(menge_abgleichsspalten, cc.Comparison.contact_fields, logger)
    else:
        logger.info("Es wurden keine Abgleichsspalten ermittelt - keine Auswertung möglich.")
        continue

    DQComparisonInstanzen.append(cc.Comparison(string_abgleichstyp, menge_abgleichsspalten, _DataFrame, quelldatei))





