import logging
import pytest
import pprint
from pathlib import Path
from datetime import datetime


from classes import class_comparison as cc
from classes import class_relation as cr
from excel_functions import excel_classes as ec
from helper_functions.pandas_functions import export_dataframe_to_excel

DataFrames = {}
DQComparisonInstanzen = []
found_relations = []
found_relations_valid = []

skript_pfad = Path(__file__).parent
logfile_pfad = skript_pfad / "logfile_DQResultshandler.txt"


# -------------------------MAIN-------------------------------------

# ImportPath -->> Pfad zur Übergabe und Prüfung an die ef - Funktionen
# ExportPath -->> Pfad zur Übergabe an die Comparison-Instanz in welche diese die Aufbereiteten Listen schreibt
# Export = Import - Schalter -->> Schalter in main - dann wird automatisch der Importpfad an die INstanz übergeben

# Zusammengefasster Call
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(module)s - %(message)s', datefmt="%Y-%m-%d %H:%M", filename = logfile_pfad, filemode = "w")
logger = logging.getLogger(__name__)
logger.info("\n" * 3)
logger.info("=== Skript gestartet am %s ===", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


ExportEqualsImport = False
ExportPath = ""

# ImportPath = "C:\\Users\\BirgerHildenbrandt\\OneDrive - Quadriga Hochschule Berlin GmbH\\Quadriga Sharepoint\\Teams\\Data Management\\MarketingServices\\10_Birger_Hildenbrandt\\0_POSTEINGANG\\01_PROJEKTE\\KLICKERTOOL"
ImportPath = "C:\\Users\\BirgerHildenbrandt\\OneDrive - Quadriga Hochschule Berlin GmbH\\General - Data Management\\30k_for_Quadriga"



if ExportEqualsImport or not ExportPath:
    logger.info("Exportpfad gleich Importpfad weil keine Info oder vorgegeben.")
    ExportPath = ImportPath

# Erstmal provisorisch die neu gestalteten ExcelKlassen integriert - es gibt noch keine expliziten Tests dafür.
# Die Excelfunktionen haben auch noch keinen Logger.

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

# Logik von class_comparison als FactoryKlasse wird hier durchbrochen - __main__ entscheidet über Klassenverwendung
# weil hier die Auswertung bereits genutzt wird und nicht innerhalb der cc-Instanz. JEDE Tabelle erhält eine Comparison-Instanz.
# Darin liegt dann die weitere Verarbeitung.
    
# Geschachere der Datentypen beenden: __main__ nur mit DataFrames.
# Mindestanforderungen an die Excel - Dateien sind vorher zu prüfen: Nur soweit sie überhaupt erst als DQ-Dateien erkennbar sein müssen
# Mengenlogik für die Spaltennamen usw. liegt aber dann alles in class_comparison\comparison als Factory - Klasse
for quelldatei, _DataFrame in DataFrames.items():
    
    menge_abgleichsspalten = set()
    menge_abgleichsspalten = cc.Comparison.extract_comparison_columns(_DataFrame.columns, cc.Comparison.default_column_names, logger)
    
    if menge_abgleichsspalten:
        string_abgleichstyp = cc.Comparison.detect_comparison_type(menge_abgleichsspalten, cc.Comparison.contact_fields, logger)
    else:
        logger.info("Es wurden keine Abgleichsspalten ermittelt - keine Auswertung möglich.")
        continue

    DQComparisonInstanzen.append(cc.Comparison(string_abgleichstyp, menge_abgleichsspalten, _DataFrame, quelldatei))


for DQComparisonInstanz in DQComparisonInstanzen:
    #logger.info("Beginne Bearbeitung für Instanz {}.".format(DQComparisonInstanz))
    
    DQComparisonInstanz.run_data_cleaning(logger)
    
    #print(DQComparisonInstanz.comparison_data.head())

    DQComparisonInstanz.detect_doublet_groups(logger)
    DQComparisonInstanz.process_doubletgroups(logger)
    DQComparisonInstanz.print_summary(logger)
    DQComparisonInstanz.summarize_company_relations(logger)
    DQComparisonInstanz.summarize_doublet_groups(logger)
    # export_dataframe_to_excel(DQComparisonInstanz.found_relations, ImportPath, DQComparisonInstanz.sourcefile, logger, "_relationen")
    # export_dataframe_to_excel(DQComparisonInstanz.reorganized_doublet_groups, ImportPath, DQComparisonInstanz.sourcefile, logger, "_dublettengruppen")
    found_relations.append(DQComparisonInstanz.found_relations)    

print(len(found_relations))
found_relations_valid =cr.prepare_dataframes(found_relations, logger)
relations_firma = cr.create_instances(found_relations_valid, logger)
export_dict = cr.reorganize_instances(relations_firma, logger)
relations_dataframe = cr.build_relation_dataframe(export_dict, logger)
export_dataframe_to_excel(relations_dataframe, ImportPath, "unknown.xlsx", logger, "_relations")

pprint.pprint(export_dict)