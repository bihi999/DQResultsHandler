import os
import pandas as pd
from datetime import datetime
import re
import pprint
import logging

def excel_liste_einlesen(excel_liste_dateipfad: str, log_datei: str = None):
    """
    Liest eine Excel-Datei ein und überprüft die Struktur.
    Erwartete Spalten: "ID", "Jobtitel".
    Gibt den Dateipfad und den DataFrame zurück.
    """
       
    logging.info("Beginn des Skriptlaufs")
    logging.info(f"Eingelesene Datei: {excel_liste_dateipfad}")
    
    # Überprüfung, ob die Datei existiert
    if not os.path.isfile(excel_liste_dateipfad):
        fehler = "Fehler: Die Datei existiert nicht oder der Pfad ist ungültig."
        logging.info(fehler)
        return fehler
    
    # Überprüfung, ob es eine Excel-Datei ist
    if not excel_liste_dateipfad.lower().endswith(('.xls', '.xlsx')):
        fehler = "Fehler: Die Datei ist keine Excel-Datei."
        logging.info(fehler)
        return fehler
    
    try:
        # Datei einlesen
        with pd.ExcelFile(excel_liste_dateipfad, engine='openpyxl') as excel_file:
            excel_liste_dataframe = pd.read_excel(excel_file)
        
        # Überprüfung, ob die erforderlichen Spalten vorhanden sind
        erforderliche_spalten = {"ID", "Jobtitel"}
        if not erforderliche_spalten.issubset(excel_liste_dataframe.columns):
            fehler = "Fehler: Die Datei enthält nicht die erwarteten Spalten ('ID', 'Jobtitel')."
            logging.info(fehler)
            return fehler
        
        logging.info("Datei erfolgreich eingelesen und überprüft.")
        return excel_liste_dataframe
    
    except Exception as e:
        fehler = f"Fehler beim Einlesen der Excel-Datei: {str(e)}"
        logging.info(fehler)
        return fehler

def export_functions_to_excel(functions: dict, folder_path: str, filename: str = "functions_export.xlsx"):
    # Sheet 1: Unique normalisierte Titel mit Frequenz
    normalized_data = {}
    for obj in functions.values():
        key = obj.function_normalized
        if key in normalized_data:
            normalized_data[key] += obj.frequency
        else:
            normalized_data[key] = obj.frequency

    df_normalized = pd.DataFrame([
        {"function_normalized": fn, "frequency": freq}
        for fn, freq in normalized_data.items()
    ]).sort_values(by="frequency", ascending=False)

    # Sheet 2: Alle Original-Normalisiert-Paare
    df_pairs = pd.DataFrame([
        {"function": obj.function, "function_normalized": obj.function_normalized, "function_unified" : obj.function_unified}
        for obj in functions.values()
    ])

    # Erstelle vollständigen Pfad zur Datei
    filepath = os.path.join(folder_path, filename)

    # Schreibe beide DataFrames in je ein Tabellenblatt
    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        df_normalized.to_excel(writer, sheet_name="Normalized Frequency", index=False)
        df_pairs.to_excel(writer, sheet_name="Function Mapping", index=False)

    print(f"Excel-Datei wurde erfolgreich exportiert: {filepath}")