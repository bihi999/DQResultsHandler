import os
import pandas as pd

def export_dataframe_to_excel(df: pd.DataFrame, pfad: str, dateiname: str, logger, zusatz: str = "") -> str:
    """
    Exportiert ein DataFrame als Excel-Datei ohne Index.
    
    Parameter:
        df (pd.DataFrame): Das DataFrame, das exportiert werden soll.
        pfad (str): Zielordner für die Datei.
        dateiname (str): Dateiname (muss .xlsx enthalten).
        zusatz (str, optional): Zusatz, der vor der Dateiendung eingefügt wird.
    
    """
   
    # Dateiendung trennen
    basis, ext = os.path.splitext(dateiname)
    if not ext:  # falls keine Endung angegeben, standardmäßig .xlsx
        ext = ".xlsx"
    
    # Zusatz einfügen (falls angegeben)
    if zusatz:
        basis = f"{basis}{zusatz}"
    
    # Vollständigen Pfad bauen
    fullpath = os.path.join(pfad, basis + ext)
    
    # Export ohne Index
    df.to_excel(fullpath, index=False, engine="openpyxl")
    
    return 
