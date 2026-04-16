import pandas as pd
import os
import logging
import pprint


class IndividualFolder:

    def __init__(self, path_string):
        self.path_string = path_string
        self.file_list = []


    @staticmethod
    def is_readable_directory(path: str) -> bool:
        """
        Prüft, ob der angegebene Pfad ein existierendes, lesbares Verzeichnis ist.

        Args:
            path (str): Der zu prüfende Verzeichnispfad.

        Returns:
            bool: True, wenn es sich um ein existierendes, lesbares Verzeichnis handelt, sonst False.
        """
        return os.path.isdir(path) and os.access(path, os.R_OK)
    
    
    def list_visible_files(self):
        """
        Gibt eine Liste aller sichtbaren Dateien im angegebenen Verzeichnis zurück (keine Unterverzeichnisse).
        Versteckte Dateien (z. B. mit . am Anfang) und Systemdateien werden ausgeschlossen.

        Args:
            directory_path (str): Der Pfad zum Verzeichnis.

        Returns:
            list: Liste von vollständigen Dateipfaden.
        """
        files = []
        for entry in os.listdir(self.path_string):
            full_path = os.path.join(self.path_string, entry)

            if (
                os.path.isfile(full_path) and          # Nur reguläre Dateien
                not entry.startswith('.') and          # Keine versteckten Dateien
                not os.path.basename(full_path).startswith('~')  # Optional: MS Office-Tempdateien etc.
            ):
                files.append(full_path)

        return files
    

import pandas as pd

class IndividualFolderExcel(IndividualFolder):
    def __init__(self, path_string):
        super().__init__(path_string)
        self.excel_file_list = []

    def get_excel_files(self):
        """
        Filtert self.file_list und speichert nur Excel-Dateien in self.excel_file_list.
        """
        self.excel_file_list = [
            file for file in self.file_list
            if file.lower().endswith(('.xls', '.xlsx', '.xlsm', '.xlsb'))
        ]

    @staticmethod
    def load_excel_as_dataframe(
        io,
        sheet_name=0,
        header=0,
        usecols=None,
        nrows=None,
        **kwargs
    ):
        """
        Lädt ein Excel-Blatt als DataFrame.
        
        Erlaubt explizite Angabe von:
        - sheet_name
        - header
        - usecols
        - nrows
        
        Weitere Parameter können per **kwargs übergeben werden und werden direkt an pd.read_excel weitergereicht.
        """
        return pd.read_excel(
            io=io,
            sheet_name=sheet_name,
            header=header,
            usecols=usecols,
            nrows=nrows,
            **kwargs
        )

    def filter_excel_files_by_columns(self, required_columns, _verbose=False):
        """
        Filtert self.excel_file_list so, dass nur Dateien übrig bleiben,
        die beim Einlesen alle in required_columns genannten Spalten enthalten.
        Loggt den Fortschritt.
        """
        valid_files = []
        for file_path in self.excel_file_list:
            logging.info(f"Prüfe Datei: {file_path}")
            try:
                df = IndividualFolderExcel.load_excel_as_dataframe(file_path, nrows=1)  # Nur Header einlesen
                
                if self._is_valid_dataframe(df):
                    logging.info(f"DataFrame erfolgreich geladen. Spalten: {list(df.columns)}")
                    
                    if _verbose:
                        logging.info(f"DataFrame-Vorschau:\n{df.head()}")
                    
                    if all(col in df.columns for col in required_columns):
                        logging.info(f"Datei akzeptiert: {file_path}")
                        valid_files.append(file_path)
                    else:
                        logging.info(f"Datei verworfen (fehlende Spalten): {file_path}")
                else:
                    logging.warning(f"Ungültiger DataFrame oder kein DataFrame eingelesen: {file_path}")
            
            except Exception as e:
                logging.error(f"Fehler beim Einlesen von Datei {file_path}: {str(e)}")
                # Datei ignorieren

        self.excel_file_list = valid_files

    def _is_valid_dataframe(self, df):
        """
        Prüft, ob das Objekt ein gültiger DataFrame ist und Spalten enthält.
        """
        return isinstance(df, pd.DataFrame) and df.columns is not None and len(df.columns) > 0



    def load_all_excel_files_as_dataframes(self, logger):
        """
        Lädt alle Dateien in self.excel_file_list als DataFrames
        und gibt eine Liste der DataFrames zurück.
        """
        dataframes = {}
        for file_path in self.excel_file_list:
            try:
                df = IndividualFolderExcel.load_excel_as_dataframe(file_path)
                dataframes[file_path] = df
            except Exception:
                logger.info("{} konnte nicht als DataFrame eingelesen werden.".format(file_path))
                continue
        return dataframes

    def extract_filenames(self, filtered_view = False):
        """
        Gibt eine Liste der Dateinamen (ohne Verzeichnispfad) zurück
        für die übergebene Liste von Dateipfaden.
        """
        if filtered_view:
            file_names = [os.path.basename(path) for path in self.excel_file_list]
        elif not filtered_view:
            file_names = [os.path.basename(path) for path in self.file_list]
        for _ in file_names:
            print(_)