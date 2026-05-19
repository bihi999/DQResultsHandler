import pandas as pd
import pytest
import logging
import re
import pprint
from .abandoned_classes import ComparisonCompany_ID_wise

from enum import Enum


class ComparisonType(Enum):
    """
        Definition einer festen Menge der Arten von erwarteten Abgleichen.
    """
    FIRMENABGLEICH = "firmenabgleich"
    KONTAKTABGLEICH = "kontaktabgleich"


class ComparisonColumnSet(Enum):
    """
        Definition der festen Spaltengruppen, die zur Erkennung von Abgleichen benoetigt werden.
    """
    DEFAULT_COLUMN_NAMES = frozenset(["Nr.", "löschen", "%", "UnvollständigerDatensatz", "Tabelle"])
    CONTACT_FIELDS = frozenset(["Vorname", "Nachname", "Funktion_Freifeld", "Position", "E-Mail", "Position / Funktion_Freifeld"])


class ComparisonTypeDetectionError(Exception):
    """
        Wird geworfen, wenn aus den Spaltennamen kein unterstuetzter Abgleichstyp erkannt werden kann.
    """


class Comparison:
    """
        Steuert die Verarbeitung einer DQ-Ergebnisliste aus der Dublettensuche.

        Die Klasse nimmt eine vollständige, valide DQ-Ergebnistabelle entgegen, bereinigt sie für die weitere Verarbeitung und zerlegt sie anhand der
        DQ-Gruppennummern in einzelne Dublettengruppen. Je nach erkanntem Abgleichstyp werden daraus spezialisierte Gruppenobjekte erzeugt,
        verarbeitet und anschließend wieder zu klickerfähigen Dublettengruppen sowie auswertbaren Firmenrelationen zusammengeführt.

        Args:
            comparison_type (ComparisonType): Erkannter Abgleichstyp.
            comparison_columns (set[str]): Spaltennamen, die im DQ-Abgleich tatsächlich als Vergleichsdaten verwendet wurden.
            comparison_data (pd.DataFrame): DQ-Ergebnistabelle mit allen Zeilen und Metadaten der Dublettensuche.
            sourcefile (str): Pfad oder Dateiname der Quelldatei, aus der die Vergleichsdaten stammen.

        Attributes:
            comparison_count (int): Anzahl der erkannten Dublettengruppen. 
            doublet_groups (dict[int, object]): Nach DQ-Gruppennummer gespeicherte, spezialisierte Dublettengruppen.
            found_relations (pd.DataFrame): Zusammengeführte Firmenrelationen aus den verarbeiteten Dublettengruppen.
            reorganized_doublet_groups (pd.DataFrame): Neu aufgebaute Dublettengruppen für die weitere Bearbeitung im Klickertool.

    """

    class relation_firmen:
        REQUIRED_SCHEMA = {
            "firmentupel_apollo": "string",
            "WebFirmenID": "Int64",
            "Relation": "string",
            "Relation_staerke": "Int64",
        }

        __slots__ = tuple(REQUIRED_SCHEMA.keys())

        def __init__(self, firmentupel_apollo, WebFirmenID, Relation, Relation_staerke):
            self.firmentupel_apollo = firmentupel_apollo
            self.WebFirmenID = WebFirmenID
            self.Relation = Relation
            self.Relation_staerke = Relation_staerke

        def _key(self):
            return (
                self.firmentupel_apollo,
                self.WebFirmenID,
                self.Relation,
                self.Relation_staerke,
            )

        def __eq__(self, other):
            if not isinstance(other, type(self)):
                return NotImplemented
            return self._key() == other._key()

        def __hash__(self):
            return hash(self._key())

        def __repr__(self):
            return f"relation_firmen({self.firmentupel_apollo})"
    
    def __init__(self, comparison_type: ComparisonType, comparison_columns, comparison_data, sourcefile):
        self.comparison_type = comparison_type
        self.comparison_columns = comparison_columns
        self.comparison_data = comparison_data
        self.stammdaten = set()
        self.extract_stammdaten_and_normalize_column_names()
        self.sourcefile = sourcefile
        self.comparison_count = 0
        self.doublet_groups = {}
        self.found_relations = pd.DataFrame()
        self.reorganized_doublet_groups = pd.DataFrame()
        self.relation_firmen_instances = []

    @property
    def comparison_type(self):
        return self._comparison_type

    @comparison_type.setter
    def comparison_type(self, comparison_type):
        if not isinstance(comparison_type, ComparisonType):
            raise TypeError("comparison_type muss ein Wert der Klasse ComparisonType sein.")

        self._comparison_type = comparison_type
        
        
    
    @staticmethod
    def extract_comparison_columns(menge_aller_spaltennamen, logger=None):    
        
        """
            Excel-Tabellen von DQ enthalten standardmäßig einige Spalten die DQ erstellt.
            Davon wird das Vorhandensein von "Nr." bereits vorab geprüft - sonst keine Gruppenbildung möglich.
            Aus den sonstigen Spalten sind die ohne Sternchen im Abgleich enthalten, alle anderen nicht.
        """
        menge_DQ_spaltennamen = ComparisonColumnSet.DEFAULT_COLUMN_NAMES.value
        
        if isinstance(menge_aller_spaltennamen, pd.Index):
            if logger:
                logger.info("df.Index-Objekt erhalten. Umwandlung in Menge.")
            menge_aller_spaltennamen = set(menge_aller_spaltennamen)
        elif isinstance(menge_aller_spaltennamen, (set, list, tuple, frozenset)):
            if logger:
                logger.info("Spaltennamen als iterierbare Sammlung erhalten. Umwandlung in Menge.")
            menge_aller_spaltennamen = set(menge_aller_spaltennamen)
        else:
            if logger:
                logger.info("Kein df.Index-Objekt erhalten. Spaltennamen lassen sich nicht ermitteln.")
            return # Definierte Fehlermeldung nachtragen
        
        comparison_columns_df = set()

        for column_name in menge_aller_spaltennamen:
            if column_name not in menge_DQ_spaltennamen and "*" not in column_name:
                comparison_columns_df.add(column_name)
        
        if logger:
            logger.info(f"{len(comparison_columns_df)} Spalten im Abgleich verwendet: {' - '.join(comparison_columns_df)}")

        return comparison_columns_df
    
    
    
    @staticmethod
    def detect_comparison_type(comparison_column_names, menge_aller_spaltennamen, logger=None):
        
        """
            Anhand der Spaltennamen die im Abgleich enthalten sind, wird die Art des Abgleichs ermittelt.
        """
        if not isinstance(comparison_column_names, set) or not all(isinstance(column_name, str) for column_name in comparison_column_names):
            if logger:
                logger.error("comparison_column_names muss eine Menge aus Strings sein.")
            raise ComparisonTypeDetectionError("comparison_column_names muss eine Menge aus Strings sein.")

        if not isinstance(menge_aller_spaltennamen, set) or not all(isinstance(column_name, str) for column_name in menge_aller_spaltennamen):
            if logger:
                logger.error("menge_aller_spaltennamen muss eine Menge aus Strings sein.")
            raise ComparisonTypeDetectionError("menge_aller_spaltennamen muss eine Menge aus Strings sein.")

        menge_aller_spaltennamen = {
            Comparison._remove_column_name_stars(column_name)
            for column_name in menge_aller_spaltennamen
        }

        default_column_names = ComparisonColumnSet.DEFAULT_COLUMN_NAMES.value
        contact_fields = ComparisonColumnSet.CONTACT_FIELDS.value
        spaltennamen_ohne_default = menge_aller_spaltennamen - default_column_names

        firmenabgleich_pflichtfelder = {"WebFirmenID", "firmentupel_apollo"}
        firmenabgleich_ausschlussfelder = {"WebID", "ApolloMemberID"}
        pruefung_firmenabgleich_erfolgreich = (
            firmenabgleich_pflichtfelder.issubset(spaltennamen_ohne_default)
            and spaltennamen_ohne_default.isdisjoint(firmenabgleich_ausschlussfelder)
            and comparison_column_names.isdisjoint(contact_fields)
        )

        if pruefung_firmenabgleich_erfolgreich:
            if logger:
                logger.info("Abgleichstyp erfolgreich erkannt: %s", ComparisonType.FIRMENABGLEICH.value)
            return ComparisonType.FIRMENABGLEICH

        if logger:
            logger.info(
                "Pruefung auf %s nicht erfolgreich. Pflichtfelder vorhanden: %s. Ausschlussfelder abwesend: %s. Keine Kontaktfelder in Vergleichsspalten: %s.",
                ComparisonType.FIRMENABGLEICH.value,
                firmenabgleich_pflichtfelder.issubset(spaltennamen_ohne_default),
                spaltennamen_ohne_default.isdisjoint(firmenabgleich_ausschlussfelder),
                comparison_column_names.isdisjoint(contact_fields),
            )

        kontaktabgleich_pflichtfelder = {"WebID", "ApolloMemberID"}
        pruefung_kontaktabgleich_erfolgreich = kontaktabgleich_pflichtfelder.issubset(spaltennamen_ohne_default)

        if pruefung_kontaktabgleich_erfolgreich:
            if logger:
                logger.info("Abgleichstyp erfolgreich erkannt: %s", ComparisonType.KONTAKTABGLEICH.value)
            return ComparisonType.KONTAKTABGLEICH

        if logger:
            logger.info(
                "Pruefung auf %s nicht erfolgreich. Pflichtfelder vorhanden: %s.",
                ComparisonType.KONTAKTABGLEICH.value,
                kontaktabgleich_pflichtfelder.issubset(spaltennamen_ohne_default),
            )

            gepruefte_comparison_types = {ComparisonType.FIRMENABGLEICH, ComparisonType.KONTAKTABGLEICH}
            ungepruefte_comparison_types = set(ComparisonType) - gepruefte_comparison_types
            if ungepruefte_comparison_types:
                logger.warning(
                    "ComparisonType enthaelt ungepruefte Typen: %s",
                    ", ".join(comparison_type.value for comparison_type in ungepruefte_comparison_types),
                )

            logger.error("Abgleichsspalten %s lassen sich keinem unterstuetzten Abgleichstyp zuordnen.", "-".join(comparison_column_names))

        raise ComparisonTypeDetectionError("Aus den Spaltennamen konnte kein unterstuetzter Abgleichstyp erkannt werden.")

    
    
    # Data Frame zunächst zentral aufbereiten.

    ORDER_CLEAN_DATA = ["remove_nan_mask", "normalize_column_names", "turn_apolloid_to_apollofirmenid"]

    @staticmethod
    def _remove_column_name_stars(column_name):
        if isinstance(column_name, str):
            return column_name.replace("*", "")
        return column_name

    def extract_stammdaten_and_normalize_column_names(self):
        normalized_column_names = []

        for column_name in self.comparison_data.columns:
            normalized_column_name = self._remove_column_name_stars(column_name)
            if isinstance(column_name, str) and "*" in column_name:
                self.stammdaten.add(normalized_column_name)
            normalized_column_names.append(normalized_column_name)

        self.comparison_data.columns = normalized_column_names

    @staticmethod
    def _is_series_string_like(series):
        if pd.api.types.is_string_dtype(series.dtype):
            return True
        if pd.api.types.is_object_dtype(series.dtype):
            non_na = series.dropna()
            if non_na.empty:
                return True
            return all(isinstance(value, str) for value in non_na)
        return False

    @staticmethod
    def _is_series_int_like(series):
        if str(series.dtype) == "Int64":
            return True
        return pd.api.types.is_integer_dtype(series.dtype)

    @staticmethod
    def _coerce_to_int64_with_logging(dataframe, column_name, logger):
        original_series = dataframe[column_name]
        numeric_series = pd.to_numeric(original_series, errors="coerce")

        non_numeric_mask = numeric_series.isna() & original_series.notna()
        non_numeric_count = int(non_numeric_mask.sum())
        if non_numeric_count:
            logger.warning("Spalte '%s': %s nicht-numerische Werte zu NA gesetzt.", column_name, non_numeric_count)

        decimal_mask = numeric_series.notna() & ((numeric_series % 1) != 0)
        decimal_count = int(decimal_mask.sum())
        if decimal_count:
            logger.warning("Spalte '%s': %s Dezimalwerte vor Int-Cast gerundet.", column_name, decimal_count)
            numeric_series = numeric_series.round(0)

        dataframe[column_name] = numeric_series.astype("Int64")

    def prepare_relation_firmen_dataframe(self, dataframe, logger, strict=True):
        """
            Prueft und normalisiert einen DataFrame fuer Comparison.relation_firmen.
        """
        schema = self.relation_firmen.REQUIRED_SCHEMA
        required_columns = list(schema.keys())

        missing_columns = [column_name for column_name in required_columns if column_name not in dataframe.columns]
        extra_columns = [column_name for column_name in dataframe.columns if column_name not in required_columns]

        if missing_columns:
            logger.error("DataFrame fuer relation_firmen fehlt folgende Spalten: %s", missing_columns)
            return None

        if strict and extra_columns:
            logger.error("DataFrame fuer relation_firmen enthaelt unerlaubte zusaetzliche Spalten: %s", extra_columns)
            return None

        prepared_dataframe = dataframe.copy()
        dtype_errors = []

        for column_name, expected_dtype in schema.items():
            series = prepared_dataframe[column_name]

            if expected_dtype == "string":
                if self._is_series_string_like(series):
                    prepared_dataframe[column_name] = series.astype("string")
                    continue

                logger.warning("Spalte '%s': falscher Typ %s, versuche Umwandlung in string.", column_name, series.dtype)
                try:
                    prepared_dataframe[column_name] = series.astype("string")
                except Exception as error:
                    dtype_errors.append(f"Spalte '{column_name}' konnte nicht in string konvertiert werden: {error}")

            elif expected_dtype == "Int64":
                if self._is_series_int_like(series):
                    prepared_dataframe[column_name] = series.astype("Int64")
                    continue

                logger.warning("Spalte '%s': falscher Typ %s, versuche Umwandlung in Int64.", column_name, series.dtype)
                try:
                    self._coerce_to_int64_with_logging(prepared_dataframe, column_name, logger)
                except Exception as error:
                    dtype_errors.append(f"Spalte '{column_name}' konnte nicht in Int64 konvertiert werden: {error}")
            else:
                dtype_errors.append(f"Spalte '{column_name}' hat nicht unterstuetzten Zieltyp {expected_dtype}.")

        if dtype_errors:
            for error_message in dtype_errors:
                logger.error("DataFrame fuer relation_firmen: %s", error_message)
            return None

        logger.info("DataFrame fuer relation_firmen erfolgreich vorbereitet.")
        return prepared_dataframe[required_columns]

    def create_relation_firmen_instances(self, dataframe, logger, deduplicate=True):
        prepared_dataframe = self.prepare_relation_firmen_dataframe(dataframe, logger)
        if prepared_dataframe is None:
            return []

        instances = []
        for row_index, row in prepared_dataframe.iterrows():
            try:
                instances.append(self.relation_firmen(**{column_name: row[column_name] for column_name in self.relation_firmen.REQUIRED_SCHEMA}))
            except Exception as error:
                logger.error("relation_firmen-Instanz konnte in Zeile %s nicht erzeugt werden: %s", row_index, error)

        if deduplicate:
            instances = list(set(instances))

        self.relation_firmen_instances = instances
        logger.info("%s relation_firmen-Instanzen in Comparison gespeichert.", len(self.relation_firmen_instances))
        return self.relation_firmen_instances

    def remove_nan_mask(self, logger):
        """
            DQ schreibt in manchen Spalten bei fehlenden Werten ein ---
            Wirkt für die Auswertung störend - Regel der Anwendung auch nicht klar.
        """

        self.comparison_data.replace("---", "", inplace=True)
        logger.info("Bereinigung der NaN für {} begonnen.".format(self.sourcefile.split("\\")[-1]))

    def normalize_column_names(self, logger):
        """
            Zugriff auf Spalten erschwert, wenn unterschiedliche Schreibweisen zu berücksichtigen sind.
            Entfernen der Sternchen aus den Spaltennamen.
        """
        logger.info("Bereinigung Spaltennamen für {} begonnen.".format(self.sourcefile.split("\\")[-1]))
        logger.info("Bestehende Spaltennamen: {}".format(self.comparison_data.columns))
        self.extract_stammdaten_and_normalize_column_names()
        logger.info("Angepasste Spaltennamen: {}".format(self.comparison_data.columns))

    def turn_apolloid_to_apollofirmenid(self, logger):
        """
            Aus praktischen Gründen soll weiterhin die Möglichkeit bestehen nicht aufbereitete Leadtabellen als Firmenabgleich
            zu nutzen. Solche Tablennen haben dann eine ApolloID und deren Firmenbestandteil ist zunächst zu entfernen.
        """
        if "ApolloID" in self.comparison_data.columns:
            logger.info("Spalte ApolloID wird neu erstellt als ApolloFirmenID.")
            self.comparison_data["ApolloFirmenID"] = self.comparison_data["ApolloID"].str.split("_").str[1]
            self.comparison_data.drop(columns="ApolloID", inplace=True)

    def run_data_cleaning(self, logger):
        for function_name in self.ORDER_CLEAN_DATA:
            getattr(self, function_name)(logger)

    def detect_doublet_groups(self, logger):
        """
            Iteriere über self.comparison_data und zerlege ihn anhand der Nummern in der Spalte
            "Nr." in seine Dublettengruppen. Befülle dafür zunächst self.comparison_count mit der höchsten
            Zahl in dieser Spalte. Iteriere dann über die Zahlen 1 bis zu dieser Zahl, erzeuge je ein Subset
            für jede Zahl aka Dublettengruppe und hänge sie ComparisonContact-Instanz in self.doublet_groups.
        """

        self.comparison_count = self.comparison_data["Nr."].max()
        single_doublet_group = pd.DataFrame()
        
        logger.info("In {} wurden {} Dublettengruppen gefunden.".format(self.sourcefile.split("\\")[-1], self.comparison_count))
        if self.comparison_type == ComparisonType.FIRMENABGLEICH:
             for _int in range(1, self.comparison_count + 1):
                single_doublet_group = self.comparison_data[self.comparison_data["Nr."] == _int]
                self.doublet_groups[_int] = ComparisonCompany_ID_wise(single_doublet_group, _int, self.comparison_columns)
        else:
            logger.info("Skript verarbeitet aktuell nur Firmenabgleiche. Erkannter Abgleichstyp {} wird nicht unterstützt.".format(self.comparison_type.value))
        
                        
        return

    def process_doubletgroups(self, logger):
        """
            Wenn Dublettengruppen vorliegen für die Comparison-Instanz, durchlaufe sie und prüfe zu welche Klasse von Abgleichen sie gehören.
            Prüfe dann anhand der klasseneigenen test_prequisites - Methode ob sie (die Dublettengruppe) vearbeitet werden kann.
            Wenn ja, rufe die klasseneigene run - Methode auf, in der alle anzuwendenden Methoden gebündelt werden.

            Aktuell nur implementiert für ComparisonCompany_ID_wise
            
        """
        
        if self.doublet_groups:
            for key, value in self.doublet_groups.items():
                #logger.info("{} - {}".format(key, value))
                if isinstance(self.doublet_groups[key], ComparisonCompany_ID_wise):
                    self.doublet_groups[key].test_prerequisites(logger)
                    if self.doublet_groups[key].prerequisites:
                        self.doublet_groups[key].run(logger)
                    else:
                        logger.info("Dublettengruppe enthält nicht die zur Vereinzelung benötigten Spaltennamen: {}".format(self.doublet_groups[key].doubletgroup_dataframe.columns))
        else:
            logger.info("Die Comparison-Instanz für {} enthält keine in self.doublet_goups keine Einträge.".format(self.sourcefile))

    def print_summary(self, logger):
        logger.info("Zusammenfassung für die Abgleichsdaten aus {}".format(self.sourcefile.split("\\")[-1]))
        logger.info("{} Dublettengruppen mit {} Zeilen wurden übergeben und {} als Abgleichsart ermittelt.".format(self.comparison_data["Nr."].max(), len(self.comparison_data), self.comparison_type))
    
    def summarize_company_relations(self, logger): # Die Relationen sind unterschiedlicher Art je nach Abgleichstyp. Die Subklassen müssen sie selbst extrahieren und sie werden nur noch zusammengeführt als df
        """
            Nach Verarbeitung der Dublettengruppen - die klassenspezifisch erfolgt - müssen die Ergebnisse eingesammelt werden, hier die Relationen.
            Je Comparison - Instanz ist eine Tabelle und ein Abgleichstyp vorhanden; die Dublettengruppen können davon nicht abweichen.
            Durchlaufe
        """
        
        
        logger.info("{} Dublettengruppen sind vorhanden in der Comparison-Instanz".format(len(self.doublet_groups)))
        sr_counter = 0
        for key, doublet_group in self.doublet_groups.items():
            if isinstance(self.doublet_groups[key], ComparisonCompany_ID_wise):
                if sr_counter == 0:
                    self.found_relations = pd.DataFrame(columns = doublet_group.relations.columns)
                    # Hier geht die erste Connection verloren - sie übergibt nur ihre Spaltennamen, wird dann aber nicht weitere beachtet.
                    sr_counter += 1
                else:
                    self.found_relations = pd.concat([self.found_relations, doublet_group.relations], ignore_index = True)
                    sr_counter += 1
            else:
                logger.info("Für diese Klasse sind die Relationen nicht defininiert und können nicht zusammengeführt werden.")

        logger.info("{} Dublettengruppen wurden durchlaufen und {} Firmenrelatione(n) extrahiert.".format(sr_counter, len(self.found_relations)))

        return
            

    def summarize_doublet_groups(self, logger):
        """
            lorem ipsum
        """

        logger.info("{} Dublettengruppen sind vorhanden in der Comparison-Instanz".format(len(self.doublet_groups)))
        sr_counter = 0
        for key, doublet_group in self.doublet_groups.items():
            logger.info("{} enthält {} Einträge in der {} für neugebildete Dublettengruppen".format(doublet_group.doubletgroup_number, len(doublet_group.new_doubletgroups), type(doublet_group.new_doubletgroups)))
            
            for list_entry in doublet_group.new_doubletgroups:    
                if sr_counter == 0:
                    self.reorganized_doublet_groups = pd.DataFrame(columns = list_entry.columns)
                    self.reorganized_doublet_groups = pd.concat([self.reorganized_doublet_groups, list_entry], ignore_index = True)
                    sr_counter += 1
                else:
                    self.reorganized_doublet_groups = pd.concat([self.reorganized_doublet_groups, list_entry], ignore_index = True)
                    sr_counter += 1
            
        logger.info("{} Dublettengruppen wurden durchlaufen und {} Firmenrelatione(n) extrahiert.".format(sr_counter, len(self.found_relations)))
        
        return   
           
            



    
            
