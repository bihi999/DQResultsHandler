import pandas as pd
import pytest
import logging
import re
import pprint


class Comparison:
    """
        Steuert die Verarbeitung einer DQ-Ergebnisliste aus der Dublettensuche.

        Die Klasse nimmt eine vollständige, valide DQ-Ergebnistabelle entgegen,
        bereinigt sie für die weitere Verarbeitung und zerlegt sie anhand der
        DQ-Gruppennummern in einzelne Dublettengruppen. Je nach erkanntem
        Abgleichstyp werden daraus spezialisierte Gruppenobjekte erzeugt,
        verarbeitet und anschließend wieder zu klickerfähigen Dublettengruppen
        sowie auswertbaren Firmenrelationen zusammengeführt.

        Args:
            comparison_type (str): Erkannter Abgleichstyp, z. B.
                "Firmenabgleich_name", "Firmenabgleich_domain",
                "Firmenabgleich_domain_name" oder "Kontaktabgleich_name".
            comparison_columns (set[str]): Spaltennamen, die im DQ-Abgleich
                tatsächlich als Vergleichsdaten verwendet wurden.
            comparison_data (pd.DataFrame): DQ-Ergebnistabelle mit allen Zeilen
                und Metadaten der Dublettensuche.
            sourcefile (str): Pfad oder Dateiname der Quelldatei, aus der die
                Vergleichsdaten stammen.

        Attributes:
            default_column_names (set[str]): DQ-Metadatenspalten, die nicht zu
                den fachlichen Vergleichsdaten gehören.
            contact_fields (set[str]): Kontaktbezogene Spalten, die in einem
                reinen Firmenabgleich nicht vorkommen dürfen.
            comparison_count (int): Anzahl der erkannten Dublettengruppen.
            doublet_groups (dict[int, object]): Nach DQ-Gruppennummer
                gespeicherte, spezialisierte Dublettengruppen.
            found_relations (pd.DataFrame): Zusammengeführte Firmenrelationen
                aus den verarbeiteten Dublettengruppen.
            reorganized_doublet_groups (pd.DataFrame): Neu aufgebaute
                Dublettengruppen für die weitere Bearbeitung im Klickertool.

    """
    # Die Klasse sollte speichern, aus welchem Dokument ihre Daten kommen.

    # Als FACTORY-KLASSE erhält Comparison ALLE Daten die valide DQ-Daten sind - was dann später damit möglich ist
    # bestimmt die Klasse je selbst. Sie ist dafür die Schaltzentrale.

    # Darum muss sie auch je prüfen, ob sie bspw. in Ermangelung von IDs überhaupt ein Abgleich möglich bzw. sinnvoll ist.

    # Dort wo nicht sinnvoll, muss sie das abfangen und benennen / loggen.

    def __init__(self, comparison_type, comparison_columns, comparison_data, sourcefile):
        self.comparison_type = comparison_type
        self.comparison_columns = comparison_columns
        self.comparison_data = comparison_data
        self.sourcefile = sourcefile
        self.comparison_count = 0
        self.doublet_groups = {}
        self.found_relations = pd.DataFrame()
        self.doubletgroup_counter = 0
        self.reorganized_doublet_groups = pd.DataFrame()
        
        
    default_column_names = set(["Nr.", "löschen", "%", "UnvollständigerDatensatz", "Tabelle"])
    contact_fields = {"Vorname", "Nachname", "Funktion_Freifeld", "Position", "E-Mail", "Position / Funktion_Freifeld"}
    
    @staticmethod
    def extract_comparison_columns(menge_aller_spaltennamen, menge_DQ_spaltennamen, logger):    
        
        if isinstance(menge_aller_spaltennamen, pd.Index):
            logger.info("df.Index-Objekt erhalten. Umwandlung in Menge.")
            menge_aller_spaltennamen = set(menge_aller_spaltennamen)
        else:
            logger.info("Kein df.Index-Objekt erhalten. Spaltennamen lassen sich nicht ermitteln.")
            return # Definierte Fehlermeldung nachtragen
        
        comparison_columns_df = set()

        for column_name in menge_aller_spaltennamen:
            if column_name not in menge_DQ_spaltennamen and "*" not in column_name:
                comparison_columns_df.add(column_name)
        
        logger.info(f"{len(comparison_columns_df)} Spalten im Abgleich verwendet: {' - '.join(comparison_columns_df)}")

        return comparison_columns_df
    
    
    
    @staticmethod
    # anforderung: eine eigene funktion soll prüfen ob überhaupt paare von ids gebildet werden können
    # das wäre dann optional eine prüfung in __main__ die zur verfügung steht
    # es war keine initiale anforderung, daher nicht in detect_comparison_type reinprügeln
    # nicht hart vercoden hier - die Namen der ID-Spalten müssen in der Klasse verwaltet werden
    def check_for_ids_in_columns():
        pass
    


    @staticmethod
    def detect_comparison_type(comparison_column_names, contact_fields, logger):
        
        detect_comparison_type_return_string = ""

        # Vorname und Nachname ist immer Abgleich auf Kontakte
        if {"Vorname", "Nachname"}.issubset(comparison_column_names):
            detect_comparison_type_return_string = "Kontaktabgleich_name"
            logger.info("abgleichstyp: Kontaktabgleich_name")
        
        # Nur Firmenname und keine individuellen Suchfelder ist immer Abgleich auf Firmen(name)
        elif (comparison_column_names.isdisjoint(contact_fields)  
            and comparison_column_names == {"Firmenname"})            :
            detect_comparison_type_return_string = "Firmenabgleich_name"
            logger.info("abgleichstyp: Firmenabgleich_name")
        
        elif (comparison_column_names == {"domain"} and
              comparison_column_names.isdisjoint(contact_fields)):
            detect_comparison_type_return_string = "Firmenabgleich_domain"
            logger.info("abgleichstyp: Firmenabgleich_domain")
        
        elif (comparison_column_names.isdisjoint(contact_fields) and
              comparison_column_names == {"Firmenname", "domain"}):
             detect_comparison_type_return_string = "Firmenabgleich_domain_name"
             logger.info("abgleichstyp: Firmenabgleich_domain_name")
        
        else:
            detect_comparison_type_return_string = "Unbekannt"
            logger.info("Ableichsspalten {} lassen sich keinem abgleichstyp zuordnen".format(("-").join(comparison_column_names)))


        
        return detect_comparison_type_return_string

    
    
    # Data Frame zunächst zentral aufbereiten.

    ORDER_CLEAN_DATA = ["remove_nan_mask", "normalize_column_names", "turn_apolloid_to_apollofirmenid"]

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
        self.comparison_data.columns = self.comparison_data.columns.str.replace(r"\*", "", regex=True)
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
        if self.comparison_type in ["Firmenabgleich_name", "Firmenabgleich_domain", "Firmenabgleich_domain_name"]:
             for _int in range(1, self.comparison_count + 1):
                single_doublet_group = self.comparison_data[self.comparison_data["Nr."] == _int]
                self.doublet_groups[_int] = ComparisonCompany_ID_wise(single_doublet_group, _int, self.comparison_columns)
        else:
            logger.info("Skript verarbeitet aktuell nur Firmenabgleiche. Erkannter Abgleichstyp {} wird nicht unterstützt.".format(self.comparison_type))
        
                        
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
            Alt:
            ComparisonContact-Instanzen durchlaufen und Relationen extrahieren mit deren Klassenmethoden.
            Zusammenfassen der danach befüllten Attribute in self.found_relations

            Neu:
            Nach Verarbeitung der Dublettengruppen - die klassenspezifisch erfolgt - müssen die Ergebnisse eingesammelt werden, hier die Relationen.
            Je Comparison - Instanz ist eine Tabelle und ein Abgleichstyp vorhanden; die Dublettengruppen können davon nicht abweichen.
            Durchlaufe

            Relationen zwischen Firmen - aktuell keine Klasse - enthalten:
                ApolloFirmenID
                Firmenname
                Ort
                WebFirmenID
                Relation
                Relation_staerke
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
                    
                    #pprint.pprint(self.reorganized_doublet_groups)
                    #pprint.pprint(list_entry)
                    #print(type(list_entry))



                    self.reorganized_doublet_groups = pd.concat([self.reorganized_doublet_groups, list_entry], ignore_index = True)
                    sr_counter += 1
                else:
                    self.reorganized_doublet_groups = pd.concat([self.reorganized_doublet_groups, list_entry], ignore_index = True)
                    sr_counter += 1
            
        logger.info("{} Dublettengruppen wurden durchlaufen und {} Firmenrelatione(n) extrahiert.".format(sr_counter, len(self.found_relations)))
        
        return   
           
            



    
            
