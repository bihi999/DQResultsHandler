import pandas as pd
import pytest
import logging
import re
import pprint
from collections import Counter


def extract_trigrams_with_frequency(text):
    """
        Ermittle alle überlappenden Trigramme eines Strings und gib sie als
        Menge von Tupeln im Format (trigramm, häufigkeit) zurück.
    """
    if not isinstance(text, str):
        raise TypeError("text muss ein String sein")

    if len(text) < 3:
        return set()

    trigrams = (text[index:index + 3] for index in range(len(text) - 2))
    trigram_counter = Counter(trigrams)

    return set(trigram_counter.items())


class Comparison:
    """
        Factory-Klasse um die Datenaufbereitung aus einer Ergebnisliste der Dublettensuche der DQ-Apps
        zu steuern und für die weitere Bearbeitung mit dem Klickertool (als GUI) aufzubereiten.

        Args    default_column_names (set) - Spaltennamen die DQ einem Abgleich mitgibt und die nicht zu den abgeglichenen Daten gehören # Konstante
                contact_fields (set) - Spaltennamen die inviduelle Daten darstellen und darum nicht in einem Firmenabgleich auftauchen dürfen

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
           
            


class ComparisonCompany_ID_wise:
    """
        Klasse zum Handling von Firmenabgleich_name, Firmenabgleich_domain und Firmenabgleich_domain_name - wenn IDs und Ortsangaben vorhanden sind.
        Die Klasse setzt ferner voraus, dass keine Kontaktdaten enthalten sind. Diese Voraussetzungen werden durch die Factory-Klasse geprüft, sie callt
        die Methoden.

        Dublettengruppen sind DataFrames.

        Relationen sind eigentlich Mengen.

        Datentypen/Typenkonvertierung muss hier mit rein.

        Args.
            self.doubletgroup_dataframe (df) - Data Frame mit allen Stammdaten aus einer Dublettengruppe im DQ-Abgleich
            self.doubletgroup_number (int) - Nummer der Dublettengruppe aus dem DQ-Abgleich
            self.doubletgroup_comparison_columns (set(str)) - Menge mit den Spaltennamen die im Abgleich enthalten sind
            self.companies_leads_dataframe (df) - DataFramen mit den Zeile aus self.doubletgroup_dataframe, die zu einem Lead gehören
            self.companies_crm_dataframe (df) - DataFramen mit den Zeile aus self.doubletgroup_dataframe, die zu einer Firma gehören
            self.new_doubletgroups (set) - Menge von Data Frames, die neu erstellten Dublettengruppen nach der Vereinzelung
            self.relations (df) - df der gefundenen Beziehungen zwischen Leadfirmen und CRM-Firmen - je 1:1 und unter Angabe von Abgleichstyp und Ähnlichkeit
            self.prerequisites (bool) (default: False) - Voraussetzungen für die Methoden sind IDs und eine Ortsangabe. Eine Dublettengruppe ohne bleibt auf False
            
    """
   
    def __init__(self, doubletgroup_dataframe, doubletgroup_number, doubletgroup_comparison_columns):
        self.doubletgroup_dataframe = doubletgroup_dataframe
        self.doubletgroup_number = doubletgroup_number
        self.doubletgroup_comparison_columns = doubletgroup_comparison_columns
        self.companies_leads_dataframe = pd.DataFrame()
        self.companies_crm_dataframe = pd.DataFrame()
        self.new_doubletgroups = [] 
        self.relations = pd.DataFrame(columns = ["ApolloFirmenID", "Firmenname", "Ort", "WebFirmenID", "Relation", "Relation_staerke"])
        self.prerequisites = False

    # Für vereinheitlichte run - Funktion muss Liste mit Funktionsnamen vorliegen.    
    ORDER = ["extract_lead_companies", "deduplicate_lead_companies", "extract_crm_companies", "build_new_doubletgroups", "extract_relations"]

    
    def test_prerequisites(self,logger):
        """
            Prüfe das Ort, WebFirmenID, Firmenname und ApolloID enthalten sind und setze self.prerequisites dann auf True - sonst Fehlerlog.
        """
        self.prerequisites = ("WebFirmenID" in self.doubletgroup_dataframe.columns
                                and "Ort" in self.doubletgroup_dataframe.columns
                                and "ApolloFirmenID" in self.doubletgroup_dataframe.columns
                                and "Firmenname" in self.doubletgroup_dataframe.columns)
        
        if self.prerequisites == False:
            logger.info("Dublettengruppe {} enthält nicht die erforderlichen Spalten für die Verzeinzelung.".format(self.doubletgroup_number))

    def extract_lead_companies(self, logger):
        """
            Erstelle einen DataFrame aus self.doubletgroup_dataframe der nur die Zeilen mit einer ApolloFirmenID enthölt.
        """

        self.companies_leads_dataframe = self.doubletgroup_dataframe[self.doubletgroup_dataframe["ApolloFirmenID"].notna() & (self.doubletgroup_dataframe["ApolloFirmenID"] != "")]
        #logger.info("Für Dublettengruppe {} wurden {} Zeilen mit Apollo-Firmeninformationen ermittelt.".format(self.doubletgroup_number, len(self.companies_leads_dataframe)))
        #logger.info(self.companies_leads_dataframe)
    
    def deduplicate_lead_companies(self, logger):
        """
            ApolloFirmenIDs sind nur in Verbindung mit einem Ort unique - so aktuell die Vorgehensweise, das muss aber nicht im Abgleich so umgesetzt werden,
            weil wir mit dem Ziel vieler Relationen abgleichen und den Abgleich daher nicht entsprechend begrenzen. Weitere Dubletten entstehen dadurch, dass 
            die Möglichkeit bestehen soll, nicht aufbereitete Leads-Listen auf Firmen abzugleichen und darum die gleiche Firma mehrfach auftreten kann.

            Praktische ließe sich auch allein aufgrund des Namens deduplizieren - aber später sollen die ApolloFirmenIDs für den Reimport ins CRM genutzt werden.
            Daher wird auf beide Werte zurückgegriffen.
        """
        initial_length = len(self.companies_leads_dataframe)
        self.companies_leads_dataframe = self.companies_leads_dataframe.drop_duplicates(subset=["ApolloFirmenID", "Ort"])
        #logger.info("Für Dublettengruppe {} bleiben nach Deduplizierung {} von {} Apollo-Firmen-Einträge erhalten.".format(self.doubletgroup_number, initial_length, len(self.companies_leads_dataframe)))

    def extract_crm_companies(self, logger):
        """
            Alle Zeilen mit einer WebfirmenID aus self.doubletgroup_dataframe ziehen und deduplizieren.
            Da diese ID im Ggs. zur ApolloFirmenID unique ist, Funktionen zusamengefasst.
        """

        self.companies_crm_dataframe = self.doubletgroup_dataframe[self.doubletgroup_dataframe["WebFirmenID"].notna() & (self.doubletgroup_dataframe["WebFirmenID"] != "")]
        initial_length = len(self.companies_crm_dataframe)
        self.companies_crm_dataframe = self.companies_crm_dataframe.drop_duplicates(subset=["WebFirmenID"])
        #logger.info("Für Dublettengruppe {} bleiben nach Deduplizierung {} von {} CRM-Firmen-Einträge erhalten.".format(self.doubletgroup_number, initial_length, len(self.companies_crm_dataframe)))
        #logger.info(self.companies_crm_dataframe)
    
    def build_new_doubletgroups(self, logger):
        """
            Kombiniere die gefundenen Firmen aus den Leads zeilenweise je mit allen CRM-Firmen.
            Fülle dabei die ApolloFirmenID nach unten aus - damit klickerfähige Suchvorschläge entstehen.
        """

        for _, row in self.companies_leads_dataframe.iterrows():
            new_doubletgroup = pd.DataFrame()
            new_doubletgroup = pd.DataFrame([row])
            new_doubletgroup = pd.concat([new_doubletgroup, self.companies_crm_dataframe], ignore_index = True)
            
            new_doubletgroup["ApolloFirmenID"] = new_doubletgroup["ApolloFirmenID"].replace(["", "nan", 0], pd.NA)
            new_doubletgroup["ApolloFirmenID"] = new_doubletgroup["ApolloFirmenID"].ffill()
            
            self.new_doubletgroups.append(new_doubletgroup)
            
    def extract_relations(self, logger):
        """
            Idee: Ansatz generalisieren und die gefundenen Relationen auf einer Ebene über
            dem einzelen Abgleich aka Comparison nutzbar zu machen.

            Herausforderung: Die Ausstattung mit Stammdaten ist u.U. unterschiedlich je Abgleich
            und eine Gewichtung / Auswertung unterschiedlicher Relationen ist konzeptionell 
            noch sehr verworren.

            Relevanz ist zu entscheiden: Wieviele Relationen finden die unterschiedlichen Abgleiche,
            die bei getrennter Verarbeitung unter den Tisch fallen würden.

            Begriff der Relation: Lead_Firma auf CRM_Firma (Namen und Ort gehören mit dazu, weil so ist
            unsere Definition einer Firma) - Art der Relation, ausgedrückt als prozentuale Ähnlichkeit
            von Textfeldern.
        """
        for doublet_group in self.new_doubletgroups:
            ApolloFirmenID = doublet_group.iloc[0]["ApolloFirmenID"]
            ApolloFirmenname = doublet_group.iloc[0]["Firmenname"]
            ApolloFirmenOrt = doublet_group.iloc[0]["Ort"]
            TypeRelation = "-".join(sorted(self.doubletgroup_comparison_columns))



            for _, row in doublet_group.iloc[1:].iterrows():
                new_relation = {"ApolloFirmenID" : ApolloFirmenID,
                                "Firmenname" : ApolloFirmenname,
                                "Ort" : ApolloFirmenOrt,
                                "WebFirmenID" : int(row["WebFirmenID"]),
                                "Relation" : TypeRelation,
                                "Relation_staerke" : int(re.sub(r"%", "", row["%"]))}
                self.relations.loc[len(self.relations)] = new_relation


    def run(self, logger):
        for function_name in self.ORDER:
            getattr(self, function_name)(logger)

    
            
# Klasse wird aktuell nicht verwendet
class ComparisonContact:
    """
        Übernimm eine einzelne Dublettengruppe aus dem DQ-Ergebnis, spalte sie in ihre einzelen Dublettengruppen auf und speichere
        die dabei ausgelesenen Relationen. Gib die neuen Dublettengruppen und die ermittelten Relationen zurück.

        Args.
            self.doubletgroup_dataframe (df) - Data Frame mit allen Stammdaten aus einer Dublettengruppe im DQ-Abgleich
            self.doubletgroup_number (int) - Nummer der Dublettengruppe aus dem DQ-Abgleich
            self.doubletgroup_comparison_columns (set(str)) - Menge mit den Spaltennamen die im Abgleich enthalten sind
            self.superior_counter (int) - Counter der von der übergeordneten Klasse mitgegeben, aufgezählt und wieder zurückgegeben wird
            self.leads_dataframe (set --> df) - Aufgetrennter self.doubletgroup_dataframe: alle enthaltenen Leads # temporäre, verschachtelte Iteration evt. besser
            self.contacts_dataframe (set --> df) - Aufgetrennter self.doubletgroup_dataframe: alle enthaltenen Kontakte
            to be continued..

    """

    def __init__(self, doubletgroup_dataframe, doubletgroup_number, doubletgroup_comparison_columns):
        self.doubletgroup_dataframe = doubletgroup_dataframe
        self.doubletgroup_number = doubletgroup_number
        self.doubletgroup_comparison_columns = doubletgroup_comparison_columns
        self.superior_counter = 0
        self.estimated_similarity = 0
        self.leads_dataframe = pd.DataFrame()
        self.contacts_dataframe = pd.DataFrame()
        self.new_doubletgroups = [] 
        self.contact_relations = []



    def extract_leads(self): 
        """
            Spalte "*(Nicht aendern) Lead*" enthält unique ID des Leads oder "---". 
        """
        self.leads_dataframe = self.doubletgroup_dataframe[self.doubletgroup_dataframe["*(Nicht aendern) Lead*"] != "---"]


        return

    def extract_contacts(self): 
        """
            Spalte "*(Nicht aendern) Lead*" enthält unique ID des Leads oder "---". 
        """
        
        self.contacts_dataframe = self.doubletgroup_dataframe[self.doubletgroup_dataframe["*(Nicht aendern) Lead*"]=="---"]

        return

    def exctract_similarity (self): # ungetestet. auswahl des werts wirkt fehleranfällig, evtl. die Einzelwerte die aber schwer interpretierbar sind
        """
            Kleinste Ähnlichkeit wird aus Spalte "%" ermittelt.
            Interpretiert als unterste Grenze im Abgleich.
        """

        self.estimated_similarity = self.doubletgroup_dataframe["%"].min()


    def create_new_doubletgroups(self):
        """
            Alle Leads werden vereinzelt und jeder einzelne Lead mit allen Kontakte zu einer neuen Dublettengruppe kombiniert.
            Resultierende Data Frames werden als Attribut gespeichert. 
        
        """

        
        CombinedDataframes = []

        for index, row in self.leads_dataframe.iterrows():
            df1_row_df = pd.DataFrame([row])

            combined_df = pd.concat([df1_row_df, self.contacts_dataframe], ignore_index=True)

            CombinedDataframes.append(combined_df)

        print(f"{len(CombinedDataframes)} Data Frames wurden erstellt.")

        self.new_doubletgroups = CombinedDataframes
                
        
    def extract_relations(self):
        """
            Extrahieren der Relationen aus der Dublettengruppe und Zusammenführen in einem neuen Data Frame.
        """    

        ContactRelation = {"lead_id" : "",
                            "WebID" : 0,
                            "Abgleichsart" : "Kontakabgleich",
                            "Abgleichsdaten" : "-".join(self.doubletgroup_comparison_columns),
                            "Originale_Dublettengruppe" : self.doubletgroup_number,
                            "Minimale_Aehnlichkeit" : self.estimated_similarity,
                            }

        # Ohne Spaltennamen lässt sich Data Frame nicht befüllen
        ContactRelations = pd.DataFrame(columns = ContactRelation.keys())

        for index, row in self.leads_dataframe.iterrows():
            ContactRelation["lead_id"] = row["*(Nicht aendern) Lead*"] 

            for index2, row2 in self.contacts_dataframe.iterrows():
                ContactRelation["WebID"] = row2["*WebID*"] 

                ContactRelations.loc[len(ContactRelations)] = ContactRelation

        self.contact_relations = ContactRelations
