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
        
        #---- a_ subset sollte nicht hier hart vercodet werden - Funktion öffnen - sie erhält die Liste von außen innerhalb bestimmter Regeln

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

        #--------- a_ WebFirmenID nur als Default in Liste hinterlegen - weitere Varianten ermöglichen

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
