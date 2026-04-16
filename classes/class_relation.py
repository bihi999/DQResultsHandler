import pandas as pd
import numpy as np
from typing import Iterable, List, Set, Tuple, Dict, Set, Optional
from pandas.api.types import (is_string_dtype, is_object_dtype, is_integer_dtype)

# Schema: Pflichtspalten + erwartete Dtypes (pandas)
REQUIRED_SCHEMA = {
    "ApolloFirmenID": "string",
    "Firmenname": "string",
    "Ort": "string",
    "WebFirmenID": "Int64",          # Integer
    "Relation": "string",
    "Relation_staerke": "Int64"      # Integer
}

class relation_firma:
    __slots__ = (
        "ApolloFirmenID", "Firmenname", "Ort",
        "WebFirmenID", "Relation", "Relation_staerke"
    )

    def __init__(self, ApolloFirmenID, Firmenname, Ort, WebFirmenID, Relation, Relation_staerke):
        self.ApolloFirmenID = ApolloFirmenID
        self.Firmenname = Firmenname
        self.Ort = Ort
        self.WebFirmenID = WebFirmenID
        self.Relation = Relation
        self.Relation_staerke = Relation_staerke

    def _key(self) -> Tuple:
        # Die Reihenfolge der Felder definiert Identität
        return (
            self.ApolloFirmenID,
            self.Firmenname,
            self.Ort,
            self.WebFirmenID,
            self.Relation,
            self.Relation_staerke,
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, relation_firma):
            return NotImplemented
        return self._key() == other._key()

    def __hash__(self) -> int:
        return hash(self._key())

    def __repr__(self):
        return f"relation_firma({self.ApolloFirmenID}, {self.Firmenname}, {self.Ort})"

def _is_series_string_like(s: pd.Series) -> bool:
    """Erlaubt pandas 'string' oder 'object' (wenn alle Nicht-NA Werte echte str sind)."""
    if is_string_dtype(s.dtype):
        return True
    if is_object_dtype(s.dtype):
        non_na = s.dropna()
        if non_na.empty:
            return True
        return all(isinstance(v, str) for v in non_na)
    return False

def _is_series_int_like(s: pd.Series) -> bool:
    """Akzeptiert nullable Int64 (pandas) oder 'int64'."""
    if str(s.dtype) == "Int64":
        return True
    if is_integer_dtype(s.dtype):
        return True
    return False

def prepare_dataframes(dataframes: list[pd.DataFrame], logger, strict: bool = True):
    """
    Prüft eine Liste von DataFrames auf:
      - Pflichtspalten (und bei strict=True: keine zusätzlichen Spalten)
      - erwartete Datentypen pro Pflichtspalte (siehe REQUIRED_SCHEMA)
      - versucht automatische Konvertierung, falls der Typ nicht passt
    Gibt die (noch unbearbeiteten) DataFrames zurück, falls gültig.
    """
    if not dataframes:
        logger.error("Die übergebene DataFrame-Liste ist leer.")
        return []

    required_cols = list(REQUIRED_SCHEMA.keys())
    valid_dataframes = []

    for idx, df in enumerate(dataframes, start=1):
        logger.info(f"Prüfe DataFrame {idx} mit {len(df)} Zeilen.")

        # Spalten-Check
        missing = [c for c in required_cols if c not in df.columns]
        extra = [c for c in df.columns if c not in required_cols]

        if missing:
            logger.error(f"DataFrame {idx} fehlt folgende Spalten: {missing}")
            continue
        if strict and extra:
            logger.error(f"DataFrame {idx} enthält unerlaubte zusätzliche Spalten: {extra}")
            continue

        # Datentyp-Check + Konvertierung
        dtype_errors = []
        for col, expected in REQUIRED_SCHEMA.items():
            s = df[col]
            is_ok = False

            if expected == "string":
                is_ok = _is_series_string_like(s)
            elif expected == "Int64":
                is_ok = _is_series_int_like(s)

            if not is_ok:
                logger.warning(f"DataFrame {idx}, Spalte '{col}': falscher Typ {s.dtype}, versuche Umwandlung in {expected}...")
                try:
                    df[col] = df[col].astype(expected)
                    logger.info(f"DataFrame {idx}, Spalte '{col}': erfolgreich in {expected} konvertiert.")
                except Exception as e:
                    dtype_errors.append(f"Spalte '{col}' konnte nicht in {expected} konvertiert werden: {e}")

        if dtype_errors:
            for msg in dtype_errors:
                logger.error(f"DataFrame {idx}: {msg}")
            continue  # DF verwerfen

        logger.info(f"DataFrame {idx} hat gültige Spalten & Typen (strict={strict}).")
        # TODO: hier später weitere Aufbereitungsschritte einbauen
        valid_dataframes.append(df)

    logger.info(f"Insgesamt gültige DataFrames: {len(valid_dataframes)}")
    return valid_dataframes

def create_instances(
    dataframes: Iterable[pd.DataFrame],
    logger,
    deduplicate: bool = True
) -> List[relation_firma]:
    """
    Erwartet: bereits validierte/bereinigte DataFrames mit korrekten Dtypes.
    Erzeugt Instanzen von relation_firma.
    - deduplicate=True: entfernt identische Instanzen über Mengenlogik (__eq__/__hash__).
    """
    total_rows = 0
    raw_instances: List[relation_firma] = []

    for idx, df in enumerate(dataframes, start=1):
        n = len(df)
        total_rows += n
        logger.info(f"Erzeuge Instanzen aus DataFrame {idx} ({n} Zeilen).")

        # Keine weitere Transformation hier – die Aufbereitung passiert vorher.
        for _, row in df.iterrows():
            try:
                inst = relation_firma(
                    row["ApolloFirmenID"],
                    row["Firmenname"],
                    row["Ort"],
                    row["WebFirmenID"],
                    row["Relation"],
                    row["Relation_staerke"],
                )
                raw_instances.append(inst)
            except KeyError as e:
                logger.error(f"Fehlende Spalte bei Instanz-Erstellung in DF {idx}: {e}")
            except Exception as e:
                logger.error(f"Allg. Fehler bei Instanz-Erstellung in DF {idx}: {e}")

    logger.info(f"Roh erzeugte Instanzen: {len(raw_instances)} (aus {total_rows} Zeilen)")

    if not deduplicate:
        return raw_instances

    # Deduplizierung via Set (nutzt __hash__/__eq__)
    unique_set: Set[relation_firma] = set(raw_instances)
    deduped = list(unique_set)
    removed = len(raw_instances) - len(deduped)
    logger.info(f"Deduplizierung: {removed} Duplikate entfernt, {len(deduped)} eindeutige Instanzen verbleiben.")

    # Optional: stabile Reihenfolge – z. B. nach Key sortieren (falls gewünscht)
    # deduped.sort(key=lambda x: x._key())

    return deduped

def reorganize_instances(
    deduped: Iterable[relation_firma],
    logger
) -> Dict[Tuple[str, str, str], Dict[int, Set[Tuple[Optional[str], Optional[int]]]]]:
    """
    Organisiert Instanzen in eine verschachtelte Struktur:

      {
        (ApolloFirmenID, Ort, Firmenname): {
            WebFirmenID: { (Relation, Relation_staerke), ... }   # Menge von Tupeln
        }
      }

    - Der kombinierte Key ist ein Tupel (ApolloFirmenID, Ort, Firmenname) und lässt sich 1:1 wieder zerlegen.
    - Für jede WebFirmenID sammeln wir *alle* beobachteten (Relation, Relation_staerke)-Paare in einer Set-Menge.
      -> identische Paare werden automatisch dedupliziert.
    - Fehlende Werte werden als None gespeichert.
    """
    result: Dict[Tuple[str, str, str], Dict[int, Set[Tuple[Optional[str], Optional[int]]]]] = {}

    total_instances = 0
    added_pairs = 0
    skipped_pairs_dup = 0
    skipped_no_webid = 0

    for i, inst in enumerate(deduped, start=1):
        total_instances += 1
        try:
            # Kombinierter Schlüssel (verlustfrei zerlegbar)
            key = (str(inst.ApolloFirmenID), str(inst.Ort), str(inst.Firmenname))

            # WebFirmenID prüfen/normalisieren
            webid_raw = inst.WebFirmenID
            if pd.isna(webid_raw):
                skipped_no_webid += 1
                logger.warning(f"Instanz {i}: WebFirmenID ist NA – übersprungen. Key={key}")
                continue
            try:
                webid = int(webid_raw)
            except Exception:
                skipped_no_webid += 1
                logger.error(f"Instanz {i}: WebFirmenID nicht als int interpretierbar ({webid_raw!r}) – übersprungen. Key={key}")
                continue

            # Relation / Stärke normalisieren
            rel = None if pd.isna(inst.Relation) else str(inst.Relation)
            staerke = None if pd.isna(inst.Relation_staerke) else int(inst.Relation_staerke)

            pair = (rel, staerke)

            # Einfügen
            inner = result.setdefault(key, {})
            pairset = inner.setdefault(webid, set())

            before = len(pairset)
            pairset.add(pair)
            if len(pairset) > before:
                added_pairs += 1
                logger.info(f"Key={key}, WebFirmenID={webid}: Paar {pair} hinzugefügt.")
            else:
                skipped_pairs_dup += 1
                logger.info(f"Key={key}, WebFirmenID={webid}: Paar {pair} bereits vorhanden – übersprungen.")

        except Exception as e:
            logger.error(f"Fehler bei Reorganisation (Instanz {i}): {e}")

    total_keys = len(result)
    total_webids = sum(len(inner) for inner in result.values())
    total_pairs = sum(len(s) for inner in result.values() for s in inner.values())

    logger.info(
        "Reorganisation abgeschlossen: "
        f"{total_instances} Instanzen, "
        f"{total_keys} kombinierte Schlüssel, "
        f"{total_webids} WebFirmenID-Gruppen, "
        f"{total_pairs} Relation-Paare insgesamt "
        f"(neu hinzugefügt: {added_pairs}, Duplikate übersprungen: {skipped_pairs_dup}, "
        f"ohne WebFirmenID: {skipped_no_webid})."
    )
    return result


# Optional: Helfer zum Zerlegen des kombinierten Schlüssels
def split_composite_key(key: Tuple[str, str, str]) -> Tuple[str, str, str]:
    """Gibt (ApolloFirmenID, Ort, Firmenname) zurück."""
    return key

def _coerce_to_int64_with_logging(df: pd.DataFrame, col: str, logger) -> None:
    """
    Konvertiert df[col] robust zu 'Int64':
      - Strings/Objekte -> numerisch (coerce zu NA)
      - Dezimalwerte -> runden
      - finaler Cast -> Int64
    Schreibt Konvertierungsdetails in den Logger.
    """
    s_orig = df[col]
    s_num = pd.to_numeric(s_orig, errors="coerce")

    non_numeric_mask = s_num.isna() & s_orig.notna()
    non_numeric_count = int(non_numeric_mask.sum())
    if non_numeric_count:
        logger.warning(f"Spalte '{col}': {non_numeric_count} nicht-numerische Werte -> NA gesetzt.")

    dec_mask = s_num.notna() & ((s_num % 1) != 0)
    dec_count = int(dec_mask.sum())
    if dec_count:
        logger.warning(f"Spalte '{col}': {dec_count} Dezimalwerte -> vor Int-Cast gerundet.")
        s_num = s_num.round(0)  # Bankers rounding

    df[col] = s_num.astype("Int64")


def build_relation_dataframe(
    grouped: Dict[Tuple[str, str, str], Dict[int, Set[Tuple[Optional[str], Optional[int]]]]],
    logger
) -> pd.DataFrame:
    """
    Wandelt die verschachtelte Struktur in einen DataFrame um.

    Spalten: ApolloFirmenID, Ort, Firmenname, WebfirmenID, abgl_<Relation...>, Score

    - pro (ApolloFirmenID, Ort, Firmenname) x WebfirmenID entsteht genau eine Zeile
    - in jeder Relations-Spalte steht die höchste gefundene Relation_staerke (falls vorhanden), sonst NA
    - Score = Anzahl der (Relation, Relation_staerke)-Paare in der Menge für diese WebfirmenID
    - dynamische Relations-Spalten sind eindeutig durch Prefix 'abgl_'
    """
    # 1) Relation-Ausprägungen sammeln (ohne None) und robust zu Strings casten
    relation_types_raw = {
        rel
        for inner in grouped.values()
        for pairset in inner.values()
        for (rel, _staerke) in pairset
        if rel is not None
    }
    non_string_samples = [r for r in relation_types_raw if not isinstance(r, str)]
    if non_string_samples:
        logger.warning(
            "Nicht-string Relation-Ausprägungen gefunden; werden zu Strings konvertiert. "
            f"Beispiele: {non_string_samples[:3]}"
        )
    relation_types = sorted({str(rel) for rel in relation_types_raw})

    # Mapping Relation -> eindeutiger Spaltenname mit Prefix
    rel_to_col = {rel: f"abgl_{rel}" for rel in relation_types}
    dynamic_cols = [rel_to_col[r] for r in relation_types]
    logger.info(
        f"{len(relation_types)} Relation-Ausprägungen erkannt. "
        f"Dynamische Spalten: {dynamic_cols}"
    )

    # 2) Zeilen aufbauen
    rows = []
    for (apollo, ort, name), web_map in grouped.items():
        for webid, pairset in web_map.items():
            # pro Relation die höchste Stärke bestimmen (None wird ignoriert)
            best_strength: Dict[str, Optional[int]] = {}
            for rel, staerke in pairset:
                rel_key = None if rel is None else str(rel)
                if rel_key is None:
                    continue
                if staerke is None or pd.isna(staerke):
                    if rel_key not in best_strength:
                        best_strength[rel_key] = None
                else:
                    val = int(staerke)
                    if rel_key not in best_strength or best_strength[rel_key] is None or val > best_strength[rel_key]:
                        best_strength[rel_key] = val

            row = {
                "ApolloFirmenID": str(apollo),
                "Ort": str(ort),
                "Firmenname": str(name),
                "WebfirmenID": int(webid),      # bereits korrekt formatiert
                "Score": int(len(pairset)),     # Mächtigkeit der Menge
            }
            # Relations-Spalten füllen (fehlende -> NA); Spaltennamen sind prefixiert
            for rel_key, col_name in rel_to_col.items():
                row[col_name] = best_strength.get(rel_key, None)

            rows.append(row)

    df = pd.DataFrame(rows)

    # 3) Spaltenreihenfolge und fehlende Spalten abfangen
    ordered_cols = ["ApolloFirmenID", "Ort", "Firmenname", "WebfirmenID"] + dynamic_cols + ["Score"]
    for col in ordered_cols:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[ordered_cols]

    # 4) Dtypes & Sortierung (robust)
    if not df.empty:
        df["ApolloFirmenID"] = df["ApolloFirmenID"].astype("string")
        df["Ort"] = df["Ort"].astype("string")
        df["Firmenname"] = df["Firmenname"].astype("string")
        # KEIN Re-Cast von "WebfirmenID" – sollte bereits int/Int64 sein

        # Relations-Spalten & Score robust in Int64 überführen
        for col in dynamic_cols + ["Score"]:
            _coerce_to_int64_with_logging(df, col, logger)

        df = df.sort_values(
            by=["ApolloFirmenID", "Ort", "Firmenname", "WebfirmenID"],
            kind="stable"
        ).reset_index(drop=True)

    logger.info(
        f"DataFrame erstellt: {len(df)} Zeilen, {len(df.columns)} Spalten "
        f"(inkl. {len(dynamic_cols)} Relations-Spalten mit Prefix 'abgl_')."
    )
    return df