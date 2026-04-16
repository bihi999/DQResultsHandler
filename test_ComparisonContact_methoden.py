import pytest
import pandas as pd
from classes import class_comparison as cc

df1 = pd.DataFrame({
    'Nr.': [1, 1],
    '%': ['100%', '100%'],
    'Tabelle': [
        'leads_crm.xlsx / Ansicht für die erweiterte ...',
        'kontakte_crm.xlsx / Ansicht für die erweiterte ...'
    ],
    '*WebID*': [None, 3686435.0],
    '*(Nicht aendern) Lead*': [
        'f2b7846d-ba3b-f011-ad5a-00155dd3b904',
        '---'
    ],
})

df6 = pd.DataFrame({
    'Nr.': [6, 6, 6, 6, 6],
    '%': ['100%', '100%', '100%', '100%', '86%'],
    'Tabelle': [
        'leads_crm.xlsx / Ansicht für die erweiterte ...',
        'kontakte_crm.xlsx / Ansicht für die erweiterte ...',
        'kontakte_crm.xlsx / Ansicht für die erweiterte ...',
        'kontakte_crm.xlsx / Ansicht für die erweiterte ...',
        'kontakte_crm.xlsx / Ansicht für die erweiterte ...'
    ],
    '*WebID*': [None, None, None, 3608104.0, 3657076.0],
    '*(Nicht aendern) Lead*': [
        '09c1101a-de04-f011-ad51-00155dd3b904',
        '09c1101a-de04-f011-ad51-00155dd3b134',
        '09c1101a-de04-f011-ad51-00155dd3k365',
        '---',
        '---'
    ],
})

df7 = pd.DataFrame({
    'Nr.': [7, 7, 7],
    '%': ['90%', '100%', '100%'],
    'Tabelle': [
        'leads_crm.xlsx / Ansicht für die erweiterte ...',
        'kontakte_crm.xlsx / Ansicht für die erweiterte ...',
        'kontakte_crm.xlsx / Ansicht für die erweiterte ...'
    ],
    '*WebID*': [None, 3686368.0, 3606110.0],
    '*(Nicht aendern) Lead*': [
        'd4f09b94-1221-f011-ad58-00155dd3b904',
        '---',
        '---'
    ],
})

df30 = pd.DataFrame({
    'Nr.': [30, 30, 30],
    '%': ['86%', '100%', '100%'],
    'Tabelle': [
        'leads_crm.xlsx / Ansicht für die erweiterte ...',
        'kontakte_crm.xlsx / Ansicht für die erweiterte ...',
        'kontakte_crm.xlsx / Ansicht für die erweiterte ...'
    ],
    '*WebID*': [None, None, 3643013.0],
    '*(Nicht aendern) Lead*': [
        '45cf101a-de04-f011-ad51-00155dd3b904',
        '45cf101a-de04-f011-ad51-00155dd3b000',
        '---'
    ],
})

df36 = pd.DataFrame({
    'Nr.': [36, 36, 36, 36],
    '%': ['95%', '95%', '95%', '94%'],
    'Tabelle': [
        'leads_crm.xlsx / Ansicht für die erweiterte ...',
        'kontakte_crm.xlsx / Ansicht für die erweiterte ...',
        'kontakte_crm.xlsx / Ansicht für die erweiterte ...',
        'kontakte_crm.xlsx / Ansicht für die erweiterte ...'
    ],
    '*WebID*': [None, 3654326.0, 3684117.0, 3635930.0],
    '*(Nicht aendern) Lead*': [
        'f796a8bd-1704-f011-ad4e-00155dd3b904',
        '---',
        '---',
        '---'
    ],
})

df40 = pd.DataFrame({
    'Nr.': [40, 40, 40],
    '%': ['100%', '100%', '78%'],
    'Tabelle': [
        'leads_crm.xlsx / Ansicht für die erweiterte ...',
        'kontakte_crm.xlsx / Ansicht für die erweiterte ...',
        'kontakte_crm.xlsx / Ansicht für die erweiterte ...'
    ],
    '*WebID*': [None, 3670496.0, 3576797.0],
    '*(Nicht aendern) Lead*': [
        'f796a8bd-1704-f011-ad4e-00155dd3b904',
        '---',
        '---'
    ],
})



import pytest
import random

# Zwei Werte aus denen sich die anderen beiden Erwartungswerte ableiten lassen.
@pytest.mark.parametrize("df, leads_len, contacts_len", [
    (df1, 1, 1),
    (df6, 3, 2),
    (df7, 1, 2),
    (df30, 2, 1),
    (df36, 1, 3),
    (df40, 1, 2)
])


def test_comparison_contact(df, leads_len, contacts_len):
    # Ableitbare Werte
    new_doubletgroups_len = leads_len
    df_len_in_new_doubletgroups = contacts_len + 1
    
    instance = cc.ComparisonContact(df, random.randint(1, 100), {"Vorname", "Nachname", "Firma"})
    
    # Test extract_leads()
    instance.extract_leads()
    assert len(instance.leads_dataframe) == leads_len, f"Leads DataFrame Länge falsch für DF Nr {df['Nr.'].iloc[0]}"
    
    # Test extract_contacts()
    instance.extract_contacts()
    assert len(instance.contacts_dataframe) == contacts_len, f"Contacts DataFrame Länge falsch für DF Nr {df['Nr.'].iloc[0]}"
    
    # Test create_new_doubletgroups()
    instance.create_new_doubletgroups()
    assert len(instance.new_doubletgroups) == new_doubletgroups_len, f"new_doubletgroups Listenlänge falsch für DF Nr {df['Nr.'].iloc[0]}"
    
    for i, subgroup_df in enumerate(instance.new_doubletgroups):
        assert len(subgroup_df) == df_len_in_new_doubletgroups, f"Länge von subgroup_df[{i}] in new_doubletgroups falsch für DF Nr {df['Nr.'].iloc[0]}"



