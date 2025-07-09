import requests
import pandas as pd
from datetime import date
import os
import psycopg2

# --- Din eksisterende datahentning og fladning ---
response = requests.get('https://api.boligsiden.dk/search/cases?cities=Grindsted&per_page=200')
result = response.json()
result_properties = result['cases']
df = pd.DataFrame(result_properties)

def flatten_entry(entry, update_date=None):
    # ... (indsæt din fulde flatten_entry-funktion her uden ændringer)
    # For korthed, kopier den præcis som du har skrevet
    # Den er for lang til at gentage fuldt her, men kopier den direkte i din fil
    pass  # <-- Erstat denne linje med din funktion

flat_data = [flatten_entry(entry, update_date=str(date.today())) for entry in df.to_dict(orient="records")]
flat_df = pd.DataFrame(flat_data)

rename_map = {
    "address_id": "adresse_id",
    "city": "by",
    "zip_code": "postnummer",
    "road": "vej",
    "house_number": "husnummer",
    "municipality": "kommune",
    "province": "region",
    "latitude": "breddegrad",
    "longitude": "laengdegrad",
    "valuation": "seneste_vurdering",
    "living_area": "boligareal_m2",
    "weighted_area": "vaegtet_areal_m2",
    "year_built": "byggeaar",
    "building_area_main": "bygningsareal_primar_m2",
    "building_area_secondary": "bygningsareal_sekundar_m2",
    "down_payment": "udbetaling_kr",
    "gross_mortgage": "brutto_realkredit_kr",
    "net_mortgage": "netto_realkredit_kr",
    "realtor_name": "maegler_navn",
    "realtor_email": "maegler_email",
    "realtor_phone": "maegler_telefon",
    "realtor_rating_seller": "maegler_rating_saelger",
    "open_house_date": "aabent_hus_dato",
    "days_on_market_current": "dage_paa_marked_nu",
    "days_on_market_total": "dage_paa_marked_total",
    "price_cash": "salgspris_kr",
    "per_area_price": "pris_per_m2_kr",
    "number_of_toilets": "antal_toiletter",
    "number_of_rooms": "antal_vaerelser",
    "number_of_floors": "antal_etasjer",
    "number_of_bathrooms": "antal_badevaerelser",
    "has_balcony": "balkon",
    "has_elevator": "elevator",
    "has_terrace": "terrasse",
    "energy_label": "energimaerke",
    "address_type": "boligtype",
    "basement_area": "kaelderareal_m2",
    "description_body": "beskrivelse",
    "description_title": "overskrift",
    "update_date": "opdateringsdato"
}

flat_df.rename(columns=rename_map, inplace=True)

# --- Nyt: upload til Supabase/Postgres ---

def upload_to_supabase(df):
    conn = psycopg2.connect(
        host=os.environ['DB_HOST'],
        database=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        port=os.environ.get('DB_PORT', 5432)
    )
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO boligsiden_data (
                adresse_id, by, postnummer, vej, husnummer, kommune, region,
                breddegrad, laengdegrad, seneste_vurdering, boligareal_m2, vaegtet_areal_m2,
                byggeaar, bygningsareal_primar_m2, bygningsareal_sekundar_m2, udbetaling_kr,
                brutto_realkredit_kr, netto_realkredit_kr, maegler_navn, maegler_email,
                maegler_telefon, maegler_rating_saelger, aabent_hus_dato, dage_paa_marked_nu,
                dage_paa_marked_total, salgspris_kr, pris_per_m2_kr, antal_toiletter,
                antal_vaerelser, antal_etasjer, antal_badevaerelser, balkon, elevator,
                terrasse, energimaerke, boligtype, kaelderareal_m2, beskrivelse,
                overskrift, opdateringsdato
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s
            )
        """, (
            row['adresse_id'], row['by'], row['postnummer'], row['vej'], row['husnummer'], row['kommune'], row['region'],
            row['breddegrad'], row['laengdegrad'], row['seneste_vurdering'], row['boligareal_m2'], row['vaegtet_areal_m2'],
            row['byggeaar'], row['bygningsareal_primar_m2'], row['bygningsareal_sekundar_m2'], row['udbetaling_kr'],
            row['brutto_realkredit_kr'], row['netto_realkredit_kr'], row['maegler_navn'], row['maegler_email'],
            row['maegler_telefon'], row['maegler_rating_saelger'], row['aabent_hus_dato'], row['dage_paa_marked_nu'],
            row['dage_paa_marked_total'], row['salgspris_kr'], row['pris_per_m2_kr'], row['antal_toiletter'],
            row['antal_vaerelser'], row['antal_etasjer'], row['antal_badevaerelser'], row['balkon'], row['elevator'],
            row['terrasse'], row['energimaerke'], row['boligtype'], row['kaelderareal_m2'], row['beskrivelse'],
            row['overskrift'], row['opdateringsdato']
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("Data uploaded successfully")

# Kald upload-funktionen
upload_to_supabase(flat_df)
