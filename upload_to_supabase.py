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
    """Flader et boligopslag fra Boligsidens API til en flad dictionary."""
    address = entry.get("address", {}) or {}
    coordinates = entry.get("coordinates", {}) or {}
    real_estate = entry.get("realEstate", {}) or {}
    realtor = entry.get("realtor", {}) or {}
    open_house = entry.get("nextOpenHouse", {}) or {}
    time_on_market = entry.get("timeOnMarket", {}) or {}

    if not isinstance(address, dict): address = {}
    if not isinstance(coordinates, dict): coordinates = {}
    if not isinstance(real_estate, dict): real_estate = {}
    if not isinstance(realtor, dict): realtor = {}
    if not isinstance(open_house, dict): open_house = {}
    if not isinstance(time_on_market, dict): time_on_market = {}

    buildings = address.get("buildings", [])
    if not isinstance(buildings, list): buildings = []

    main_building_area = 0
    secondary_building_area = 0
    if buildings:
        main_building_area = buildings[0].get("totalArea", 0)
        secondary_building_area = sum(b.get("totalArea", 0) for b in buildings[1:] if isinstance(b, dict))

    flattened = {
        # Adresse
        "address_id": address.get("addressID"),
        "city": address.get("city", {}).get("name") if isinstance(address.get("city"), dict) else None,
        "zip_code": address.get("zipCode"),
        "road": address.get("roadName"),
        "house_number": address.get("houseNumber"),
        "municipality": address.get("municipality", {}).get("name") if isinstance(address.get("municipality"), dict) else None,
        "province": address.get("province", {}).get("name") if isinstance(address.get("province"), dict) else None,
        "latitude": coordinates.get("lat"),
        "longitude": coordinates.get("lon"),
        "valuation": address.get("latestValuation"),
        "living_area": address.get("livingArea"),
        "weighted_area": address.get("weightedArea"),
        "year_built": buildings[0].get("yearBuilt") if buildings and isinstance(buildings[0], dict) else None,
        "building_area_main": main_building_area,
        "building_area_secondary": secondary_building_area,

        # RealEstate og Mægler
        "down_payment": real_estate.get("downPayment"),
        "gross_mortgage": real_estate.get("grossMortgage"),
        "net_mortgage": real_estate.get("netMortgage"),
        "realtor_name": realtor.get("name"),
        "realtor_email": realtor.get("contactInformation", {}).get("email") if isinstance(realtor.get("contactInformation"), dict) else None,
        "realtor_phone": realtor.get("contactInformation", {}).get("phone") if isinstance(realtor.get("contactInformation"), dict) else None,
        "realtor_rating_seller": realtor.get("rating", {}).get("seller", {}).get("score") if isinstance(realtor.get("rating", {}), dict) else None,

        # Åbent hus og tid på markedet
        "open_house_date": open_house.get("date"),
        "days_on_market_current": time_on_market.get("current", {}).get("days") if isinstance(time_on_market.get("current"), dict) else None,
        "days_on_market_total": time_on_market.get("total", {}).get("days") if isinstance(time_on_market.get("total"), dict) else None,

        # Direkte fra entry
        "price_cash": entry.get("priceCash"),
        "per_area_price": entry.get("perAreaPrice"),
        "number_of_toilets": entry.get("numberOfToilets"),
        "number_of_rooms": entry.get("numberOfRooms"),
        "number_of_floors": entry.get("numberOfFloors"),
        "number_of_bathrooms": entry.get("numberOfBathrooms"),
        "has_balcony": entry.get("hasBalcony"),
        "has_elevator": entry.get("hasElevator"),
        "has_terrace": entry.get("hasTerrace"),
        "energy_label": entry.get("energyLabel"),
        "address_type": entry.get("addressType"),
        "basement_area": entry.get("basementArea"),
        "description_body": entry.get("descriptionBody"),
        "description_title": entry.get("descriptionTitle"),

        # Dato for scraping
        "update_date": update_date,
    }
    return flattened


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

# Liste over kolonner der skal være Int64 (nullable int)
int_cols = [
    'postnummer', 'seneste_vurdering', 'boligareal_m2', 'byggeaar',
    'bygningsareal_primar_m2', 'bygningsareal_sekundar_m2', 'dage_paa_marked_nu',
    'dage_paa_marked_total', 'salgspris_kr', 'antal_toiletter', 'antal_vaerelser',
    'antal_etasjer', 'antal_badevaerelser', 'kaelderareal_m2'
]

for col in int_cols:
    if col in flat_df.columns:
        flat_df[col] = pd.to_numeric(flat_df[col], errors='coerce').astype('Int64')

# Konverter dato-kolonner
flat_df['opdateringsdato'] = pd.to_datetime(flat_df['opdateringsdato'], errors='coerce')
if 'aabent_hus_dato' in flat_df.columns:
    flat_df['aabent_hus_dato'] = pd.to_datetime(flat_df['aabent_hus_dato'], errors='coerce')

# Dobbelttjek bool kolonner er bool dtype
bool_cols = ['balkon', 'elevator', 'terrasse']
for col in bool_cols:
    if col in flat_df.columns:
        flat_df[col] = flat_df[col].astype(bool)



# --- Nyt: upload til Supabase/Postgres ---

import pandas as pd

def upload_to_supabase(df):
    import os
    import psycopg2

    def convert_missing_to_none(row):
        return [None if (pd.isna(x) or x is pd.NA) else x for x in row]

    conn = psycopg2.connect(
        host=os.environ['DB_HOST'],
        database=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        port=os.environ.get('DB_PORT', 5432)
    )
    cur = conn.cursor()

    for _, row in df.iterrows():
        values = convert_missing_to_none(row)
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
        """, values)

    conn.commit()
    cur.close()
    conn.close()
    print("Data uploaded successfully")



# Kald upload-funktionen
upload_to_supabase(flat_df)
