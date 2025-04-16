# https://www.kaggle.com/datasets/kannan1314/innocent-deaths-caused-by-police-all-time/data
import pandas as pd

df = pd.read_csv('Innocent Deaths caused by Police (All time).csv')

print("Colunas originais:")
print(df.columns.tolist())

df.columns = df.columns.str.lower().str.replace(' ', '_')

print("\nColunas após renomeação:")
print(df.columns.tolist())

victims = df[['unique_id', 'name', 'age', 'gender', 'race', 'url_of_image']].copy()
victims = victims.rename(columns={'url_of_image': 'image_url'})

incidents = df[['unique_id', '_date_of_injury_resulting_in_death_(month/day/year)', 
               'agency_or_agencies_involved', 'highest_level_of_force', 
               'alleged_weapon', 'aggressive_physical_movement', 
               'fleeing/not_fleeing', 'dispositions/exclusions_internal_use,_not_for_analysis',
               'intended_use_of_force_(developing)', 'supporting_document_link',
               'foreknowledge_of_mental_illness', 'brief_description']].copy()
incidents = incidents.rename(columns={
    '_date_of_injury_resulting_in_death_(month/day/year)': 'incident_date',
    'fleeing/not_fleeing': 'fleeing_status',
    'dispositions/exclusions_internal_use,_not_for_analysis': 'disposition',
    'intended_use_of_force_(developing)': 'intended_force',
    'supporting_document_link': 'document_link',
    'foreknowledge_of_mental_illness': 'mental_illness_known',
    'brief_description': 'description'
})

locations = df[['unique_id', 'location_of_injury_(address)', 'location_of_death_(city)',
               'state', 'location_of_death_(zip_code)', 'location_of_death_(county)',
               'full_address', 'latitude', 'longitude']].copy()
locations = locations.rename(columns={
    'location_of_injury_(address)': 'injury_address',
    'location_of_death_(city)': 'death_city',
    'location_of_death_(zip_code)': 'death_zip',
    'location_of_death_(county)': 'death_county'
})

agencies = df[['unique_id', 'agency_or_agencies_involved']].copy()
agencies = agencies.rename(columns={'agency_or_agencies_involved': 'agency_name'})

victims.to_csv('victims.csv', index=False)
incidents.to_csv('incidents.csv', index=False)
locations.to_csv('locations.csv', index=False)
agencies.to_csv('agencies.csv', index=False)

print("\nArquivos CSV criados com sucesso!")
print(f"- victims.csv: {len(victims)} registros")
print(f"- incidents.csv: {len(incidents)} registros")
print(f"- locations.csv: {len(locations)} registros")
print(f"- agencies.csv: {len(agencies)} registros")