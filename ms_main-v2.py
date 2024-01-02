import pyodbc
from datetime import datetime, timedelta
import json
import requests
from dotenv import load_dotenv

load_dotenv()


# Function to make API call
def get_api_data(api_url, token):
    response = requests.get(
        api_url, headers={
            "Authorization": f"Token {token}"
        }
    )
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: Unable to fetch data from the API. Status code: {response.status_code}")
        return None


# Function to connect to Microsoft SQL Server database
def connect_to_database():
    try:
        connection = pyodbc.connect(
            f'DRIVER={os.getenv("DRIVER")}'
            f'SERVER={os.getenv("SERVER")}'
            f'DATABASE={os.getenv("DATABASE")}'
            f'UID={os.getenv("UID")}'
            f'PWD={os.getenv("PWD")}'
        )
        return connection
    except pyodbc.Error as err:
        print(f"Error: {err}")
        return None


def data_exists_eguide_election(cursor, election_id):
    cursor.execute("SELECT * FROM eguide_election WHERE election_id = ?", (election_id,))
    return cursor.fetchone() is not None


def insert_eguide_election_data(cursor, data):
    election_id = data['election_id']

    # Check if data already exists
    if data_exists_eguide_election(cursor, election_id):
        print(f"Data already exists for election_id: {election_id}. Skipping insertion.")
        return

    voting_methods_type = None
    voting_methods_primary = None
    voting_methods_start_date = None
    voting_methods_end_date = None
    voting_methods_execuse_required = None
    voting_methods_instructions = None

    # Check if data['voting_methods'] is not None
    if data.get('voting_methods') is not None:
        # Update values if voting_methods is present in data
        voting_methods_type = '!!'.join(str(method.get('type', '')) for method in data['voting_methods'])
        voting_methods_primary = '!!'.join(str(method.get('primary', '')) for method in data['voting_methods'])
        voting_methods_start_date = '!!'.join(str(method.get('start', '')) for method in data['voting_methods'])
        voting_methods_end_date = '!!'.join(str(method.get('end', '')) for method in data['voting_methods'])
        voting_methods_execuse_required = '!!'.join(
            str(method.get('excuse-required', '')) if method.get('excuse-required') is not None else '' for method in
            data['voting_methods']
        )
        voting_methods_instructions = '!!'.join(
            str(method.get('instructions', '')) for method in data['voting_methods']
        )

    # Extracting specific fields from the JSON data
    election_data = {
        'election_id': election_id,
        'election_name_encode': 'en_US',
        'election_name': data['election_name']['en_US'],
        'election_date_updated': data.get('date_updated', ''),
        'election_issues': data.get('election_issues', ''),
        'is_snap_election': data.get('is_snap_election', ''),
        'original_election_year': data.get('original_election_year', ''),
        'election_range_start_date': data.get('election_range_start_date', ''),
        'election_range_end_date': data.get('election_range_end_date', ''),
        'is_delayed_covid19': str(data.get('is_delayed_covid19', '')),
        'covid_effects': data.get('covid_effects', ''),
        'election_declared_start_date': data.get('election_declared_start_date', ''),
        'election_declared_end_date': data.get('election_declared_end_date', ''),
        'election_blackout_start_date': data.get('election_blackout_start_date', ''),
        'election_blackout_end_date': data.get('election_blackout_end_date', ''),
        'election_type': data.get('election_type', ''),
        'election_scope': data.get('election_scope', ''),
        'electoral_system': data.get('electoral_system', ''),
        'election_commission_name': data.get('election_commission_name', ''),
        'administring_election_commission_website': data.get('administering_election_commission_website', ''),
        'election_source': data.get('source', ''),
        'district_ocd_id': data['district'].get("district_ocd_id"),
        'district_name': data['district'].get("district_name"),
        'district_country': data['district'].get("district_country"),
        'district_type': data['district'].get("district_type"),
        'government_functions': data['government_functions'].get('details', ''),
        'government_functions_updated_date': data['government_functions'].get('updated', ''),
        'voter_registration_day_deadline': data.get('voter_registration_day', ''),
        'voting_age_minimum_inclusive': data.get('voting_age_minimum_inclusive', ''),
        'eligible_voters': data.get('eligible_voters', None),
        'first_time_voters': data.get('first_time_voters', None),
        'voting_methods_type': voting_methods_type,
        'voting_methods_primary': voting_methods_primary,
        'voting_methods_start_date': voting_methods_start_date,
        'voting_methods_end_date': voting_methods_end_date,
        'voting_methods_execuse_required': voting_methods_execuse_required,
        'voting_methods_instructions': voting_methods_instructions,
    }

    # Calculate election_range_end_date
    if election_data['election_range_start_date']:
        start_date = datetime.strptime(election_data['election_range_start_date'], '%Y-%m-%d')
        six_months = start_date - timedelta(days=180)
        election_data['election_range_start_date'] = six_months.strftime('%Y-%m-%d')

    # Inserting data into the eguide_election table
    cursor.execute(
        '''
        INSERT INTO eguide_election (
            election_id,
            election_name_encode,
            election_name,
            election_date_updated,
            election_issues,
            is_snap_election,
            original_election_year,
            election_range_start_date,
            election_range_end_date,
            is_delayed_covid19,
            covid_effects,
            election_declared_start_date,
            election_declared_end_date,
            election_blackout_start_date,
            election_blackout_end_date,
            election_type,
            election_scope,
            electoral_system,
            election_commission_name,
            administring_election_commission_website,
            election_source,
            district_ocd_id,
            district_name,
            district_country,
            district_type,
            government_functions,
            government_functions_updated_date,
            voter_registration_day_deadline,
            voting_age_minimum_inclusive,
            eligible_voters,
            first_time_voters,
            voting_methods_type,
            voting_methods_primary,
            voting_methods_start_date,
            voting_methods_end_date,
            voting_methods_execuse_required,
            voting_methods_instructions
        )
        VALUES (
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?
        )
        ''', (
            election_data['election_id'],
            election_data['election_name_encode'],
            election_data['election_name'],
            election_data['election_date_updated'],
            election_data['election_issues'],
            election_data['is_snap_election'],
            election_data['original_election_year'],
            election_data['election_range_start_date'],
            election_data['election_range_end_date'],
            election_data['is_delayed_covid19'],
            election_data['covid_effects'],
            election_data['election_declared_start_date'],
            election_data['election_declared_end_date'],
            election_data['election_blackout_start_date'],
            election_data['election_blackout_end_date'],
            election_data['election_type'],
            election_data['election_scope'],
            election_data['electoral_system'],
            election_data['election_commission_name'],
            election_data['administring_election_commission_website'],
            election_data['election_source'],
            election_data['district_ocd_id'],
            election_data['district_name'],
            election_data['district_country'],
            election_data['district_type'],
            election_data['government_functions'],
            election_data['government_functions_updated_date'],
            election_data['voter_registration_day_deadline'],
            election_data['voting_age_minimum_inclusive'],
            election_data['eligible_voters'],
            election_data['first_time_voters'],
            election_data['voting_methods_type'],
            election_data['voting_methods_primary'],
            election_data['voting_methods_start_date'],
            election_data['voting_methods_end_date'],
            election_data['voting_methods_execuse_required'],
            election_data['voting_methods_instructions']
        )
    )


# Replace 'YOUR_API_ENDPOINT' with the actual API endpoint
api_endpoint = 'https://electionguide.org/api/v2/elections_demo/'
api_token = os.getenv('TOKEN')

# Connect to MySQL database
connection = connect_to_database()
if connection is None:
    exit()

cursor = connection.cursor()

try:
    # Make API call
    api_data = get_api_data(api_endpoint, api_token)

    if api_data:
        for object in api_data:
            print("==============")
            print(f"starting insert eguide_election, election id : {object['election_id']}")
            insert_eguide_election_data(cursor, object)

        print("Data inserted into the database.")
except KeyboardInterrupt:
    print("Script terminated by user.")
except Exception as e:
    print("Error : ", e)
finally:
    # Close the database connection
    connection.close()
