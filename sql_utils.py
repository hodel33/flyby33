# Built-in libraries
import json
from datetime import datetime, timedelta
import sqlite3 as sql

# Third-party libraries -> requirements.txt
import airportsdata



class SQLiteX33:
    """A streamlined SQLite Database Context Manager for easy SQL queries. Import as 'import sqlite_x33 as sql', then use sql.execute(db, query). /hodel33 & dyaland"""
    def __init__(self, db_file_path:str):
        self.db_file = db_file_path
        
    def __enter__(self):
        self.connection = sql.connect(self.db_file)
        self.connection.row_factory = sql.Row  # Enable dictionary-like row access, e.g. row['name']
        self.cursor = self.connection.cursor()
        self.cursor.execute("PRAGMA foreign_keys = True;") # Enable FOREIGN KEYS for SQLite 3
        return self
    
    def __exit__(self, exc_class, exc, traceback):
        try:
            self.connection.commit()
        except AttributeError: # isn't closable
            return True # exception handled successfully
        finally:
            self.cursor.close(); self.connection.close()
     
    def execute_query(self, query:str, params=()):
        if isinstance(params, list) and len(params) > 0 and isinstance(params[0], (list, tuple)): # Batch operation
            self.cursor.executemany(query, params)
            return self.cursor.rowcount # Return number of affected rows for batch INSERT/UPDATE/DELETE
        else: # Single operation
            self.cursor.execute(query, params)
            return self.cursor.fetchall() # Return result of a SELECT query; empty list [] for INSERT/UPDATE/DELETE

def execute(db_path:str, query:str, params=()):
    with SQLiteX33(db_path) as db:
        return db.execute_query(query, params)
    

class DatabaseUtils:
    """
    Utility class for database operations related to flight tracking.
    
    This class provides methods for saving, loading, and updating flight data, airport information, and airline details 
    in a SQLite database. It handles data enrichment, batch processing operations and manages the database schema.
    
    The database schema consists of three main tables:
    - flights: Stores flight tracking data including trail information
    - airport_data: Contains reference data for airports including coordinates
    - airline_data: Contains reference data for airline companies
    """
    
    # Initialize tables for the database
    db_tables = [
        """CREATE TABLE IF NOT EXISTS flights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            flight_id TEXT UNIQUE,
            callsign TEXT,
            tail_no TEXT,
            flight_no TEXT,
            aircraft_icao TEXT,
            aircraft TEXT,
            airline_icao TEXT,
            airline TEXT,
            origin_airport_iata TEXT,
            origin_city TEXT,
            destination_airport_iata TEXT,
            destination_city TEXT,
            destination_airport_coords TEXT,
            trail_data TEXT,
            trail_data_details TEXT,
            last_fetch_timestamp DATETIME,
            last_fetch_timestamp_details DATETIME
        );""",

        """CREATE TABLE IF NOT EXISTS airport_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            icao TEXT UNIQUE,
            iata TEXT,
            name TEXT,
            lat REAL,
            lng REAL,
            city TEXT,
            country TEXT,
            last_fetch_timestamp DATETIME      
        );""",

        """CREATE TABLE IF NOT EXISTS airline_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            icao TEXT UNIQUE,
            name TEXT,
            last_fetch_timestamp DATETIME
        );"""
    ]
   
    @staticmethod
    def load_flights_from_db(db_path, flight_ids=None):
        """
        Get flights from the database, optionally filtered by flight IDs.
        
        :param db_path: Path to the SQLite database file
        :param flight_ids (optional): List of flight IDs to filter by. If None, returns all flights.
        :return: list: List of dictionaries, each representing a flight with its data
        """
        # JSON field parsing helper func
        def _parse_json_db_field(row, field_name, default=None):
            try:
                return json.loads(row[field_name]) if row[field_name] else default
            except json.JSONDecodeError:
                return default

        # Query flights from the database
        if flight_ids:
            placeholders = ', '.join(['?'] * len(flight_ids)) # Create placeholders for SQL query
            rows = execute(db_path, f"SELECT * FROM flights WHERE flight_id IN ({placeholders})", flight_ids) # Query specific flights from the db
        else:
            rows = execute(db_path, "SELECT * FROM flights") # Query all flights from the db

        flights = []

        for row in rows:
            # Create a dict with all flight data directly from row keys (excluding the 'id' column)
            existing_flight = {key: value for key, value in dict(row).items() if key != 'id'}
            
            # Process JSON fields
            json_fields = ["destination_airport_coords", "trail_data", "trail_data_details"]
            for field in json_fields:
                existing_flight[field] = _parse_json_db_field(row, field)

            # Add the processed flight to the flights list
            flights.append(existing_flight)

        return flights    
    
    @staticmethod
    def get_flights_with_details_fetched(db_path, max_age_minutes=6):
        """
        Return a list of flight_id's for flights where last_fetch_timestamp_details is not empty
        and the timestamp is less than max_age_minutes old.
                
        :param db_path: Path to the SQLite database
        :param max_age_minutes: Maximum age in minutes for a fetch to be considered valid (default: 6)
        :return list: List of flight IDs with recent detailed API data
        """

        def is_recent_timestamp(current_time, timestamp_str, max_age_minutes):
            """Helper function to check if a timestamp is recent"""
            try:
                time_diff = current_time - datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                return time_diff.total_seconds() < max_age_minutes * 60
            except (ValueError, TypeError):
                return False
            
        flights = DatabaseUtils.load_flights_from_db(db_path)
        current_time = datetime.now()
        
        # Filter flights with recent timestamps
        flight_ids = [flight["flight_id"] for flight in flights if flight["last_fetch_timestamp_details"] and 
                    is_recent_timestamp(current_time, flight["last_fetch_timestamp_details"], max_age_minutes)]
        
        return flight_ids            
    
    @staticmethod
    def save_flights_to_db(db_path, flight_list):
        """
        Save flight data to database.
            
        :param db_path: Path to the SQLite database
        :param flight_list: List of flight dictionaries to save
        :return int: Number of flight records saved to database
        """

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        saved_count = 0
        
        for flight in flight_list:
            flight_id = flight.get('flight_id')
            is_detailed = flight.get('api_details_fetch', False) # Check if detailed fetch was performed for this flight
            
            # Retrieve existing trail data from database
            existing_data = execute(db_path, "SELECT trail_data FROM flights WHERE flight_id = ?", (flight_id,))
            
            # Prepare all trail data (both new + old) - Removing duplicates if present and sorting newest trail first
            updated_trail_points = [flight.get('trail_data')] # Add the new trail point
            if existing_data and existing_data[0][0]:
                try:
                    old_points = json.loads(existing_data[0][0]) # existing trail points
                    for point in (old_points if isinstance(old_points, list) else [old_points]):
                        if point['ts'] != flight['trail_data']['ts']: # Skip point if it has same timestamp as new point (skipping duplicates)
                            updated_trail_points.append(point)
                    updated_trail_points.sort(key=lambda x: x['ts'], reverse=True) # Sort by timestamp descending (newest first)
                # except: pass # Ignore invalid data
                except Exception as parse_error:
                    print(f"Error parsing existing trail data for flight {flight_id}: {parse_error}")
           
            # Set processed values
            flight['trail_data'] = json.dumps(updated_trail_points[:6])
            flight['last_fetch_timestamp'] = current_time
            
            if is_detailed:
                flight['last_fetch_timestamp_details'] = current_time
                if flight.get('trail_data_details'):
                    flight['trail_data_details'] = json.dumps(flight['trail_data_details'])
                if flight.get('destination_airport_coords'):
                    flight['destination_airport_coords'] = json.dumps(flight['destination_airport_coords'])
                
            # Clean up (remove keys which don't correspond to columns in db)
            if 'api_details_fetch' in flight: 
                del flight['api_details_fetch']

            # Dynamically generate SQL from flight dict keys - works because dict keys match column names 
            try:
                if is_detailed or not existing_data: # Insert / Replace so it works for both use cases (Detailed fetch / General fetch)
                    execute(db_path, 
                        f"INSERT OR REPLACE INTO flights ({','.join(flight.keys())}) VALUES ({','.join(['?']*len(flight))})",
                        list(flight.values()))
                    
                else: # Update using COALESCE to only change fields that have values
                    updates = [f"{k}=COALESCE(?,{k})" for k in flight if k != 'flight_id']
                    params = [flight[k] for k in flight if k != 'flight_id'] + [flight_id]
                    
                    if updates:
                        execute(db_path, f"UPDATE flights SET {','.join(updates)} WHERE flight_id=?", params)
            
                saved_count += 1

            except Exception as error:
                print(f"Unexpected error processing flight: {error}")
                print(f"Problematic flight data: {flight}")
                return 0    
        
        return saved_count
       
    @staticmethod
    def save_airport_data_to_db(db_path, airport_data_list):
        """
        Save airport data to database using batch processing.
            
        :param db_path: Path to the SQLite database
        :param airport_data_list: List of airport data dictionaries to save
        :return int: Number of airport records saved to database
        """
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            # Add timestamp to all records
            airport_data_list = [{**airport, 'last_fetch_timestamp': current_time} for airport in airport_data_list]
            
            # Ensure we have airports to process
            if not airport_data_list:
                return 0
                
            # Get columns from the first airport (assuming all have same structure)
            columns = ','.join(airport_data_list[0].keys())
            placeholders = ','.join(['?'] * len(airport_data_list[0]))
            
            # Create SQL statement
            sql = f"INSERT OR REPLACE INTO airport_data ({columns}) VALUES ({placeholders})"
            
            # Prepare parameter lists for each airport
            param_sets = [list(airport.values()) for airport in airport_data_list]
            
            # Execute as a batch operation
            saved_count = execute(db_path, sql, param_sets)
            
            return saved_count
            
        except Exception as e:
            print(f"Error during batch airport save: {e}")
            return 0

    @staticmethod
    def save_airline_data_to_db(db_path, airline_data_list):
        """
        Save airline data to database using batch processing.
       
        :param db_path: Path to the SQLite database
        :param airline_data_list: List of airline data dictionaries to save
        :return int: Number of airline records saved to database
        """
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            
            # Add timestamp to all records
            airline_data_list = [{**airline, 'last_fetch_timestamp': current_time} for airline in airline_data_list]
            
            # Ensure we have airlines to process
            if not airline_data_list:
                return 0
                
            # Get columns from the first airline (assuming all have same structure)
            columns = ','.join(airline_data_list[0].keys())
            placeholders = ','.join(['?'] * len(airline_data_list[0]))
            
            # Create SQL statement
            sql = f"INSERT OR REPLACE INTO airline_data ({columns}) VALUES ({placeholders})"
            
            # Prepare parameter lists for each airline
            param_sets = [list(airline.values()) for airline in airline_data_list]
            
            # Execute as a batch operation
            saved_count = execute(db_path, sql, param_sets)
            
            return saved_count
            
        except Exception as e:
            print(f"Error during batch airline save: {e}")
            return 0
        
    @staticmethod
    def fetch_airport_city_data(db_path):
        """
        Fetch city information for airports that need it.
        
        :param db_path: Path to the SQLite database
        :return list: List of dictionaries with airport ID and city information
        """        
        # Load airport data with both IATA and ICAO codes as keys
        iata_airports = airportsdata.load('IATA')
        icao_airports = airportsdata.load('ICAO')
        
        # Get all airports that need city information
        airports = execute(db_path, "SELECT id, icao, iata FROM airport_data WHERE city IS NULL")
        
        if not airports: # o airports found needing city data
            return []
        
        # Create result list
        airport_city_data = []
        
        # Process each airport
        for airport in airports:
            city = None
            
            # Try to get data using ICAO code first (more reliable)
            if airport['icao'] and airport['icao'] in icao_airports:
                city = icao_airports[airport['icao']]['city']
            
            # If no city found and IATA code exists, try this
            if not city and airport['iata'] and airport['iata'] in iata_airports:
                city = iata_airports[airport['iata']]['city']
            
            # If city found, add to results
            if city:
                airport_city_data.append({
                    'id': airport['id'],
                    'city': city
                })
        
        return airport_city_data

    @staticmethod
    def save_airport_city_data_to_db(db_path, airport_city_data):
        """
        Save airport city info to database using batch processing.
        
        :param db_path: Path to the SQLite database
        :param airport_city_data: List of dictionaries with airport ID and city
        :return int: Number of airports updated
        """        
        try:
            # Ensure we have data to process
            if not airport_city_data:
                return 0
                
            # Prepare update parameters [city, id]
            param_sets = [[airport['city'], airport['id']] for airport in airport_city_data]
            
            # Execute batch update
            updated_count = execute(db_path, "UPDATE airport_data SET city = ? WHERE id = ?", param_sets)
            
            return updated_count
            
        except Exception as e:
            print(f"Error during batch airport city update: {e}")
            return 0
        
    @staticmethod
    def enrich_missing_flight_data_from_db(db_path, flight_list):
        """
        Enrich flight data with information from airport and airline database tables
        
        :param db_path: Path to the SQLite database
        :param flight_list: List of flight dictionaries to enrich
        :return list: List of enriched flight dictionaries
        """       
        # If no flights, return empty list
        if not flight_list:
            return []
        
        # Get all airline data as a dictionary for quick lookup
        airlines = execute(db_path, "SELECT icao, name FROM airline_data")
        airline_dict = {airline['icao']: airline['name'] for airline in airlines if airline['icao']}
        
        # Get all airport data as a dictionary for quick lookup
        airports = execute(db_path, "SELECT icao, iata, city, lat, lng FROM airport_data")
        airport_dict_by_iata = {airport['iata']: airport for airport in airports if airport['iata']}
        
        enriched_flight_list = []

        # Enrich each flight
        for flight in flight_list:

            original_flight = flight.copy()

            # Add airline name if missing in the flight data and available in our airline_data table
            if not flight.get('airline') and flight.get('airline_icao') and flight['airline_icao'] in airline_dict:
                flight['airline'] = airline_dict[flight['airline_icao']]
            
            # Add origin city if missing in the flight data and available in our airport_data table
            if not flight.get('origin_city') and flight.get('origin_airport_iata') and flight['origin_airport_iata'] in airport_dict_by_iata:
                origin_airport = airport_dict_by_iata[flight['origin_airport_iata']]
                flight['origin_city'] = origin_airport['city']

            # Add destination city if missing in the flight data and available in our airport_data table
            if not flight.get('destination_city') and flight.get('destination_airport_iata') and flight['destination_airport_iata'] in airport_dict_by_iata:
                dest_airport = airport_dict_by_iata[flight['destination_airport_iata']]
                flight['destination_city'] = dest_airport['city']
                
                # Add destination airport coordinates if missing in the flight data and available in our airport_data table
                if not flight.get('destination_airport_coords') and dest_airport['lat'] and dest_airport['lng']:
                    flight['destination_airport_coords'] = {"lat": dest_airport['lat'], "lng": dest_airport['lng']}

            # Only append if the flight was modified (enriched)
            if flight != original_flight:
                enriched_flight_list.append(flight)
        
        return enriched_flight_list

    @staticmethod
    def save_enriched_flights_to_db(db_path, flight_list):
        """
        Save enriched flight data to database, focusing on fields that come from enrichment.
        
        :param db_path: Path to the SQLite database
        :param flight_list: List of enriched flight dictionaries
        :return int: Number of flights updated with enrichment data
        """     
        try:
            # Ensure we have data to process
            if not flight_list:
                return 0
                
            # Prepare update parameters for each flight
            updates = []

            for flight in flight_list:

                # Only include flights that have at least one enriched field
                if any(field in flight for field in ['airline', 'origin_city', 'destination_city', 'destination_airport_coords']):
                    
                    # Prepare update parameters
                    params = []
                    
                    # Add parameters in the same order as the SQL query
                    params.append(flight.get('airline') or None)
                    params.append(flight.get('origin_city') or None)
                    params.append(flight.get('destination_city') or None)

                    # Convert destination_airport_coords to JSON string if needed
                    dest_coords = flight.get('destination_airport_coords')
                    if dest_coords is not None:
                        params.append(json.dumps(dest_coords) if isinstance(dest_coords, dict) else dest_coords)
                    else:
                        params.append(None)

                    params.append(flight.get('flight_id'))
                    
                    updates.append(params)
            
            # Execute batch update
            if updates:
                updated_count = execute(db_path, """
                    UPDATE flights SET 
                        airline = COALESCE(?, airline),
                        origin_city = COALESCE(?, origin_city),
                        destination_city = COALESCE(?, destination_city),
                        destination_airport_coords = COALESCE(?, destination_airport_coords)
                    WHERE flight_id = ?
                """, updates)
                
                return updated_count
                
            return 0
            
        except Exception as e:
            print(f"Error during flight enrichment update: {e}")
            return 0
        
    @staticmethod
    def is_reference_data_refresh_needed(db_path):
        """
        Check if airport and airline reference data needs to be refreshed.
        
        :param db_path: Path to the SQLite database
        :return bool: True if no data exists or if the most recent fetch is older than time passed threshold
        """
        # Check latest fetch timestamp from both airport and airline tables
        latest_fetch_result = execute(db_path, """
            SELECT MAX(last_fetch_timestamp) AS latest_timestamp
            FROM (
                SELECT last_fetch_timestamp FROM airport_data
                UNION ALL
                SELECT last_fetch_timestamp FROM airline_data
            )
        """)
        latest_timestamp = latest_fetch_result[0][0] if latest_fetch_result and latest_fetch_result[0][0] else None
        
        # If no data exists in either table, we need to refresh
        if not latest_timestamp:
            return True

        try:
            latest_fetch_date = datetime.strptime(latest_timestamp, "%Y-%m-%d %H:%M:%S")
            time_passed_refresh_threshold = datetime.now() - timedelta(days=30) # Check if the most recent fetch is older than the time threshold

            return latest_fetch_date < time_passed_refresh_threshold
            
        except (ValueError, TypeError):
            # If there's any issue parsing the date, refresh to be safe
            return True

    @staticmethod
    def cleanup_old_flights(db_path, days_threshold=7):
        """
        Delete flights older than specified days threshold and reset table indexing.
        
        :param db_path: Path to SQLite database
        :param days_threshold: Number of days to keep data (default: 7)
        :return: int: Number of deleted flights
        """
        try:
            # Calculate the cutoff date (current time - days_threshold)
            cutoff_date = (datetime.now() - timedelta(days=days_threshold)).strftime("%Y-%m-%d %H:%M:%S")
            
            # Delete old flights
            deleted_count = execute(db_path, "DELETE FROM flights WHERE last_fetch_timestamp < ?", (cutoff_date,))
            
            # Reset the indexing/AUTOINCR sequence for the flights table
            execute(db_path, "DELETE FROM sqlite_sequence WHERE name = 'flights'")
            
            return deleted_count
            
        except Exception as e:
            print(f"Error during cleanup of old flights: {e}")
            return 0