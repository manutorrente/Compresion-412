import os
import json
import pandas as pd
from pathlib import Path

# Define paths
WORKSPACE_DIR = r"c:\Users\Manu\OneDrive\Escritorio\DBland\rutas 412"
CSV_FILE = os.path.join(WORKSPACE_DIR, "20251112_131417.csv")
PARAMS_DIR = os.path.join(WORKSPACE_DIR, "params_Ingestas")
ATLAS_PROD_DIR = os.path.join(WORKSPACE_DIR, "atlas_prod-main")
OUTPUT_ROUTES_FILE = os.path.join(WORKSPACE_DIR, "rutas_landing.txt")

# Build a mapping of atlas_term to json_file path for faster lookup
json_file_cache = {}

def build_json_cache():
    """
    Build a cache of all JSON files in both params_Ingestas and atlas_prod-main directories.
    Maps atlas_term (filename without .json) to full file path.
    """
    print("Building JSON file cache...")
    
    # Search in params_Ingestas
    if os.path.exists(PARAMS_DIR):
        for filename in os.listdir(PARAMS_DIR):
            if filename.endswith('.json'):
                atlas_term = filename[:-5]  # Remove .json extension
                json_file_cache[atlas_term] = os.path.join(PARAMS_DIR, filename)
    
    # Search recursively in atlas_prod-main
    if os.path.exists(ATLAS_PROD_DIR):
        for root, dirs, files in os.walk(ATLAS_PROD_DIR):
            for filename in files:
                if filename.endswith('.json'):
                    atlas_term = filename[:-5]  # Remove .json extension
                    full_path = os.path.join(root, filename)
                    # If duplicate, keep the first one found
                    if atlas_term not in json_file_cache:
                        json_file_cache[atlas_term] = full_path
    
    print(f"Found {len(json_file_cache)} JSON files")


def get_route(atlas_term, entidad):
    """
    Extract the route from a JSON file based on atlas_term.
    Try "ruta_landing" first, then fallback to "directorio_de_archivo_en_hdfs".
    Replace $ENTIDAD with lowercase entidad value.
    Returns (route, error_reason) tuple. route is None if error.
    """
    # Try to find the JSON file in cache
    if atlas_term not in json_file_cache:
        return None, f"FILE_NOT_FOUND: {atlas_term}.json"
    
    json_file = json_file_cache[atlas_term]
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        return None, f"JSON_READ_ERROR: {str(e)}"
    
    # Try to get ruta_landing first
    route = data.get("ruta_landing", "").strip() if "ruta_landing" in data else ""
    
    # If ruta_landing is empty or not found, try fallback key
    if not route:
        route = data.get("directorio_de_archivo_en_hdfs", "").strip() if "directorio_de_archivo_en_hdfs" in data else ""
    
    # If still no route found
    if not route:
        return None, "NO_VALID_ROUTE_KEY"
    
    # Replace $ENTIDAD with lowercase entidad
    route = route.replace("$ENTIDAD", entidad.lower())
    
    # Basic validation: route should not be empty after replacement
    if not route or not route.strip():
        return None, "RUTA_INVALID_AFTER_REPLACEMENT"
    
    return route, None

def main():
    # Build cache first
    build_json_cache()
    
    print("Reading CSV file...")
    try:
        df = pd.read_csv(CSV_FILE, index_col=0)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return
    
    print(f"Processing {len(df)} rows...")
    
    # Create lists to store results
    routes_list = []
    ruta_landing_column = []
    
    for idx, row in df.iterrows():
        atlas_term = row['atlas_term']
        entidad = row['entidad']
        
        print(f"Row {idx}: Processing {atlas_term} for {entidad}...")
        
        route, error = get_route(atlas_term, entidad)
        
        if route is not None:
            # Success: add to routes file and update column
            routes_list.append(route)
            ruta_landing_column.append(route)
            print(f"  ✓ Found: {route}")
        else:
            # Failure: only update column with error reason, don't add to routes file
            ruta_landing_column.append(error)
            print(f"  ✗ Error: {error}")
    
    # Overwrite the ruta_landing column in dataframe
    df['ruta_landing'] = ruta_landing_column
    
    # Save updated CSV (close file first if open)
    print(f"\nSaving updated CSV with {len(df)} rows...")
    try:
        df.to_csv(CSV_FILE)
        print(f"✓ Updated CSV saved to: {CSV_FILE}")
    except PermissionError:
        print(f"⚠ CSV file is locked. Please close it and run again.")
        return
    
    # Save routes file (only successful routes) - completely overwrite
    print(f"\nSaving {len(routes_list)} valid routes to file...")
    with open(OUTPUT_ROUTES_FILE, 'w', encoding='utf-8') as f:
        for route in routes_list:
            f.write(route + '\n')
    print(f"✓ Routes file saved to: {OUTPUT_ROUTES_FILE}")
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Total rows processed: {len(df)}")
    print(f"Successful routes: {len(routes_list)}")
    print(f"Failed routes: {len(df) - len(routes_list)}")
    
    # Count error types
    error_counts = {}
    for error in ruta_landing_column:
        if isinstance(error, str) and (error.startswith("RUTA_") or error.startswith("FILE_") or error.startswith("JSON_")):
            error_type = error.split(":")[0]
            error_counts[error_type] = error_counts.get(error_type, 0) + 1
    
    if error_counts:
        print(f"\nError breakdown:")
        for error_type, count in sorted(error_counts.items()):
            print(f"  {error_type}: {count}")


if __name__ == "__main__":
    main()

