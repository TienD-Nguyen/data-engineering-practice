import boto3
import os
from pathlib import Path
import json
import csv

def log_error(message):
    print(f"[ERROR] - {message}")
    
def log_info(message):
    print(f"[INFO] - {message}")    

def find_all_json_files(root_directory):
    json_files = []
    for dirpath, dirnames, filenames in os.walk(root_directory):
        for filename in filenames:
            if filename.lower().endswith('.json'):
                json_files.append(os.path.join(dirpath, filename))
    return json_files

# def find_all_json_files_pathlib(root_directory):
#     root = Path(root_directory)
#     return [str(path.resolve()) for path in root.rglob('*.json')]

def read_all_json_files(json_files):
    parsed_data = []
    for path in json_files:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                parsed_data.append(data)
        except json.JsonDecodeError as e:
            log_error(f"Error decoding JSON from {path}: {e}")
        except Exception as e:
            log_error(f"Error reading {path}: {e}")
    
    return parsed_data

def flatten_json_data(json_data, parent_key="", sep="."):
    items = []
    
    if isinstance(json_data, dict):
        for key, value in json_data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            items.extend(flatten_json_data(value, new_key, sep=sep).items())
    elif isinstance(json_data, list):
        for index, value in enumerate(json_data):
            new_key = f"{parent_key}{sep}{index}" if parent_key else str(index)
            items.extend(flatten_json_data(value, new_key, sep=sep).items())
    else:
        items.append((parent_key, json_data))
        
    return dict(items)
    
def export_flatten_to_csv(flattened_data, output_dir, source_json_paths):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    for i, flattened_rows in enumerate(flattened_data):
        json_file = Path(source_json_paths[i])
        csv_filename = output_dir / f"{json_file.stem}.csv"
        if not flattened_rows:
            log_info(f"Skipping empty JSON file: {json_file}")
            continue
        fieldnames = flattened_rows.keys()
        
        with open(csv_filename, "w", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(flattened_rows)
            # for row in flattened_rows:
            #     writer.writerow(row)
        log_info("Exported to {csv_filename}")

def main():
    json_files = find_all_json_files(os.getcwd())
    parsed_data = read_all_json_files(json_files)
    flatten_data = [flatten_json_data(entry) for entry in parsed_data]
    export_flatten_to_csv(flatten_data, "output", json_files)

if __name__ == "__main__":
    main()
