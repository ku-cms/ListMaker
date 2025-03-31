import json
import argparse
import os
from deepdiff import DeepDiff

class JSONUpdater:
    def __init__(self, base_file):
        self.base_file = base_file
        self.data = self._load_json(base_file)
    
    def _load_json(self, file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                return json.load(file)
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            return []
    
    def update_with(self, other_files):
        for file in other_files:
            other_data = self._load_json(file)
            self._merge_data(other_data)
    
    def _merge_data(self, new_data):
        existing_processes = {entry["process_name"]: entry for entry in self.data}
        for entry in new_data:
            if entry["process_name"] not in existing_processes:
                self.data.append(entry)
    
    def save(self, output_file=None):
        output_path = output_file if output_file else self.base_file
        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(self.data, file, indent=4, sort_keys=True)
        print(f"Updated JSON saved to {output_path}")

    @staticmethod
    def get_json_files_from_directory(directory):
        return [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".json")]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update a base JSON file with data from other JSON files.",
        epilog=(
        "Example usage:\n"
        "python3 json_updater.py --base base.json --updates update1.json update2.json --output updated.json \n"
        "python3 json_updater.py --base base.json --dir path/to/jsons \n"
        "python3 json_updater.py --base base.json --updates update1.json --dir path/to/jsons --output merged.json \n"
        ),
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--base", "-b", required=True, help="Path to the base JSON file to be updated.")
    parser.add_argument("--updates", "-u", "-i", nargs="*", help="List of JSON files to merge into the base file.")
    parser.add_argument("--dir", "-d", help="Path to a directory containing JSON files to merge.")
    parser.add_argument("--output", "-o", help="Optional output file to save the updated JSON.")
    
    args = parser.parse_args()
    
    update_files = args.updates if args.updates else []
    if args.dir:
        update_files.extend(JSONUpdater.get_json_files_from_directory(args.dir))
    
    if not update_files:
        print("No update files provided.")
        exit(1)
    
    updater = JSONUpdater(args.base)
    updater.update_with(update_files)
    updater.save(args.output)

