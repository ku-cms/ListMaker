import os, sys, subprocess, threading, json
from optparse import OptionParser
import concurrent.futures

# Parse options
parser = OptionParser()
parser.add_option("-i", "--idir", dest="directory", help="Input directory containing dataset .txt files.")
parser.add_option("-o", "--odir", dest="output", default="samples/", help="Output directory for .txt and .list files.")
parser.add_option("--mini", action="store_true", dest="mini", default=False, help="Process MiniAOD datasets instead of NanoAOD.")
(options, args) = parser.parse_args()

directory = options.directory
output = options.output
is_mini = options.mini
env_vars = os.environ.copy()
file_lock = threading.Lock()  # Define globally so all threads share the same lock

def run_command(command):
    """Run a shell command efficiently with streaming output."""
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env_vars)
    stdout, _ = process.communicate()
    return stdout.strip()

def get_tags(filename):
    """Extract everything except the version and the version itself from filename."""
    versions = ["_102X", "_106X", "_130X"]
    for version in versions:
        if version in filename:
            # Split the filename into two parts: everything before the version and the version
            base_name = filename.split(version)[0]  # Everything except the version
            version_part = version  # The version itself
            return base_name, version_part  # Return both parts
    return [], []  # Return empty lists if no version found

def get_nanoaod_versions(cmssw, is_mini):
    """Determine the correct AOD versions based on the cmmsw release."""
    if is_mini:
        return ["v4", "v3", "v2"]  # MiniAOD preferred order
    if "130X" in cmssw:
        return ["v12"]
    elif "106X" in cmssw:
        return ["v9"]
    elif "102X" in cmssw:
        return ["v7", "v4"]  # Preferred order
    else:
        return [""]

def get_files_for_dataset(dataset):
    """Query files for a given dataset in parallel."""
    query = f'dasgoclient -query="file dataset={dataset}"'
    return run_command(query).split('\n')

def get_dataset_paths(dataset, yeartag, query_type, version, last_version):
    """Fetch dataset paths."""
    special_campaigns = ["APV", "EE", "BPix"]
    special_campaign = ""
    for sc in special_campaigns:
        if sc in yeartag:
            yeartag = yeartag.replace(sc,'')
            special_campaign = sc
    if "Summer20UL" in yeartag: query = f'dasgoclient -query="dataset=/{dataset}/*{yeartag}NanoAOD{special_campaign}{version}*/{query_type}*"'
    else: query = f'dasgoclient -query="dataset=/{dataset}/*{yeartag}{special_campaign}NanoAOD{version}*/{query_type}*"'
    results = dataset, run_command(query).split("\n")
    if results[1] == [''] and last_version:
        if "Summer20UL" in yeartag: query = f'dasgoclient -query="dataset status=* dataset=/{dataset}/*{yeartag}NanoAOD{special_campaign}{version}*/{query_type}*"'
        else: query = f'dasgoclient -query="dataset status=* dataset=/{dataset}/*{yeartag}{special_campaign}NanoAOD{version}*/{query_type}*"'
        results = dataset, run_command(query).split("\n")
        if results[1] == [''] and last_version:
            print(dataset,"in",yeartag+special_campaign,"not available from",query,flush=True)
        else:
            if "Summer20UL" in yeartag: query = f'dasgoclient -json -query="dataset status=* dataset=/{dataset}/*{yeartag}NanoAOD{special_campaign}{version}*/{query_type}*"'
            else: query = f'dasgoclient -json -query="dataset status=* dataset=/{dataset}/*{yeartag}{special_campaign}NanoAOD{version}*/{query_type}*"'
            results = dataset, run_command(query)
            data = json.loads(results[1])
            status = data[0]['dataset'][0]['status']
            print(dataset,"in",yeartag+special_campaign,"available with dataset status=",status,flush=True)
    return results

def make_filelists(txt_filename, paths):
    """Writes dataset file paths to a text file using parallel fetching."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        results = executor.map(get_files_for_dataset, paths)  # Parallel fetch dataset file paths
        with file_lock:  # Lock file access
            with open(txt_filename, 'a') as dsfile:  # Use append mode ('a') to avoid overwriting
                for file_list in results:
                    for file in file_list:
                        dsfile.write(f"root://cmsxrootd.fnal.gov/{file}\n")

def process_file(filepath, is_mini, is_data, is_sms, output_dir, outpaths):
    """Process a file with a list of datasets."""
    with open(filepath, 'r') as file:
        datasets = [line.strip() for line in file if line.strip() and not line.startswith("#")]

    yeartag, cmssw_version = get_tags(os.path.basename(filepath))
    aod_versions = get_nanoaod_versions(cmssw_version, is_mini)

    if not is_data and (not yeartag or not cmssw_version):
        return
    AODType = "NANO"
    if is_mini: AODType = "MINI"
    outpath = f"{output_dir}/{AODType}/{yeartag}{cmssw_version}"
    if is_data:
        outpath += "_Data"
    elif is_sms:
        outpath += "_SMS"
    outpath += "/"

    os.makedirs(outpath, exist_ok=True)
    outpaths.add(outpath)  # Track processed directories

    if is_data:
        for dataset in datasets:
            dataset_name = "_".join(dataset.split("/")[1:-1])
            txt_filename = f"{outpath}/{dataset_name}_{yeartag}{cmssw_version}_Data.txt"
            make_filelists(txt_filename, [dataset])
    else:
        for dataset in datasets:
            for version in aod_versions:
                dataset_paths = get_dataset_paths(dataset, yeartag, AODType, version, version == aod_versions[-1])
                dataset, paths = dataset_paths

                paths = [path for path in paths if "JME" not in path and "PUFor" not in path and "PU35ForTRK" not in path and "LowPU" not in path]
                is_fs_only = all("FS" in path for path in paths)

                if not is_fs_only:
                    paths = [path for path in paths if "FS" not in path]

                if paths != ['']:
                    txt_filename = f"{outpath}{dataset}.txt"
                    make_filelists(txt_filename, paths)
                    break  # Break after writing one version

def main():
    """Main processing loop."""
    is_data = "data" in directory
    is_sms = "sms" in directory
    skip_files = []
    if not is_sms:
       skip_files = ["102X"]
    all_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".txt") and not any (skip in f for skip in skip_files)]
    outpaths = set()  # Keep track of processed directories

    # Use ThreadPoolExecutor to process files in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_file, file, is_mini, is_data, is_sms, output, outpaths): file for file in all_files}

        # Wait for completion and handle exceptions
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing {futures[future]}: {e}")

    # Run addPath.py only once per unique directory
    for outpath in outpaths:
        subprocess.run(f'python3 addPath.py -p {outpath}', shell=True)
    print("Processing complete.")

if __name__ == "__main__":
    main()
    # Note: AN for EXO-25-001 has good starting list for EGamma & Muon datasets if needed
