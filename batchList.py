#!/usr/bin/env python3
import os
import sys
import subprocess
import threading
import json
from optparse import OptionParser
import concurrent.futures
from pathlib import Path

# ----------------- CLI ----------------- #
parser = OptionParser()
parser.add_option("-i", "--idir", dest="directory", help="Input directory containing dataset .txt files.")
parser.add_option("-o", "--odir", dest="output", default="samples/", help="Output directory for .txt and .list files.")
parser.add_option("--mini", action="store_true", dest="mini", default=False, help="Process MiniAOD datasets instead of NanoAOD.")
(options, args) = parser.parse_args()

directory = options.directory
output = options.output.rstrip("/")
is_mini = options.mini

env_vars = os.environ.copy()
file_lock = threading.Lock()

# ----------------- helpers ----------------- #
def run_command(command):
    """Run a shell command and return stdout (strip)."""
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env_vars)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        print(f"[WARN] command failed ({process.returncode}): {command}\nstderr: {stderr.strip()}", flush=True)
    return stdout.strip()

def get_tags(filename):
    """Extract everything except the version and the version itself from filename."""
    versions = ["_102X", "_106X", "_130X"]
    for version in versions:
        if version in filename:
            base_name = filename.split(version)[0]
            version_part = version
            return base_name, version_part
    return "", ""  # return empty strings when not found

def get_nanoaod_versions(cmssw, is_mini_flag):
    if is_mini_flag:
        return ["v6", "v5", "v4", "v3", "v2", ""]
    if "130X" in cmssw:
        return ["v12"]
    elif "106X" in cmssw:
        return ["v9"]
    elif "102X" in cmssw:
        return ["v7", "v4"]
    else:
        return [""]

def get_files_for_dataset(dataset):
    query = f'dasgoclient -query="file dataset={dataset}"'
    return run_command(query).split('\n')

def get_dataset_paths(dataset, yeartag, query_type, version, last_version):
    special_campaigns = ["APV", "EE", "BPix"]
    special_campaign = ""
    for sc in special_campaigns:
        if sc in yeartag:
            yeartag = yeartag.replace(sc,'')
            special_campaign = sc
    AODType = "NanoAOD"
    if is_mini:
        AODType = "MiniAOD"
    if "Summer20UL" in yeartag:
        query = f'dasgoclient -query="dataset=/{dataset}/*{yeartag}{AODType}{special_campaign}{version}*/{query_type}*"'
    else:
        query = f'dasgoclient -query="dataset=/{dataset}/*{yeartag}{special_campaign}{AODType}{version}*/{query_type}*"'
    results = dataset, run_command(query).split("\n")
    if results[1] == [''] and last_version:
        if "Summer20UL" in yeartag:
            query = f'dasgoclient -query="dataset status=* dataset=/{dataset}/*{yeartag}{AODType}{special_campaign}{version}*/{query_type}*"'
        else:
            query = f'dasgoclient -query="dataset status=* dataset=/{dataset}/*{yeartag}{special_campaign}{AODType}{version}*/{query_type}*"'
        results = dataset, run_command(query).split("\n")
        if results[1] == [''] and last_version:
            print(dataset,"in",yeartag+special_campaign,"not available from",query,flush=True)
        else:
            if "Summer20UL" in yeartag:
                query = f'dasgoclient -json -query="dataset status=* dataset=/{dataset}/*{yeartag}{AODType}{special_campaign}{version}*/{query_type}*"'
            else:
                query = f'dasgoclient -json -query="dataset status=* dataset=/{dataset}/*{yeartag}{special_campaign}{AODType}{version}*/{query_type}*"'
            results_json = dataset, run_command(query)
            try:
                data = json.loads(results_json[1])
                status = data[0]['dataset'][0]['status']
                print(dataset,"in",yeartag+special_campaign,"available with dataset status=",status,flush=True)
            except Exception as e:
                print("[WARN] failed to parse JSON dasgoclient response for", dataset, e, flush=True)
    return results

def make_filelists(txt_filename, paths):
    """Write dataset file paths to a text file using dasgoclient for each dataset path in `paths`."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        results = executor.map(get_files_for_dataset, paths)
        with file_lock:
            os.makedirs(os.path.dirname(txt_filename), exist_ok=True)
            with open(txt_filename, 'w') as dsfile:
                for file_list in results:
                    for file in file_list:
                        if file.strip():
                            dsfile.write(f"root://cmsxrootd.fnal.gov/{file}\n")

# ----------------- EOS helpers ----------------- #
def run_xrdfs(path):
    """Run xrdfs ls and return list of non-empty stripped lines."""
    cmd = f"xrdfs root://cmseos.fnal.gov/ ls {path}"
    out = run_command(cmd)
    if not out:
        return []
    lines = [l.strip() for l in out.splitlines() if l.strip()]
    return lines

def detect_tag_and_version_eos(path):
    """First-match-wins heuristic for EOS path. Returns (path, tag_dir, cmssw_tag, nano_label)."""
    p = path.upper()

    # Define patterns as tuples: (substring in path, tag_dir, cmssw_tag, nano_label)
    patterns = [
        ("102X", "16", "Summer16_102X_SMS", "102X", "NanoAODv7"),
        ("102X", "17", "Fall17_102X_SMS", "102X", "NanoAODv7"),
        ("102X", "18", "Autumn18_102X_SMS", "102X", "NanoAODv7"),

        ("106X", "APV", "Summer20UL16APV_106X_SMS", "106X", "NanoAODv9"),
        ("106X", "16", "Summer20UL16_106X_SMS", "106X", "NanoAODv9"),
        ("106X", "17", "Summer20UL17_106X_SMS", "106X", "NanoAODv9"),
        ("106X", "18", "Summer20UL18_106X_SMS", "106X", "NanoAODv9"),
        ("UL", "APV", "Summer20UL16APV_106X_SMS", "106X", "NanoAODv9"),
        ("UL", "16", "Summer20UL16_106X_SMS", "106X", "NanoAODv9"),
        ("UL", "17", "Summer20UL17_106X_SMS", "106X", "NanoAODv9"),
        ("UL", "18", "Summer20UL18_106X_SMS", "106X", "NanoAODv9"),

        ("130X", "EE", "Summer22EE_130X_SMS", "130X", "NanoAODv12"),
        ("130X", "BPIX", "Summer23BPix_130X_SMS", "130X", "NanoAODv12"),
        ("130X", "22", "Summer22_130X_SMS", "130X", "NanoAODv12"),
        ("130X", "23", "Summer23_130X_SMS", "130X", "NanoAODv12"),
        ("130X", "24", "Summer24_130X_SMS", "130X", "NanoAODv12"),
        ("130X", "25", "Summer25_130X_SMS", "130X", "NanoAODv12"),
        ("130X", "26", "Summer26_130X_SMS", "130X", "NanoAODv12"),
    ]

    for main, sub, tag_dir, cmssw_tag, nano_label in patterns:
        if main in p and sub in p:
            return tag_dir, cmssw_tag, nano_label

    print(f"Can't find matching labels for {p}")
    return "UnknownCampaign", "unknown", "NanoAODvX"

def normalize_eos_xrootd_path(p):
    """Ensure EOS path is a full xrootd URL using cmseos redirector.
       - If already starts with root:// keep as-is.
       - If starts with /store/ use 'root://cmseos.fnal.gov' prefix + path.
       - Otherwise best-effort prefix.
    """
    if not p:
        return None
    p = p.strip()
    if p.startswith("root://"):
        return p
    if p.startswith("/"):
        # /store/...
        return f"root://cmseos.fnal.gov{p}"
    # fallback: treat as relative path under /store/ (best-effort)
    return f"root://cmseos.fnal.gov/{p}"

def write_eos_filelist(outfile, root_files):
    """Append normalized root xrootd URLs to outfile using cmseos prefix; thread-safe."""
    with file_lock:
        os.makedirs(os.path.dirname(outfile), exist_ok=True)
        with open(outfile, "w") as f:
            for rf in root_files:
                url = normalize_eos_xrootd_path(rf)
                if url:
                    f.write(url + "\n")

def walk_eos_and_write(eos_base, out_root, is_mini_flag, outpaths):
    """Walk EOS base dir, find *_MINI or *_NANO (per is_mini_flag), descend until .root files are found, write lists."""
    top_entries = run_xrdfs(eos_base)
    if not top_entries:
        print(f"[EOS] no entries under {eos_base}", flush=True)
        return

    # choose suffix exactly per your request
    suffix = "_MINI" if is_mini_flag else "_NANO"

    # keep only entries that end with suffix; fallback to fuzzy match if none found
    candidates = [e for e in top_entries if e.rstrip("/").upper().endswith(suffix)]
    if not candidates:
        candidates = [e for e in top_entries if suffix in e.upper()]

    for top in candidates:
        top = top.rstrip("/")
        dataset_base = os.path.basename(top)
        if dataset_base.upper().endswith("_MINI"):
            AODType = "MINI"
            dataset_name = dataset_base[:-5]
        elif dataset_base.upper().endswith("_NANO"):
            AODType = "NANO"
            dataset_name = dataset_base[:-5]
        else:
            dataset_name = dataset_base
            AODType = "MINI" if is_mini_flag else "NANO"

        # BFS down until we find .root files
        queue = [top]
        found_any = False
        while queue:
            current = queue.pop(0)
            entries = run_xrdfs(current)
            if not entries:
                continue
            # entries are absolute EOS paths from xrdfs; collect .root files
            root_files = [e for e in entries if e.lower().endswith(".root")]
            if root_files:
                tag_dir, year, cmssw_tag, nano_label = detect_tag_and_version_eos(current)
                out_tag_dir = tag_dir
                outpath = os.path.join(out_root, AODType, out_tag_dir)
                os.makedirs(outpath, exist_ok=True)
                # addPath.py expects trailing slash in previous flows; keep that format
                outpaths.add(outpath + "/")
                outfile = os.path.join(outpath, f"{dataset_name}_{nano_label}_JustinPrivateMC_{out_tag_dir}.txt")
                write_eos_filelist(outfile, root_files)
                found_any = True
                # continue scanning siblings (do not descend into files)
                continue
            # otherwise treat entries as directories to descend
            for e in entries:
                low = e.lower()
                if low.endswith(".root") or low.endswith(".txt") or low.endswith(".log"):
                    continue
                queue.append(e)
        if not found_any:
            print(f"[EOS] no .root files found under {top}", flush=True)

# ----------------- processing flow ----------------- #
def process_file(filepath, is_mini_flag, is_data, is_sms, output_dir, outpaths):
    """Process a .txt dataset-list file for DAS flow (unchanged behaviour)."""
    with open(filepath, 'r') as file:
        datasets = [line.strip() for line in file if line.strip() and not line.startswith("#")]

    yeartag, cmssw_version = get_tags(os.path.basename(filepath))
    aod_versions = get_nanoaod_versions(cmssw_version, is_mini_flag)

    if not is_data and (not yeartag or not cmssw_version):
        return
    AODType = "NANO"
    if is_mini_flag: AODType = "MINI"
    outpath = f"{output_dir}/{AODType}/{yeartag}{cmssw_version}"
    if is_data:
        outpath += "_Data"
    elif is_sms:
        outpath += "_SMS"
    outpath += "/"

    os.makedirs(outpath, exist_ok=True)
    outpaths.add(outpath)

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

                # filter rules
                paths = [path for path in paths if "JME" not in path and "PUFor" not in path and "PU35ForTRK" not in path and "LowPU" not in path]
                is_fs_only = all("FS" in path for path in paths)

                if not is_fs_only:
                    paths = [path for path in paths if "FS" not in path]

                if paths != ['']:
                    txt_filename = f"{outpath}{dataset}.txt"
                    make_filelists(txt_filename, paths)
                    break

# ----------------- main ----------------- #
def main():
    # must at least provide -i or rely solely on EOS scanning, so allow directory=None
    is_data = False
    is_sms = False
    if directory:
        is_data = "data" in directory
        is_sms = "sms" in directory

    skip_files = []
    if directory and not is_sms:
       skip_files = ["102X"]

    all_files = []
    if directory:
        all_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".txt") and not any(skip in f for skip in skip_files)]

    outpaths = set()

    # DAS processing (if -i provided)
    if all_files:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(process_file, file, is_mini, is_data, is_sms, output, outpaths): file for file in all_files}
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing {futures[future]}: {e}", flush=True)

    # Call addPath.py once per unique outpath
    for outpath in sorted(outpaths):
        try:
            subprocess.run(f'python3 addPath.py -p {outpath}', shell=True, check=True)
        except subprocess.CalledProcessError as e:
            print(f"[WARN] addPath.py failed for {outpath}: {e}", flush=True)

    # Always run EOS scan (automatic). EOS base hardcoded to cascadeMC path:
    #eos_base = "/store/user/lpcsusylep/cascadeMC/"
    #walk_eos_and_write(eos_base, output, is_mini, outpaths)

    print("Processing complete.", flush=True)

if __name__ == "__main__":
    main()
    # Note: AN for EXO-25-001 has good starting list for EGamma & Muon datasets if needed
