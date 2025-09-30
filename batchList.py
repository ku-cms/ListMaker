#!/usr/bin/env python3
import os
import sys
import subprocess
import threading
import json
import re
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

# ----------------- EOS helpers ----------------- #
def run_xrdfs(path):
    """Run xrdfs ls and return list of non-empty stripped lines."""
    cmd = f"xrdfs root://cmseos.fnal.gov/ ls {path}"
    out = run_command(cmd)
    if not out:
        return []
    lines = [l.strip() for l in out.splitlines() if l.strip()]
    return lines

def detect_tag_and_version_eos(path_or_files):
    """
    Detect campaign tag dir, cmssw tag, and AOD label from EOS path(s) or root filenames.
    Returns: (tag_dir, version_token, cmssw_tag, nano_label)

    Example tag_dir results:
      - "Summer22_130X_SMS"
      - "Summer22_130X_Cascades"
    """
    # Normalize input: either a single string or an iterable of strings
    if isinstance(path_or_files, str):
        combined = path_or_files.upper()
        first_item = path_or_files
    else:
        # join for searching; also keep the first non-empty element for basename checks
        items = [str(p) for p in path_or_files if p is not None]
        combined = " ".join(items).upper()
        first_item = items[0] if items else ""

    # basename to decide family (use first input if possible)
    basename = os.path.basename(first_item).upper() if first_item else ""

    # defaults
    campaign_base = None
    cmssw_tag = "unknown"
    nano_label = "NanoAODvX"

    # ---------- 0) UL tokens (highest priority for UL datasets) ----------
    # If UL token present we want Summer20ULxx_106X mapping
    if "UL18" in combined:
        campaign_base = "Summer20UL18_106X"
        cmssw_tag = "106X"
        nano_label = "NanoAODv9"
    elif "UL17" in combined:
        campaign_base = "Summer20UL17_106X"
        cmssw_tag = "106X"
        nano_label = "NanoAODv9"
    elif "UL16" in combined:
        campaign_base = "Summer20UL16_106X"
        cmssw_tag = "106X"
        nano_label = "NanoAODv9"

    # ---------- 1) AOD token detection (NanoAODv*/MiniAODv*) ----------
    # This is authoritative for mapping when present (unless UL took precedence above).
    aod_to_campaign = {
        "NANOAODV12": ("Summer22_130X", "130X", "NanoAODv12"),
        "NANOAODV9":  ("Summer20UL18_106X", "106X", "NanoAODv9"),
        "NANOAODV7":  ("Summer16_102X", "102X", "NanoAODv7"),
        "MINIAODV4":  ("Summer22_130X", "130X", "MiniAODv4"),  # private-Mini fallback -> Summer22
    }
    # Only run this if we haven't already set campaign_base from UL OR even if we did,
    # we allow UL to override; if UL not set, AOD sets it.
    if campaign_base is None:
        m = re.search(r'(NANOAODV\d+|MINIAODV\d+)', combined, re.IGNORECASE)
        if m:
            token = m.group(0).upper().replace("_", "").replace(" ", "")
            if token in aod_to_campaign:
                campaign_from_aod, cmssw_tag, nano_label = aod_to_campaign[token]
                campaign_base = campaign_from_aod
            else:
                # numeric fallback: interpret the trailing number
                num_m = re.search(r'(\d+)$', token)
                if num_m:
                    ver = int(num_m.group(1))
                    if ver >= 12:
                        campaign_from_aod, cmssw_tag, nano_label = ("Summer22_130X", "130X", f"NanoAODv{ver}")
                        campaign_base = campaign_from_aod
                    elif ver >= 9:
                        campaign_from_aod, cmssw_tag, nano_label = ("Summer20UL18_106X", "106X", f"NanoAODv{ver}")
                        campaign_base = campaign_from_aod
                    else:
                        campaign_from_aod, cmssw_tag, nano_label = ("Summer16_102X", "102X", f"NanoAODv{ver}")
                        campaign_base = campaign_from_aod

    # ---------- 2) Explicit campaign keywords ----------
    # Only apply if campaign still unknown
    if campaign_base is None:
        campaign_keywords = {
            "SUMMER22EE": "Summer22EE_130X",
            "SUMMER22": "Summer22_130X",
            "SUMMER23BPIX": "Summer23BPix_130X",
            "SUMMER23": "Summer23_130X",
            "SUMMER20UL16APV": "Summer20UL16APV_106X",
            "SUMMER20UL16": "Summer20UL16_106X",
            "SUMMER20UL17": "Summer20UL17_106X",
            "SUMMER20UL18": "Summer20UL18_106X",
            "FALL17": "Fall17_102X",
            "AUTUMN18": "Autumn18_102X",
        }
        for key, base in campaign_keywords.items():
            if key in combined:
                campaign_base = base
                parts = base.split("_")
                if len(parts) > 1 and parts[1].endswith("X"):
                    cmssw_tag = parts[1]
                    if cmssw_tag == "130X":
                        nano_label = "NanoAODv12"
                    elif cmssw_tag == "106X":
                        nano_label = "NanoAODv9"
                    elif cmssw_tag == "102X":
                        nano_label = "NanoAODv7"
                break

    # ---------- 3) CMSSW tokens (130X/106X/102X) ----------
    # If still unknown, we can detect explicit cmssw token inside the path and set nano defaults.
    if campaign_base is None:
        if "130X" in combined:
            cmssw_tag = "130X"
            nano_label = "NanoAODv12"
            # prefer a Summer22-like base
            campaign_base = "Summer22_130X"
        elif "106X" in combined:
            cmssw_tag = "106X"
            nano_label = "NanoAODv9"
            campaign_base = "Summer20UL18_106X"
        elif "102X" in combined:
            cmssw_tag = "102X"
            nano_label = "NanoAODv7"
            campaign_base = "Summer16_102X"

    # ---------- 4) Year fallback (lowest priority) ----------
    if campaign_base is None:
        if "2022" in combined:
            campaign_base = "Summer22_130X"
            cmssw_tag = "130X"
            nano_label = "NanoAODv12"
        elif "2023" in combined:
            campaign_base = "Summer23_130X"
            cmssw_tag = "130X"
            nano_label = "NanoAODv12"
        else:
            campaign_base = "UnknownCampaign_unknown"

    # ---------- 5) Family suffix detection (SMS vs Cascades) ----------
    # Use the dataset/file name itself (first_item basename) and case-insensitive checks
    if basename:
        if basename.startswith(("SMS-", "SMS_", "SMS")):
            family_suffix = "SMS"
        elif basename.startswith("SLEPSNUCASCADE") or "CASC" in basename or basename.startswith("SLEP"):
            family_suffix = "Cascades"
        else:
            # if dataset base (directory) contains 'Slep' prefer Cascades
            if "SLEP" in combined and "CASC" in combined:
                family_suffix = "Cascades"
            elif "SMS-" in combined or "SMS_" in combined or "SMS" in combined.split():
                family_suffix = "SMS"
            else:
                family_suffix = "SMS"
    else:
        # fallback default
        family_suffix = "SMS"

    # final tag_dir = e.g. "Summer22_130X_Cascades" or "Summer22_130X_SMS"
    tag_dir = f"{campaign_base}_{family_suffix}"

    # Return (tag_dir, version_token, cmssw_tag, nano_label)
    # Keep version_token same as cmssw_tag for compatibility
    return tag_dir, cmssw_tag, cmssw_tag, nano_label

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
        return f"root://cmseos.fnal.gov/{p}"
    # fallback: treat as relative path under /store/ (best-effort)
    return f"root://cmseos.fnal.gov/{p}"

def write_eos_filelist(outfile, root_files):
    """Write normalized root xrootd URLs to outfile using cmseos prefix; thread-safe."""
    # ensure directory exists (protect against empty dirname)
    dirn = os.path.dirname(outfile)
    if dirn:
        os.makedirs(dirn, exist_ok=True)
    with file_lock:
        # open with "w" (overwrite). If you want append use "a".
        with open(outfile, "w") as f:
            for rf in root_files:
                url = normalize_eos_xrootd_path(rf)
                if url:
                    f.write(url + "\n")

def walk_eos_and_write(eos_base, out_root, is_mini_flag, outpaths):
    """
    Walk EOS base dir, find *_MINI or *_NANO (per is_mini_flag), descend until .root files are found,
    and write lists into out_root/<AODType>/<tag_dir>/...
    'outpaths' should be a set-like container (e.g. set()) that will receive output directories.
    """
    top_entries = run_xrdfs(eos_base)
    if not top_entries:
        print(f"[EOS] no entries under {eos_base}", flush=True)
        return

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
                tag_dir, version_token, cmssw_tag, nano_label = detect_tag_and_version_eos(root_files[0])
                out_tag_dir = tag_dir
                outpath = os.path.join(out_root, AODType, out_tag_dir)
                os.makedirs(outpath, exist_ok=True)
                outpaths.add(outpath + "/")
                outfile = os.path.join(outpath, f"{dataset_name}_{nano_label}_JustinPrivateMC_{out_tag_dir}.txt")
                write_eos_filelist(outfile, root_files)
                found_any = True
                # do not descend into files; continue scanning siblings
                continue
            # otherwise treat entries as directories to descend
            for e in entries:
                low = e.lower()
                # skip obvious file types
                if low.endswith(".root") or low.endswith(".txt") or low.endswith(".log"):
                    continue
                queue.append(e)
        if not found_any:
            print(f"[EOS] no .root files found under {top}", flush=True)

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
    eos_base = "/store/user/lpcsusylep/cascadeMC/"
    walk_eos_and_write(eos_base, output, is_mini, outpaths)

    print("Processing complete.", flush=True)

if __name__ == "__main__":
    main()
    # Note: AN for EXO-25-001 has good starting list for EGamma & Muon datasets if needed
