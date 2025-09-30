#!/usr/bin/env python3
import argparse
import os
import re
import subprocess
import sys
import glob
import shlex
from typing import List, Tuple, Optional

CMS_ENV = "/cvmfs/cms.cern.ch/cmsset_default.sh"
MARKER_OUT = "Wrote output to:"

# -------------------- parsing helpers --------------------
def _sanitize_token(s: str, maxlen: int = 80) -> str:
    if not s:
        return ""
    t = re.sub(r'[\\/:\s]+', '_', s)
    t = re.sub(r'[^0-9A-Za-z._\-]+', '_', t)
    t = re.sub(r'_+', '_', t).strip('_')
    return t[:maxlen] if len(t) > maxlen else t

def _derive_log_token(args_str: str, item: str, procid: int) -> str:
    m = re.search(r'([A-Za-z0-9._\-]+_\d+)\.txt\b', args_str)
    if m:
        return _sanitize_token(m.group(1))
    m2 = re.search(r'([^/\s"\'`]+)\.txt\b', args_str)
    if m2:
        return _sanitize_token(m2.group(1))
    base_item = os.path.basename(item)
    base_item = re.sub(r'\.root$', '', base_item, flags=re.I)
    if base_item:
        return _sanitize_token(f"{base_item}_{procid}")
    return f"job_{procid}"

def _derive_dataset_token(item: str, args_str: str = "", txt_token: str = "") -> str:
    """
    Single, authoritative dataset token generator:
     - prefer explicit txt_token basename (no .txt)
     - else use any name_N.txt in args_str
     - else use basename of URL/file (strip .root)
     - strip trailing _NNN suffix
     - never return empty or 'root'
    """
    candidate = None

    if txt_token:
        candidate = _sanitize_token(os.path.splitext(os.path.basename(txt_token))[0])

    if not candidate:
        m = re.search(r'([A-Za-z0-9._\-]+_\d+)\.txt\b', args_str)
        if m:
            candidate = _sanitize_token(m.group(1))

    if not candidate and item:
        scheme_match = re.match(r'^[A-Za-z0-9+.\-]+://(.*)', item)
        if scheme_match:
            trailing = scheme_match.group(1)
            base = os.path.basename(trailing) or trailing.split('/')[0]
            base = re.sub(r'\.root$', '', base, flags=re.I)
            candidate = _sanitize_token(base)

    if not candidate and item:
        base = os.path.basename(item)
        base = re.sub(r'\.root$', '', base, flags=re.I)
        candidate = _sanitize_token(base)

    if candidate:
        candidate = re.sub(r'_\d+$', '', candidate)

    if not candidate or candidate.lower() == "root":
        candidate = "dataset"

    return candidate

# -------------------- file helpers --------------------
def read_file(path: str) -> Optional[str]:
    try:
        with open(path, "r", errors="ignore") as f:
            return f.read()
    except Exception:
        return None

def find_submit_file(base_dir: str, submit_name: str) -> Optional[str]:
    p1 = os.path.join(base_dir, f"{submit_name}.sub")
    if os.path.isfile(p1):
        return p1
    srcdir = os.path.join(base_dir, "src")
    if os.path.isdir(srcdir):
        for fn in os.listdir(srcdir):
            if fn.endswith(".submit"):
                return os.path.join(srcdir, fn)
    for root, _, files in os.walk(base_dir):
        for fn in files:
            if fn.endswith(".submit"):
                return os.path.join(root, fn)
    return None

def extract_line_value(content: str, key: str) -> Optional[str]:
    pattern = re.compile(r'^\s*' + re.escape(key) + r'\s*=\s*(.+)$', flags=re.M)
    m = pattern.search(content)
    if m:
        return m.group(1).strip()
    return None

def extract_transfer_output_remaps(content: str) -> Optional[str]:
    pattern = re.compile(r'^\s*transfer_output_remaps\s*=\s*"(.*?)"\s*$', flags=re.M)
    m = pattern.search(content)
    if not m:
        pattern2 = re.compile(r'^\s*transfer_output_remaps\s*=\s*(.+)$', flags=re.M)
        m2 = pattern2.search(content)
        if m2:
            txt = m2.group(1).strip()
        else:
            return None
    else:
        txt = m.group(1)
    parts = txt.split(';')
    for part in parts:
        if '=' in part:
            lhs, rhs = part.split('=', 1)
            if '.txt' in rhs or '.txt' in lhs:
                return rhs.strip().strip('"')
    if '=' in parts[0]:
        return parts[0].split('=', 1)[1].strip().strip('"')
    return None

def extract_queue_from_path(content: str) -> Optional[str]:
    matches = re.findall(r'^\s*queue\b.*\bfrom\b\s+(.+)$', content, flags=re.M | re.I)
    if not matches:
        return None
    last = matches[-1].strip().rstrip(';').strip()
    last = last.split("#", 1)[0].strip()
    if (last.startswith('"') and last.endswith('"')) or (last.startswith("'") and last.endswith("'")):
        return last[1:-1]
    toks = last.split()
    return toks[-1]

# -------------------- template expansion --------------------
def expand_template(s: str, item: str, procid: int, cwd_replacement: Optional[str] = None) -> str:
    if s is None:
        return ""
    out = s
    if cwd_replacement is None:
        cwd_replacement = os.getcwd()
    out = out.replace("$ENV(PWD)", cwd_replacement)
    out = out.replace("$(Item)", item)
    out = out.replace("$(ITEM)", item)
    out = out.replace("${Item}", item)
    out = out.replace("$(ProcId)", str(procid))
    out = out.replace("$(PROCId)", str(procid))
    out = out.replace("$(PROCID)", str(procid))
    out = out.replace("$(ProcID)", str(procid))
    if out.startswith('"') and out.endswith('"'):
        out = out[1:-1]
    return out

# -------------------- checks --------------------
def _file_nonzero(p: str) -> bool:
    try:
        return os.path.exists(p) and os.path.getsize(p) > 0
    except Exception:
        return False

def out_file_ok(out_path: str) -> bool:
    if not os.path.exists(out_path):
        return False
    try:
        subprocess.check_call(['bash', '-c', f'grep -q "{MARKER_OUT}" {out_path}'])
        return True
    except subprocess.CalledProcessError:
        return False
    except Exception:
        return False

# -------------------- resubmit writer --------------------
def make_resubmit_header_from_submit_content(submit_content: str, submit_name: str, forced_dataset: Optional[str] = None) -> str:
    exec_line = extract_line_value(submit_content, "executable") or "execute_script.sh"
    transfer_input = extract_line_value(submit_content, "transfer_input_files") or ""
    req_mem = extract_line_value(submit_content, "request_memory") or "2 GB"

    # Decide proc_type and OS correctly: 130X -> Run3 -> EL9, else -> UL -> SL7
    if "130X" in submit_name:
        proc_type = "Run3"
        os_type = '+DesiredOS="EL9"\n'
    else:
        proc_type = "UL"
        os_type = '+DesiredOS="SL7"\n'

    # Use f-string so proc_type is actually substituted into the Arguments line.
    header = (
        "universe = vanilla\n"
        f"executable = {exec_line}\n"
        "use_x509userproxy = true\n"
        f"Arguments = $(Args) $(TxtFile) {proc_type}\n"
    )
    if forced_dataset:
        header += f"Dataset = {forced_dataset}\n"
    header += (
        f"output = $ENV(PWD)/{submit_name}/out/$(Dataset)/$(LogFile).out\n"
        f"error  = $ENV(PWD)/{submit_name}/err/$(Dataset)/$(LogFile).err\n"
        f"log    = $ENV(PWD)/{submit_name}/log/$(Dataset)/$(LogFile).log\n"
        f"request_memory = {req_mem}\n"
    )
    if transfer_input:
        header += f"transfer_input_files = {transfer_input}\n"
    header += (
        "should_transfer_files = YES\n"
        "when_to_transfer_output = ON_EXIT\n"
        "transfer_output_files = $(TxtFile)\n"
        f'transfer_output_remaps = "$(TxtFile)=$ENV(PWD)/{submit_name}/txt/$(Dataset)/$(TxtFile)"\n'
    )
    header += os_type + '\n'
    return header

def write_resubmit_file(resubmit_path: str, submit_name: str, submit_content: str,
                        failed_entries: List[Tuple[int, str, str]], forced_dataset: Optional[str] = None) -> None:
    if not failed_entries:
        raise RuntimeError("No failed entries to write")

    header = make_resubmit_header_from_submit_content(submit_content, submit_name, forced_dataset)

    # determine proc_type same way as make_submit_sh: "130X" -> Run3 else UL
    proc_type_token = "Run3" if "130X" in submit_name else "UL"

    with open(resubmit_path, "w") as f:
        f.write("# AUTO-GENERATED resubmit file (standalone)\n")
        f.write("# Header (derived from original submit)\n")
        f.write(header)
        if forced_dataset:
            f.write("queue LogFile, Args, TxtFile from (\n")
        else:
            f.write("queue LogFile, Dataset, Args, TxtFile from (\n")

        for procid, item, args_str in failed_entries:
            safe_item = _sanitize_token(item)[:80] if item else "dataset"

            # log token (prefer explicit name_N.txt in args_str)
            m = re.search(r'([^\s/\\"]+_\d+)\.txt\b', args_str)
            if m:
                log_token = _sanitize_token(m.group(1))
            else:
                log_token = _sanitize_token(f"{safe_item}_{procid}")

            # split args once
            try:
                tokens = shlex.split(args_str)
            except Exception:
                tokens = args_str.strip().split()

            # Search for any token that ends with .txt (case-insensitive), remove it and use as txt_token
            txt_token_raw = None
            txt_index = None
            for i, t in enumerate(tokens):
                if t.lower().endswith('.txt'):
                    txt_index = i
                    break

            if txt_index is not None:
                txt_token_raw = tokens.pop(txt_index)
            else:
                txt_token_raw = f"{safe_item}_{procid}.txt"

            txt_token = os.path.basename(txt_token_raw)

            # Remove the proc_type token (UL or Run3) if it is present in tokens,
            # so the final Arguments = $(Args) $(TxtFile) <ProcType> produces the correct ordering.
            tokens = [t for t in tokens if t != proc_type_token and t.upper() != proc_type_token.upper()]

            if forced_dataset:
                dataset_token = forced_dataset
            else:
                dataset_token = _derive_dataset_token(item, args_str=args_str, txt_token=txt_token)

            # Quote the entire Args field as a single token so condor sees it as one field.
            args_quoted = shlex.quote(" ".join(tokens))

            if forced_dataset:
                f.write(f"{log_token} {args_quoted} {txt_token}\n")
            else:
                f.write(f"{log_token} {dataset_token} {args_quoted} {txt_token}\n")

        f.write(")\n")

# -------------------- main flow --------------------
def parse_args():
    p = argparse.ArgumentParser(description="Check condor submission using original listfile and resubmit failed jobs.")
    p.add_argument("submit_name", nargs="*", help="Optional: one or more submission folder names or submit-file paths. If omitted, all condor_*/src/*.submit under --root-dir will be processed.")
    p.add_argument("--root-dir", default=".", help="Parent path containing the condor submission folders (default: current dir).")
    p.add_argument("--no-submit", action="store_true", help="Do not submit the resubmit file (only write it).")
    p.add_argument("--dry-run", action="store_true", help="Do everything except write the resubmit file; prints planned actions.")
    return p.parse_args()

def build_jobs_from_submit(submit_path: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], List[Tuple[int,str,str]]]:
    content = read_file(submit_path)
    if content is None:
        return None, None, None, None, []
    args_template = extract_line_value(content, "Arguments")
    transfer_remap = extract_transfer_output_remaps(content)
    output_template = extract_line_value(content, "output")
    error_template = extract_line_value(content, "error")
    queue_path = extract_queue_from_path(content)

    items: List[Tuple[int,str,str]] = []
    if queue_path:
        submit_dir = os.path.dirname(submit_path)
        possible_path = queue_path
        if not os.path.isabs(possible_path):
            p_candidate = os.path.normpath(os.path.join(submit_dir, possible_path))
            if os.path.isfile(p_candidate):
                listfile = p_candidate
            else:
                p_candidate2 = os.path.normpath(os.path.join(os.getcwd(), possible_path))
                if os.path.isfile(p_candidate2):
                    listfile = p_candidate2
                else:
                    listfile = possible_path
        else:
            listfile = possible_path

        if os.path.isfile(listfile):
            with open(listfile, 'r') as lf:
                raw_lines = [ln.rstrip("\n") for ln in lf]
            clean_lines = [ln.strip() for ln in raw_lines if ln.strip() and not ln.strip().startswith("#")]
            for idx, item in enumerate(clean_lines):
                if args_template:
                    args_str = expand_template(args_template, item, idx)
                else:
                    args_str = item
                items.append((idx, item, args_str))
        else:
            items = []
    return args_template, transfer_remap, output_template, error_template, items

def main():
    opts = parse_args()

    # Build list of submit files to process
    submit_paths = []

    if opts.submit_name:
        # User provided one or more args; try to resolve each
        for arg in opts.submit_name:
            # 1) if it's already a file path to a .submit, accept it
            if os.path.isfile(arg) and arg.endswith(".submit"):
                submit_paths.append(os.path.abspath(arg))
                continue

            # 2) if it's a directory path, try to find a .submit under it
            if os.path.isdir(arg):
                found = find_submit_file(arg, os.path.basename(arg))
                if found:
                    submit_paths.append(os.path.abspath(found))
                    continue

            # 3) try under root-dir: root-dir/arg
            cand = os.path.join(opts.root_dir, arg)
            if os.path.isdir(cand):
                found = find_submit_file(cand, os.path.basename(cand))
                if found:
                    submit_paths.append(os.path.abspath(found))
                    continue

            # 4) try with condor_ prefix under root-dir
            cand2 = os.path.join(opts.root_dir, "condor_" + arg)
            if os.path.isdir(cand2):
                found = find_submit_file(cand2, os.path.basename(cand2))
                if found:
                    submit_paths.append(os.path.abspath(found))
                    continue

            # 5) try glob expansion for user convenience (e.g., "condor_*/src/*.submit")
            glob_matches = glob.glob(arg)
            glob_added = False
            for gm in glob_matches:
                if gm.endswith(".submit") and os.path.isfile(gm):
                    submit_paths.append(os.path.abspath(gm))
                    glob_added = True
            if glob_added:
                continue

            # 6) fallback: try to locate a submit under root-dir/condor_arg/src/*.submit
            fallback = os.path.join(opts.root_dir, f"condor_{arg}", "src", "*.submit")
            gm2 = sorted(glob.glob(fallback))
            if gm2:
                submit_paths.extend([os.path.abspath(x) for x in gm2])
                continue

            # If we reached here, we couldn't resolve the argument; warn user
            print(f"[checkJobs] Warning: could not resolve submit argument '{arg}' to a .submit file or directory. Skipping.", file=sys.stderr)

    else:
        # No args: scan for all condor_*/src/*.submit under root-dir
        pattern = os.path.join(opts.root_dir, "condor_*", "src", "*.submit")
        submit_paths = sorted(glob.glob(pattern))

    if not submit_paths:
        print("[checkJobs] No submit files found to process.", file=sys.stderr)
        sys.exit(1)

    # Process each submit file independently
    any_failures = False
    for submit_path in submit_paths:
        print("\n" + "="*80)
        print(f"[checkJobs] Processing submit: {submit_path}", flush=True)
        print("="*80 + "\n", flush=True)

        # Determine base_dir as the parent of the 'src' directory containing submit
        submit_dir = os.path.dirname(submit_path)
        base_dir = os.path.normpath(os.path.join(submit_dir, ".."))

        if not os.path.isdir(base_dir):
            print(f"[checkJobs] ERROR: derived submission directory not found for {submit_path}: {base_dir}", file=sys.stderr)
            any_failures = True
            continue

        submit_content = read_file(submit_path) or ""
        args_template, transfer_remap, output_template, error_template, items = build_jobs_from_submit(submit_path)

        if not items:
            print(f"[checkJobs] ERROR: could not parse any items from the submit listfile for {submit_path}. Skipping.", file=sys.stderr)
            any_failures = True
            continue

        print(f"[checkJobs] Found {len(items)} items from listfile (submit: {submit_path})", flush=True)

        failed_entries = []
        passed_count = 0

        # Use cwd replacement for $ENV(PWD)
        cwd = os.getcwd()

        for procid, item, args_str in items:
            # Determine expected txt path (same logic as before but we won't use item as dataset token)
            if transfer_remap:
                txt_path = expand_template(transfer_remap, item, procid, cwd_replacement=cwd)
            else:
                m = re.search(r'([^\s"\'`]+\.txt)\b', args_str)
                if m:
                    candidate = m.group(1)
                    if os.path.isabs(candidate):
                        txt_path = candidate
                    elif "/" in candidate or "\\" in candidate:
                        txt_path = os.path.normpath(os.path.join(os.path.dirname(submit_path), candidate))
                        if not os.path.exists(txt_path):
                            txt_path = os.path.join(base_dir, "txt", candidate)
                    else:
                        # candidate is a bare filename like SOME_0.txt; place under base_dir/txt/<dataset_guess>/
                        dataset_guess = item.split()[0] if item and not item.startswith('root://') else ""
                        if dataset_guess:
                            txt_path = os.path.join(base_dir, "txt", dataset_guess, candidate)
                        else:
                            # put directly under <base_dir>/txt/<candidate>
                            txt_path = os.path.join(base_dir, "txt", candidate)
                else:
                    # fallback: build safe path using sanitized item
                    safe_item = _sanitize_token(item)[:80]
                    txt_path = os.path.join(base_dir, "txt", safe_item, f"{safe_item}_{procid}.txt")
        
            # Derive log_token and dataset_token robustly & sanitized
            log_token = _derive_log_token(args_str, item, procid)

            # Use the basename of the expected txt file (if any) as the primary hint
            txt_basename = os.path.basename(txt_path) if txt_path else ""
            dataset_token = _derive_dataset_token(item, args_str=args_str, txt_token=txt_basename)
            
            # out_path (prefer template)
            if output_template:
                out_path = expand_template(output_template, item, procid, cwd_replacement=cwd)
            else:
                out_path = os.path.join(base_dir, "out", dataset_token, f"{log_token}.out")
            
            # err_path
            if error_template:
                err_path = expand_template(error_template, item, procid, cwd_replacement=cwd)
            else:
                err_path = os.path.join(base_dir, "err", dataset_token, f"{log_token}.err")
        
            txt_ok = _file_nonzero(txt_path)
            err_ok = True # err_file_ok(err_path) # Err file not worth checking at this stage from verbose output
            out_ok = out_file_ok(out_path)
        
            if txt_ok and err_ok and out_ok:
                passed_count += 1
            else:
                failed_entries.append((procid, item, args_str))
                reasons = []
                if not txt_ok:
                    reasons.append("missing/empty txt")
                if not err_ok:
                    reasons.append("err has content")
                if not out_ok:
                    reasons.append("out missing or missing marker")
                print(f"[checkJobs] FAIL (proc {procid}): item='{item}' -> reasons: {', '.join(reasons)}", flush=True)

        print(f"[checkJobs] Summary for {os.path.basename(base_dir)}: passed={passed_count}, failed={len(failed_entries)}", flush=True)

        if not failed_entries:
            print(f"[checkJobs] No failed jobs to resubmit for {os.path.basename(base_dir)}.", flush=True)
            continue

        # Compute a single forced_dataset based on the first failed entry so all resubmitted jobs
        # write back into the same directory (avoid per-entry numeric-suffixed dataset dirs).
        first_procid, first_item, first_args = failed_entries[0]
        txt_candidate = None
        try:
            toks = shlex.split(first_args)
            if toks:
                last = toks[-1]
                if last.lower().endswith('.txt'):
                    txt_candidate = os.path.basename(last)
        except Exception:
            pass
        forced_dataset = _derive_dataset_token(first_item, args_str=first_args, txt_token=txt_candidate)

        # Ensure directories exist for resubmit header expectations (use forced_dataset)
        for s in ("out", "err", "log", "txt"):
            try:
                os.makedirs(os.path.join(base_dir, s, forced_dataset), exist_ok=True)
            except Exception:
                pass

        resubmit_name = f"resubmit_failed_{os.path.basename(base_dir)}.sub"
        resubmit_path = os.path.join(base_dir, resubmit_name)

        if opts.dry_run:
            print(f"[checkJobs] Dry run for {os.path.basename(base_dir)}: would write resubmit file with the following failed entries:", flush=True)
            for procid, item, args in failed_entries:
                print(f"   proc={procid}, item='{item}', args='{args}'", flush=True)
            print(f"[checkJobs] Dry-run complete for {os.path.basename(base_dir)}. (would write to: {resubmit_path})", flush=True)
            continue

        try:
            write_resubmit_file(resubmit_path, os.path.basename(base_dir), submit_content, failed_entries, forced_dataset=forced_dataset)
            print(f"[checkJobs] Resubmit file written: {resubmit_path}", flush=True)
        except Exception as e:
            print(f"[checkJobs] ERROR: failed to write resubmit file for {os.path.basename(base_dir)}: {e}", file=sys.stderr)
            any_failures = True
            continue

        if opts.no_submit:
            print(f"[checkJobs] --no-submit specified: not running condor_submit for {os.path.basename(base_dir)}.", flush=True)
            continue

        # submit
        submit_cmd = f"source {CMS_ENV} && condor_submit {resubmit_path}"
        print(f"[checkJobs] Submitting resubmit file for {os.path.basename(base_dir)} with:\n  {submit_cmd}\n", flush=True)
        proc = subprocess.run(submit_cmd, shell=True, executable="/bin/bash", capture_output=True, text=True)
        if proc.returncode != 0:
            print(f"[checkJobs] condor_submit failed for {os.path.basename(base_dir)} with exit code {proc.returncode}", file=sys.stderr)
            print(proc.stdout, file=sys.stderr)
            print(proc.stderr, file=sys.stderr)
            any_failures = True
            continue

        # parse cluster id (best-effort)
        stdout = proc.stdout or ""
        m = re.search(r"submitted to cluster\s+(\d+)", stdout)
        if m:
            cluster = m.group(1)
            per_sub_file = os.path.join(base_dir, "submitted_clusters.txt")
            try:
                with open(per_sub_file, "a") as cf:
                    cf.write(f"{cluster}\n")
            except Exception:
                pass

        print(f"[checkJobs] Resubmit submitted successfully for {os.path.basename(base_dir)}", flush=True)

    # exit with non-zero if any submission processing failed
    if any_failures:
        sys.exit(2)
    print("[checkJobs] All done.", flush=True)
    sys.exit(0)

if __name__ == "__main__":
    main()
