#!/usr/bin/env python3

import os
import sys


def get_relative_files(directory):
    files = set()

    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            fullpath = os.path.join(root, filename)
            relpath = os.path.relpath(fullpath, directory)
            files.add(relpath)

    return files


def sorted_file_contents(path):
    with open(path, "r") as f:
        return sorted(f.readlines())


def main():

    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} DIR1 DIR2")
        sys.exit(1)

    dir1 = sys.argv[1].rstrip("/")
    dir2 = sys.argv[2].rstrip("/")

    files1 = get_relative_files(dir1)
    files2 = get_relative_files(dir2)

    print("=== Checking file lists ===")

    only_in_dir1 = sorted(files1 - files2)
    only_in_dir2 = sorted(files2 - files1)

    for f in only_in_dir1:
        print(f"Only in {dir1}: {f}")

    for f in only_in_dir2:
        print(f"Only in {dir2}: {f}")

    print()
    print("=== Checking file contents (sorted) ===")

    common_files = sorted(files1 & files2)

    for relpath in common_files:

        file1 = os.path.join(dir1, relpath)
        file2 = os.path.join(dir2, relpath)

        try:
            contents1 = sorted_file_contents(file1)
            contents2 = sorted_file_contents(file2)

            if contents1 != contents2:
                print(f"DIFFER: {relpath}")

        except Exception as e:
            print(f"ERROR reading {relpath}: {e}")

    print()
    print("Done.")


if __name__ == "__main__":
    main()

