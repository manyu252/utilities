import os
import argparse
import time
import xxhash
from collections import defaultdict
from multiprocessing import Pool, cpu_count
import multiprocessing as mp
from functools import partial
import humanize  # For human-readable file sizes

# SETTINGS
CHUNK_SIZE = 1024 * 1024 * 16 # 16MB

def scan_folder(folder):
    folder_files = []
    try:
        for root, _, files in os.walk(folder):
            for name in files:
                try:
                    path = os.path.join(root, name)
                    stat = os.stat(path)
                    folder_files.append({
                        'path': path,
                        'size': stat.st_size,
                        'mtime': stat.st_mtime,
                    })
                except OSError:
                    continue
    except Exception as e:
        print(f"GODDAMN ERROR scanning folder {folder}: {e}")
    return folder_files

def process_folder_chunk(chunk):
    result = []
    for folder in chunk:
        result.extend(scan_folder(folder))
    return result

def scan_files_parallel(base_folders, num_processes=None):
    # Use available CPU cores if not specified
    if num_processes is None:
        num_processes = mp.cpu_count()

    # For a small number of top-level folders, we can parallelize at the folder level
    if len(base_folders) <= num_processes:
        with mp.Pool(processes=min(len(base_folders), num_processes)) as pool:
            results = pool.map(scan_folder, base_folders)
        return [file for sublist in results for file in sublist]

    # For a large number of top-level folders, we can split them into chunks
    else:
        chunk_size = max(1, len(base_folders) // num_processes)
        folder_chunks = [base_folders[i:i+chunk_size] for i in range(0, len(base_folders), chunk_size)]

        with mp.Pool(processes=len(folder_chunks)) as pool:
            results = pool.map(process_folder_chunk, folder_chunks)
        return [file for sublist in results for file in sublist]

# Step 2 – Group by file size to quickly eliminate non-duplicates
def group_by_size(files):
    size_map = defaultdict(list)
    for f in files:
        size_map[f['size']].append(f)
    return {size: group for size, group in size_map.items() if len(group) > 1}

def compute_fast_hash(file_path):
    h = xxhash.xxh64()
    try:
        with open(file_path, 'rb') as f:
            while chunk := f.read(CHUNK_SIZE):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def hash_wrapper(file):
    return (file['path'], file['size'], compute_fast_hash(file['path']))

# Step 4 – Group files with same fast hash
def group_by_hash(files, workers=cpu_count()):
    hash_map = defaultdict(list)
    size_map = {}  # To store file sizes for each hash
    
    with Pool(processes=workers) as pool:
        for path, size, h in pool.imap_unordered(partial(hash_wrapper), files):
            if h:
                hash_map[h].append(path)
                size_map[h] = size  # Store the file size for this hash

    return {hash_: paths for hash_, paths in hash_map.items() if len(paths) > 1}, size_map

def find_duplicates(folders, output_duplicates_file):
    print("[*] SCANNING THESE MOTHERFUCKING FILES...")
    t0 = time.time()
    files = scan_files_parallel(folders)
    t1 = time.time()
    print(f"[*] SCANNING COMPLETED IN {t1 - t0:.2f} SECONDS, BITCH!")
    print(f"[*] FOUND {len(files)} TOTAL GODDAMN FILES!")

    t0 = time.time()
    grouped_by_size = group_by_size(files)
    t1 = time.time()
    print(f"[*] GROUPING BY SIZE COMPLETED IN {t1 - t0:.2f} SECONDS, MOTHERFUCKER!")
    print(f"[*] {len(grouped_by_size)} GROUPS WITH THE SAME DAMN SIZE!")

    duplicates = []
    total_duplicate_size = 0
    duplicate_groups_count = 0

    # For tracking largest duplicates
    duplicate_groups_with_size = []
    
    for size, group in grouped_by_size.items():
        # print(f"[*] Processing size group: {size} bytes with {len(group)} files")
        fast_hash_groups, size_map = group_by_hash(group)
        
        for hash_val, paths in fast_hash_groups.items():
            duplicates.append(paths)
            # For each duplicate group, we count all files except one as wasted space
            wasted_copies = len(paths) - 1
            wasted_size = wasted_copies * size_map[hash_val]
            total_duplicate_size += wasted_size
            duplicate_groups_count += 1

            # Store the group with its size for later analysis
            duplicate_groups_with_size.append({
                'paths': paths,
                'size': size_map[hash_val],
                'count': len(paths),
                'wasted_size': wasted_size
            })

    # Find the group with the most duplicates (by count)
    most_duplicated_group = max(duplicate_groups_with_size, key=lambda x: x['count']) if duplicate_groups_with_size else None

    # Find the group with the largest wasted space
    largest_waste_group = max(duplicate_groups_with_size, key=lambda x: x['wasted_size']) if duplicate_groups_with_size else None

    # Sort groups by wasted space for the report
    t0 = time.time()
    sorted_groups = sorted(duplicate_groups_with_size, key=lambda x: x['wasted_size'], reverse=True)
    t1 = time.time()
    print(f"[*] SORTING THESE DUPLICATE MOTHERFUCKERS BY WASTED SPACE COMPLETED IN {t1 - t0:.2f} SECONDS!")

    print(f"[*] FOUND {duplicate_groups_count} TOTAL DUPLICATE GROUPS, YOU MESSY BASTARD!")
    print(f"[*] TOTAL WASTED MEMORY: {total_duplicate_size} BYTES ({humanize.naturalsize(total_duplicate_size)}) - WHAT THE FUCK WERE YOU THINKING?!")

    if most_duplicated_group:
        print(f"\n[*] MOST DUPLICATED FILE: {most_duplicated_group['count']} GODDAMN COPIES!")
        print(f"    EXAMPLE: {most_duplicated_group['paths'][0]}")
        print(f"    SIZE PER FILE: {humanize.naturalsize(most_duplicated_group['size'])}")
        print(f"    TOTAL WASTED SPACE: {humanize.naturalsize(most_duplicated_group['wasted_size'])} - THAT'S JUST FUCKING STUPID!")

    if largest_waste_group:
        print(f"\n[*] LARGEST WASTE OF SPACE: {humanize.naturalsize(largest_waste_group['wasted_size'])} - ARE YOU SHITTING ME?!")
        print(f"    FROM {largest_waste_group['count']} COPIES OF FILE SIZE {humanize.naturalsize(largest_waste_group['size'])}")
        print(f"    EXAMPLE: {largest_waste_group['paths'][0]}")

    # save duplicates to a file or handle them as needed
    with open(output_duplicates_file, 'w') as f:
        f.write(f"LISTEN UP, MOTHERFUCKER! FOUND {duplicate_groups_count} DUPLICATE GROUPS!\n")
        f.write(f"TOTAL WASTED MEMORY: {total_duplicate_size} BYTES ({humanize.naturalsize(total_duplicate_size)}) - YOU SHOULD BE ASHAMED!\n\n")

        if most_duplicated_group:
            f.write("MOST DUPLICATED FILE (BY COUNT) - THIS IS RIDICULOUS:\n")
            f.write(f"  {most_duplicated_group['count']} DAMN COPIES OF THE SAME FILE!\n")
            f.write(f"  SIZE PER FILE: {most_duplicated_group['size']} BYTES ({humanize.naturalsize(most_duplicated_group['size'])})\n")
            f.write(f"  TOTAL WASTED SPACE: {most_duplicated_group['wasted_size']} BYTES ({humanize.naturalsize(most_duplicated_group['wasted_size'])}) - WHAT THE HELL?!\n")
            f.write("  EXAMPLE: " + most_duplicated_group['paths'][0] + "\n\n")

        if largest_waste_group:
            f.write("LARGEST WASTE OF SPACE - THIS IS WHY YOUR HARD DRIVE IS CRYING:\n")
            f.write(f"  {largest_waste_group['count']} COPIES OF THE SAME DAMN FILE!\n")
            f.write(f"  SIZE PER FILE: {largest_waste_group['size']} BYTES ({humanize.naturalsize(largest_waste_group['size'])})\n")
            f.write(f"  TOTAL WASTED SPACE: {largest_waste_group['wasted_size']} BYTES ({humanize.naturalsize(largest_waste_group['wasted_size'])}) - JESUS CHRIST!\n")
            f.write("  EXAMPLE: " + largest_waste_group['paths'][0] + "\n\n")
        
        f.write("\nALL DUPLICATE GROUPS ORDERED BY WASTED SPACE - READ IT AND WEEP, MOTHERFUCKER:\n")
        f.write("=========================================\n\n")
        for group in sorted_groups:
            f.write(f"GROUP: {group['count']} FILES, {group['size']} BYTES EACH - SAME SHIT, DIFFERENT FOLDER!\n")
            f.write(f"WASTED SPACE: {group['wasted_size']} BYTES ({humanize.naturalsize(group['wasted_size'])}) - COULD'VE DOWNLOADED MORE PORN INSTEAD!\n")
            f.write("FILES:\n")
            for path in group['paths']:
                f.write(f"  {path}\n")
            f.write("\n")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Find duplicate files in specified folders.")
    parser.add_argument('-f', '--folders', nargs='+', required=True, help='Folders to scan for duplicates')
    parser.add_argument('-df', '--duplicates_file', default='duplicates.txt', help='File to save duplicate file paths')
    args = parser.parse_args()

    if args.folders:
        folders_to_scan = args.folders
    else:
        print("NO FOLDERS SPECIFIED, DUMBASS! USING DEFAULT FOLDERS.")

    time_start = time.time()
    find_duplicates(folders_to_scan, args.duplicates_file)
    print("[*] DUPLICATE FILE SEARCH COMPLETED, MOTHERFUCKER!")
    time_end = time.time()
    print(f"[*] TIME TAKEN: {time_end - time_start:.2f} SECONDS - I'VE SEEN GLACIERS MOVE FASTER!")