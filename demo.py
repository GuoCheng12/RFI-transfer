import os

import os
import subprocess
import re
from tqdm import tqdm

def parse_size(size_str):
    """
    Convert a size string with units (e.g., '256.0 KiB') to bytes.

    Args:
        size_str (str): Size string with units
    Returns:
        float: Size in bytes
    """
    units = {
        'B': 1,
        'KiB': 1024,
        'MiB': 1024**2,
        'GiB': 1024**3,
        'TiB': 1024**4,
    }
    match = re.match(r'(\d+\.?\d*)\s*(\w+)', size_str)
    if match:
        value, unit = match.groups()
        return float(value) * units.get(unit, 1)
    return 0

def upload_to_ceph(filepath, source_dir, bucket_name, endpoint_url, uploaded_files):
    """
    Upload a local file to Ceph storage with a progress bar.

    Args:
        filepath (str): Path to the local file
        source_dir (str): Source directory (e.g., 'D:\\')
        bucket_name (str): Ceph bucket name (e.g., 'rfi_data')
        endpoint_url (str): Ceph endpoint URL
        uploaded_files (set): Set of already uploaded file paths
    """
    # Skip if the file has already been uploaded
    if filepath in uploaded_files:
        print(f"{filepath} has already been uploaded, skipping")
        return

    # Calculate the relative path and convert to S3 key (using / separator)
    relative_path = os.path.relpath(filepath, source_dir)
    key = relative_path.replace('\\', '/')

    # Construct the upload command, quoting the filepath to handle spaces
    upload_command = f'aws s3 cp "{filepath}" s3://{bucket_name}/{key} --endpoint-url={endpoint_url}'

    # Print upload start information
    print(f"Starting upload of {filepath} to s3://{bucket_name}/{key}")

    # Execute the command and capture output
    process = subprocess.Popen(upload_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Get file size and create a progress bar
    file_size = os.path.getsize(filepath)
    pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc=os.path.basename(filepath))

    # Read stdout to update progress
    for line in iter(process.stdout.readline, b''):
        line = line.decode().strip()
        match = re.search(r'Completed (\d+\.?\d*\s*\w+)/(\d+\.?\d*\s*\w+)', line)
        if match:
            uploaded_str, total_str = match.groups()
            uploaded = parse_size(uploaded_str)
            # Update progress bar, ensuring it doesn’t exceed file size
            pbar.update(min(uploaded - pbar.n, file_size - pbar.n))

    # Close stdout and wait for the command to complete
    process.stdout.close()
    process.wait()
    pbar.close()

    # Check upload result
    if process.returncode == 0:
        print(f"Successfully uploaded {os.path.basename(filepath)}")
        # Record the successful upload
        with open("uploaded_files.txt", "a", encoding="utf-8") as f:
            f.write(filepath + "\n")
    else:
        print(f"Upload failed for {os.path.basename(filepath)}, return code: {process.returncode}")
        # Record the failed upload
        with open("failed_files.txt", "a", encoding="utf-8") as f:
            f.write(filepath + "\n")

if __name__ == "__main__":
    # Configuration parameters
    source_dir = 'D:\\'  # Source directory (mobile drive root)
    bucket_name = 'rfi_data'  # Ceph bucket name
    endpoint_url = 'http://10.140.31.252'  # Ceph endpoint URL
    target_file = "A.fits"  # The file to start uploading from

    # Load the list of already uploaded files
    if os.path.exists("uploaded_files.txt"):
        with open("uploaded_files.txt", "r", encoding="utf-8") as f:
            uploaded_files = set(line.strip() for line in f)
    else:
        uploaded_files = set()

    # Flag to start uploading once the target file is found
    start_upload = False

    # Traverse the D drive to find and upload .fit files
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            if file.endswith('.fit'):
                filepath = os.path.join(root, file)
                # Check if this is the target file "A.fits"
                if not start_upload and os.path.basename(filepath) == target_file:
                    start_upload = True
                # Upload if we’ve reached or passed the target file
                if start_upload:
                    upload_to_ceph(filepath, source_dir, bucket_name, endpoint_url, uploaded_files)
