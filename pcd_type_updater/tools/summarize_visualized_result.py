import os
import shutil
import yaml
import argparse
from tqdm import tqdm
from typing import List

def combine_log_files(src: str, combined_log_file_path: str) -> None:
    """
    Combines the contents of all .log files from the source directory into a single log file.

    Args:
        src (str): Source directory containing .log files.
        combined_log_file_path (str): Path to the combined log file.
    """
    log_files: List[str] = [f for f in os.listdir(src) if f.endswith('.log')]
    
    if len(log_files) != 1:
        raise ValueError("Expected exactly one .log file in the source directory.")
    
    log_file_path: str = os.path.join(src, log_files[0])
    with open(combined_log_file_path, 'a') as combined_log_file:
        with open(log_file_path, 'r') as log_file:
            for line in log_file:
                combined_log_file.write(line)
        combined_log_file.write("\n")

def summarize_visualized_result(dataset_name: str, yaml_path: str, log_path: str) -> None:
    """
    Summarizes the visualized result by creating a specified dataset directory,
    reading YAML file for dataset IDs, obtaining directory names from a log path,
    and moving matching directories to the dataset directory.

    Args:
        dataset_name (str): Name of the dataset directory to create.
        yaml_path (str): Path to the YAML file containing dataset IDs.
        log_path (str): Path to the directory containing logs.
    """
    # Create dataset_name directory under log_path
    dataset_dir: str = os.path.join(log_path, dataset_name)
    if not os.path.exists(dataset_dir):
        os.makedirs(dataset_dir)

    # Load YAML file
    with open(yaml_path, 'r') as yaml_file:
        yaml_content: dict = yaml.safe_load(yaml_file)

    # Store t4dataset_ids in a list
    t4dataset_ids: List[str] = yaml_content.get('t4dataset_ids', [])

    # Get all directory names under log_path
    directories: List[str] = [d for d in os.listdir(log_path) if os.path.isdir(os.path.join(log_path, d))]

    # Create the combined log file
    combined_log_path: str = os.path.join(dataset_dir, f"{dataset_name}.log")

    # If directory names exist in t4dataset_ids, move them under dataset_name directory
    for dir_name in tqdm(directories, desc="Processing directories"):
        if dir_name in t4dataset_ids:
            src: str = os.path.join(log_path, dir_name)
            dst: str = os.path.join(dataset_dir, dir_name)
            shutil.move(src, dst)

            # Combine .log files from src directory into the combined log file
            combine_log_files(dst, combined_log_path)
            
    print("Log summarization finished successfully.")

def main() -> None:
    parser = argparse.ArgumentParser(description="Dataset preparation script")
    parser.add_argument('--dataset-name', required=True, help="Name of the dataset")
    parser.add_argument('--yaml-path', required=True, help="Path to the YAML file")
    parser.add_argument('--log-path', required=True, help="Path to the log directory")
    args = parser.parse_args()

    summarize_visualized_result(args.dataset_name, args.yaml_path, args.log_path)

if __name__ == '__main__':
    main()
