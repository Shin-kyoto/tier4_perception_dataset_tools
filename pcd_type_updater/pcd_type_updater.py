#!/usr/bin/env python3
import argparse
from pathlib import Path
import yaml
import subprocess
from convert_pointcloud_types import process_bag


class WebAutoT4DatasetPuller:
    def __init__(self, project_id: str, work_dir_path: Path):
        self.project_id = project_id
        self.work_dir_path = work_dir_path

    def _extract_dataset_path_from_webauto_pull_output(self, output_str: str) -> str:
        lines = output_str.split("\n")

        dataset_path = None
        for line in lines:
            if "t4_dataset_path" in line:
                parts = line.split()
                dataset_path = parts[1]
                break
        return dataset_path

    def _download_dataset(self, t4dataset_id: str, dataset_dir_path: Path) -> None:
        print(f"Downloading t4dataset: {t4dataset_id}")

        download_cmd = (
            f"webauto data t4dataset pull "
            f"--project-id {self.project_id} "
            f"--t4dataset-id {t4dataset_id}"
        )
        result = subprocess.run(
            download_cmd, shell=True, capture_output=True, text=True
        )

        dataset_version = Path(
            self._extract_dataset_path_from_webauto_pull_output(result.stdout)
        ).name
        move_cmd = (
            f"mv {self._extract_dataset_path_from_webauto_pull_output(result.stdout)} "
            f"{dataset_dir_path}"
        )
        subprocess.run(move_cmd, shell=True)

        return dataset_dir_path / dataset_version, dataset_version

    def pull(
        self,
        t4dataset_id: str,
    ):
        """
        Pull rosbag from Web.Auto
        Args:
            project_id (str): Project ID
            vehicle_id (str): Vehicle ID
            time_from (str): Start time in ISO 8601 format
            time_to (str): End time in ISO 8601 format
            output_dir (Path): Output directory
        Returns:
            t4dataset_path (Path): Path to the downloaded t4dataset
        """

        t4dataset_path, dataset_version = self._download_dataset(
            t4dataset_id, self.work_dir_path
        )

        return t4dataset_path, dataset_version


def main(args):
    # Load YAML configuration
    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    # with tempfile.TemporaryDirectory() as temp_dir:
    if True:
        temp_dir = "/home/shintarotomie/src/github.com/Shin-kyoto/tier4_perception_dataset_tools/pcd_type_updater/"
        work_dir_path = Path(temp_dir)
        t4dataset_puller = WebAutoT4DatasetPuller(
            project_id=config["project_id"], work_dir_path=work_dir_path
        )

        for t4dataset_id in config["t4dataset_ids"]:
            t4dataset_path, dataset_version = t4dataset_puller.pull(t4dataset_id)
            print(f"download t4dataset to {t4dataset_path}")

            rosbag_path = t4dataset_path / "input_bag"
            process_bag(
                rosbag_path, t4dataset_path / f"input_bag_version_{dataset_version}"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update pointcloud in rosbag using configuration from YAML file"
    )
    parser.add_argument("--config", help="Path to the config.yaml file")
    parser.add_argument(
        "--upload",
        action="store_true",
        default=False,
        help="Upload the processed rosbags to webauto",
    )

    args = parser.parse_args()
    main(args)
