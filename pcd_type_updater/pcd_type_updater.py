#!/usr/bin/env python3
import os
import shutil
import argparse
from pathlib import Path
import yaml
import subprocess
import tempfile
import logging
import sys
from convert_pointcloud_types import process_bag
from constant import current_webauto_versions
from compare_bags import ConvertedRosbagValidator


# ロガーの設定
def setup_logger(name, log_file, level=logging.INFO):
    # ファイルハンドラ
    file_handler = logging.FileHandler(log_file)

    # コンソールハンドラ
    console_handler = logging.StreamHandler(sys.stdout)

    # ロガーを設定
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


class WebAutoT4DatasetInterface:
    def __init__(self, project_id: str, work_dir_path: Path, logger: logging.Logger):
        self.project_id = project_id
        self.work_dir_path = work_dir_path
        self.logger: logging.Logger = logger

        self.webauto_version: str = (
            subprocess.run(
                "webauto --version", shell=True, capture_output=True, text=True
            )
            .stdout.rstrip("\n")
            .split(" ")[-1]
        )

        if self.webauto_version not in current_webauto_versions:
            raise ValueError(
                f"Web.Auto version {self.webauto_version} is not supported. "
                f"Please use one of the following versions: {current_webauto_versions}"
            )

    def _extract_dataset_path_from_webauto_pull_output(self, output_str: str) -> str:
        lines = output_str.split("\n")

        dataset_path = None
        for line in lines:
            if "annotation_dataset_path" in line:
                parts = line.split()
                dataset_path = parts[1]
                break
        return dataset_path

    def _download_dataset(self, t4dataset_id: str) -> Path:
        self.logger.info(f"\nDownloading t4dataset: {t4dataset_id}")

        download_cmd = (
            f"webauto data annotation-dataset pull "
            f"--project-id {self.project_id} "
            f"--annotation-dataset-id {t4dataset_id}"
        )
        result = subprocess.run(
            download_cmd, shell=True, capture_output=True, text=True
        )
        download_path: Path = Path(
            self._extract_dataset_path_from_webauto_pull_output(result.stdout)
        )

        return download_path

    def pull(
        self,
        t4dataset_id: str,
    ):
        """
        Pull rosbag from Web.Auto
        Args:
            t4dataset_id (str): T4Dataset ID
        Returns:
            t4dataset_path (Path): Path to the downloaded t4dataset
        """

        t4dataset_path_original: Path = self._download_dataset(t4dataset_id)
        dataset_id: str = t4dataset_path_original.parent.name
        rosbag_name: str = sorted(
            sorted(t4dataset_path_original.glob("input_bag"))[0].glob("*.db3")
        )[0].stem
        # 右端から，_0を削除
        rosbag_name = rosbag_name[: rosbag_name.rfind("_")]
        self.logger.info(f"Rosbag name: {rosbag_name}")

        os.rename(t4dataset_path_original, self.work_dir_path / dataset_id)

        return dataset_id, rosbag_name

    def push(self, t4dataset_path: Path, t4dataset_id: str):
        """
        Upload rosbag to Web.Auto
        Args:
            t4dataset_path (Path): Path to the t4dataset
        """
        self.logger.info(f"Uploading t4dataset: {t4dataset_path}\n")

        upload_cmd = (
            f"webauto data annotation-dataset push-version"
            f"--annotation-dataset-id {t4dataset_id}"
            f"--project-id {self.project_id} "
            f"--path {t4dataset_path}"
        )
        subprocess.run(upload_cmd, shell=True)


def main(args):
    # Load YAML configuration
    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    # log directory
    os.makedirs("./log", exist_ok=True)
    # logger
    logger: logging.Logger = setup_logger("Logger", "./log/pcd_type_updater.log")

    with tempfile.TemporaryDirectory() as temp_dir:
        work_dir_path = Path(temp_dir)
        webauto_t4dataset_interface = WebAutoT4DatasetInterface(
            project_id=config["project_id"], work_dir_path=work_dir_path, logger=logger
        )

        for t4dataset_id in config["t4dataset_ids"]:
            # log directory for each t4dataset
            os.makedirs(f"./log/{t4dataset_id}", exist_ok=True)
            file_handler = logging.FileHandler(
                f"./log/{t4dataset_id}/pcd_type_updater.log"
            )
            logger.addHandler(file_handler)

            dataset_id, rosbag_name = webauto_t4dataset_interface.pull(t4dataset_id)

            rosbag_path_old: Path = work_dir_path / dataset_id / "input_bag"
            rosbag_path_new: Path = work_dir_path / dataset_id / f"{rosbag_name}"
            process_bag(rosbag_path_old, rosbag_path_new)
            # 元のrosbagディレクトリを"input_bag"から"input_bag_old"に変更
            os.rename(rosbag_path_old, work_dir_path / dataset_id / "input_bag_old")

            # 処理後のディレクトリを"input_bag"に変更
            os.rename(rosbag_path_new, work_dir_path / dataset_id / "input_bag")

            # 2つのrosbagの内容を比較
            validator = ConvertedRosbagValidator(
                rosbag_path_new=work_dir_path / dataset_id / "input_bag",
                rosbag_path_old=work_dir_path / dataset_id / "input_bag_old",
                t4dataset_id=t4dataset_id,
                visualize_intensity=args.visualize_intensity,
                logger=logger,
            )
            if validator.compare():
                logger.info("Validation result: OK")
            else:
                logger.error("Validation result: NG")
                raise ValueError("Validation failed")

            # 比較結果が問題なければ、元のrosbagのディレクトリを削除
            shutil.rmtree(
                work_dir_path / dataset_id / "input_bagの_old", ignore_errors=True
            )

            # t4datasetのディレクトリをWeb.Autoにアップロード
            if args.upload:
                webauto_t4dataset_interface.push(
                    work_dir_path / dataset_id, t4dataset_id
                )

            logger.removeHandler(file_handler)

        for t4dataset_id in config["t4dataset_ids"]:
            # アップロード後のディレクトリを削除
            shutil.rmtree(work_dir_path / dataset_id, ignore_errors=True)


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
    parser.add_argument(
        "--visualize-intensity",
        action="store_true",
        default=False,
        help="Visualize intensity values",
    )

    args = parser.parse_args()
    main(args)
