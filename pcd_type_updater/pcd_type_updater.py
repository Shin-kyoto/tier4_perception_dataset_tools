#!/usr/bin/env python3
import argparse
from pathlib import Path
import yaml
import subprocess
from convert_pointcloud_types import process_bag, get_rosbag_options
from constant import current_webauto_version
import rosbag2_py


class WebAutoT4DatasetInterface:
    def __init__(self, project_id: str, work_dir_path: Path):
        self.project_id = project_id
        self.work_dir_path = work_dir_path

        self.webauto_version: str = (
            subprocess.run(
                "webauto --version", shell=True, capture_output=True, text=True
            )
            .stdout.rstrip("\n")
            .split(" ")[-1]
        )

        self.current_webauto_version: list = current_webauto_version
        if self.webauto_version not in self.current_webauto_version:
            raise ValueError(
                f"Web.Auto version {self.webauto_version} is not supported. "
                f"Please use one of the following versions: {self.current_webauto_version}"
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
        print(f"Downloading t4dataset: {t4dataset_id}")

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
        dataset_version: str = t4dataset_path_original.name
        rosbag_name: str = sorted(
            sorted(t4dataset_path_original.glob("input_bag"))[0].glob("*.db3")
        )[0].stem
        # 右端から，_0を削除
        rosbag_name = rosbag_name[: rosbag_name.rfind("_")]
        move_cmd = f"mv {t4dataset_path_original} {self.work_dir_path}"
        subprocess.run(move_cmd, shell=True)

        return dataset_version, rosbag_name

    def push(self, t4dataset_path: Path):
        """
        Upload rosbag to Web.Auto
        Args:
            t4dataset_path (Path): Path to the t4dataset
        """
        print(f"Uploading t4dataset: {t4dataset_path}")

        raise NotImplementedError("Not implemented yet")
        upload_cmd = (
            f"webauto data annotation-dataset push "
            f"--project-id {self.project_id} "
            f"--annotation-dataset-path {t4dataset_path}"
        )
        subprocess.run(upload_cmd, shell=True)


def get_topic_nums(bag_path: Path):
    input_bag_path = str(bag_path)
    input_storage_options, input_converter_options = get_rosbag_options(input_bag_path)

    reader = rosbag2_py.SequentialReader()
    reader.open(input_storage_options, input_converter_options)

    topic_types = reader.get_all_topics_and_types()

    # Create a map for quicker lookup
    type_map = {
        topic_types[i].name: topic_types[i].type for i in range(len(topic_types))
    }
    topic_metadata = {
        topic_types[i].name: topic_types[i] for i in range(len(topic_types))
    }

    # bag_path直下にあるyamlファイルを読み込む
    # bag_path直下からmetadata.yamlファイルを探す
    yaml_path = sorted(bag_path.glob("metadata.yaml"))[0]

    with open(yaml_path, "r") as file:
        rosbag_metadata_yaml = yaml.safe_load(file)

    # topic_nameとmessage_countのマップを作成
    topic_nums = {
        topic["topic_metadata"]["name"]: topic["message_count"]
        for topic in rosbag_metadata_yaml["rosbag2_bagfile_information"][
            "topics_with_message_count"
        ]
    }
    return type_map, topic_metadata, topic_nums


def compare(t4dataset_dir_path: Path, rosbag_name_old: str, rosbag_name_new: str):
    """
    変換前後で
        topic数が一緒であるべき
        concatenated_pcdについて
            点群数が一緒であるべき
            xyzの値が一緒であるべき
            intensityの値が、tolerance = hogeで一緒であるべき
    R,Cがdummyであるべき
    """
    bag_path_old: Path = t4dataset_dir_path / rosbag_name_old
    bag_path_new: Path = t4dataset_dir_path / rosbag_name_new

    topic_types_old, topic_metadata_old, topic_nums_old = get_topic_nums(bag_path_old)
    topic_types_new, topic_metadata_new, topic_nums_new = get_topic_nums(bag_path_new)

    # topic_nums_oldとtopic_nums_newが一致するか確認
    assert (
        topic_nums_old == topic_nums_new
    ), "topic_nums_old and topic_nums_new are different"


def main(args):
    # Load YAML configuration
    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    # with tempfile.TemporaryDirectory() as temp_dir:
    if True:
        temp_dir = "/home/shintarotomie/src/github.com/Shin-kyoto/tier4_perception_dataset_tools/pcd_type_updater/"
        work_dir_path = Path(temp_dir)
        webauto_t4dataset_interface = WebAutoT4DatasetInterface(
            project_id=config["project_id"], work_dir_path=work_dir_path
        )

        for t4dataset_id in config["t4dataset_ids"]:
            dataset_version, rosbag_name = webauto_t4dataset_interface.pull(
                t4dataset_id
            )
            print(f"download t4dataset to {work_dir_path}")

            rosbag_path_old: Path = work_dir_path / dataset_version / "input_bag"
            rosbag_path_new: Path = work_dir_path / dataset_version / f"{rosbag_name}"
            process_bag(rosbag_path_old, rosbag_path_new)
            # 元のディレクトリを"input_bag"から"input_bag_old"に変更
            subprocess.run(
                f"mv {rosbag_path_old} {work_dir_path / dataset_version / 'input_bag_old'}",
                shell=True,
            )
            # 処理後のディレクトリを"input_bag"に変更
            subprocess.run(
                f"mv {rosbag_path_new} {work_dir_path / dataset_version / 'input_bag'}",
                shell=True,
            )
            # 2つのrosbagの内容を比較
            compare(
                t4dataset_dir_path=work_dir_path / dataset_version,
                rosbag_name_old="input_bag_old",
                rosbag_name_new="input_bag",
            )
            # 比較結果が問題なければ、元のディレクトリを削除
            # t4datasetのディレクトリをWeb.Autoにアップロード
            # webauto_t4dataset_interface.push(work_dir_path / dataset_version)


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
