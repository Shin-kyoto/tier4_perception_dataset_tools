#!/usr/bin/env python3
import argparse
from pathlib import Path
import yaml
import subprocess
from convert_pointcloud_types import process_bag, get_rosbag_options
from constant import current_webauto_versions
import rosbag2_py

# compare
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message
from collections import defaultdict
from typing import NamedTuple
from sensor_msgs.msg import PointCloud2
import numpy as np
import ros2_numpy


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
        dataset_id: str = t4dataset_path_original.parent.name
        rosbag_name: str = sorted(
            sorted(t4dataset_path_original.glob("input_bag"))[0].glob("*.db3")
        )[0].stem
        # 右端から，_0を削除
        rosbag_name = rosbag_name[: rosbag_name.rfind("_")]
        move_cmd = f"mv {t4dataset_path_original} {self.work_dir_path / dataset_id}"
        subprocess.run(move_cmd, shell=True)

        return dataset_id, rosbag_name

    def push(self, t4dataset_path: Path, t4dataset_id: str):
        """
        Upload rosbag to Web.Auto
        Args:
            t4dataset_path (Path): Path to the t4dataset
        """
        print(f"Uploading t4dataset: {t4dataset_path}")

        upload_cmd = (
            f"webauto data annotation-dataset push-version"
            f"--annotation-dataset-id {t4dataset_id}"
            f"--project-id {self.project_id} "
            f"--path {t4dataset_path}"
        )
        subprocess.run(upload_cmd, shell=True)


class PointCloudData(NamedTuple):
    msg: PointCloud2
    timestamp: int


class T4datasetRosbag:
    def __init__(self, bag_path: Path):
        self.bag_path: Path = bag_path

    def read(self):
        rosbag_reader = self._reader(self.bag_path)
        self.topic_type_map, self.topic_metadata = self._topic_infos(rosbag_reader)
        self.topic_nums_yaml: dict[str, int] = self._topic_nums_yaml(self.bag_path)
        self.topic_nums: dict[str, int] = {}
        self.pointcloud_msgs = defaultdict(list)

        while rosbag_reader.has_next():
            (topic_name, data, t) = rosbag_reader.read_next()

            if topic_name not in self.topic_nums:
                self.topic_nums[topic_name] = 1
            else:
                self.topic_nums[topic_name] += 1

            if self.topic_type_map[topic_name] == "sensor_msgs/msg/PointCloud2":
                msg_type = get_message(self.topic_type_map[topic_name])
                msg = deserialize_message(data, msg_type)

                self.pointcloud_msgs[topic_name].append(PointCloudData(msg, t))

    @staticmethod
    def _reader(bag_path: Path) -> rosbag2_py.SequentialReader:
        input_bag_path = str(bag_path)
        input_storage_options, input_converter_options = get_rosbag_options(
            input_bag_path
        )
        reader = rosbag2_py.SequentialReader()
        reader.open(input_storage_options, input_converter_options)

        return reader

    @staticmethod
    def _topic_nums_yaml(bag_path: Path):
        # bag_path直下にあるyamlファイルを読み込む
        # bag_path直下からmetadata.yamlファイルを探す
        yaml_path = sorted(bag_path.glob("metadata.yaml"))[0]

        with open(yaml_path, "r") as file:
            rosbag_metadata_yaml = yaml.safe_load(file)

        # topic_nameとmessage_countのマップを作成
        return {
            topic["topic_metadata"]["name"]: topic["message_count"]
            for topic in rosbag_metadata_yaml["rosbag2_bagfile_information"][
                "topics_with_message_count"
            ]
        }

    @staticmethod
    def _topic_infos(reader: rosbag2_py.SequentialReader):
        topic_types = reader.get_all_topics_and_types()

        # Create a map for quicker lookup
        type_map = {
            topic_types[i].name: topic_types[i].type for i in range(len(topic_types))
        }
        topic_metadata = {
            topic_types[i].name: topic_types[i] for i in range(len(topic_types))
        }

        return type_map, topic_metadata


class ConvertedRosbagValidator:
    def __init__(self, rosbag_path_new: str, rosbag_path_old: str):
        self.rosbag_new = T4datasetRosbag(rosbag_path_new)
        self.rosbag_new.read()
        if not self.check_topic_num_consistency_with_yaml(
            self.rosbag_new.topic_nums, self.rosbag_new.topic_nums_yaml
        ):
            raise ValueError("Numbers of topic between rosbag and yaml are different")

        self.rosbag_old = T4datasetRosbag(rosbag_path_old)
        self.rosbag_old.read()
        self.check_topic_num_consistency_with_yaml(
            self.rosbag_old.topic_nums, self.rosbag_old.topic_nums_yaml
        )

    @staticmethod
    def check_topic_num_consistency_with_yaml(
        topic_nums: dict, topic_nums_yaml: dict
    ) -> bool:
        for topic_name in topic_nums.keys():
            if topic_name in topic_nums_yaml:
                if topic_nums[topic_name] != topic_nums_yaml[topic_name]:
                    print(
                        f"Numbers of topic {topic_name} between rosbag and yaml are different"
                    )
                    return False
            else:
                if topic_nums[topic_name] != 0:
                    print(
                        f"Numbers of topic {topic_name} between rosbag and yaml are different"
                    )
                    return False
        return True

    def _compare_topic_infos(self) -> bool:
        # topic_nums_oldとtopic_nums_newが一致するか確認
        if not self.rosbag_new.topic_nums == self.rosbag_old.topic_nums:
            print("Numbers of topic between old rosbag and new rosbag are different")
            return False

        # topic_types_oldとtopic_types_newが一致するか確認
        if not self.rosbag_new.topic_type_map == self.rosbag_old.topic_type_map:
            print("Topic types between old rosbag and new rosbag are different")
            return False

        # topic_metadata_oldとtopic_metadata_newが一致するか確認
        for topic_name in self.rosbag_new.topic_metadata.keys():
            if not self.rosbag_new.topic_metadata[topic_name].equals(
                self.rosbag_old.topic_metadata[topic_name]
            ):
                print("Topic metadata between old rosbag and new rosbag are different")
                return False

        return True

    @staticmethod
    def _compare_pointcloud_num(
        pointcloud_msgs_new: PointCloud2, pointcloud_msgs_old: PointCloud2
    ) -> bool:
        if pointcloud_msgs_new.msg.width != pointcloud_msgs_old.msg.width:
            return False
        return True

    @staticmethod
    def _compare_xyz_in_pointcloud(
        pointcloud_msgs_new: PointCloud2, pointcloud_msgs_old: PointCloud2
    ) -> bool:
        pointcloud_array_new: np.ndarray = ros2_numpy.numpify(pointcloud_msgs_new.msg)
        pointcloud_array_old: np.ndarray = ros2_numpy.numpify(pointcloud_msgs_old.msg)

        for xyz in ["x", "y", "z"]:
            if not np.allclose(pointcloud_array_new[xyz], pointcloud_array_old[xyz]):
                return False
        return True

    @staticmethod
    def _compare_intensity_in_pointcloud(
        pointcloud_msgs_new: PointCloud2,
        pointcloud_msgs_old: PointCloud2,
        tolerance: float,
    ) -> bool:
        pointcloud_array_new: np.ndarray = ros2_numpy.numpify(pointcloud_msgs_new.msg)
        pointcloud_array_old: np.ndarray = ros2_numpy.numpify(pointcloud_msgs_old.msg)

        if not np.allclose(
            pointcloud_array_new["intensity"],
            pointcloud_array_old["intensity"],
            atol=tolerance,
        ):
            return False
        return True

    def _compare_pointcloud_data(self) -> bool:
        for index, pointcloud_msg_new in enumerate(
            self.rosbag_new.pointcloud_msgs["/sensing/lidar/concatenated/pointcloud"]
        ):
            pointcloud_msg_old = self.rosbag_old.pointcloud_msgs[
                "/sensing/lidar/concatenated/pointcloud"
            ][index]

            # 点群数が一緒であるべき
            if not self._compare_pointcloud_num(pointcloud_msg_new, pointcloud_msg_old):
                print(
                    "Pointcloud number between old rosbag and new rosbag are different"
                )
                return False

            # xyzの値が一緒であるべき
            if not self._compare_xyz_in_pointcloud(
                pointcloud_msg_new, pointcloud_msg_old
            ):
                print("XYZ value between old rosbag and new rosbag are different")
                return False

            # intensityの値が、tolerance = hogeで一緒であるべき
            if not self._compare_intensity_in_pointcloud(
                pointcloud_msg_new, pointcloud_msg_old, tolerance=0.1
            ):
                print("Intensity value between old rosbag and new rosbag are different")
                return False

        return True

    def _check_dummy_field(self) -> bool:
        # R,Cがdummyであるべき
        for pointcloud_msg_new in self.rosbag_new.pointcloud_msgs[
            "/sensing/lidar/concatenated/pointcloud"
        ]:
            pointcloud_array_new: np.ndarray = ros2_numpy.numpify(
                pointcloud_msg_new.msg
            )
            if not np.allclose(pointcloud_array_new["return_type"], 0):
                return False
            if not np.allclose(pointcloud_array_new["channel"], 0):
                return False

        return True

    def compare(self) -> bool:
        if not self._compare_topic_infos():
            return False

        if not self._compare_pointcloud_data():
            return False

        if not self._check_dummy_field():
            return False

        return True


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
            dataset_id, rosbag_name = webauto_t4dataset_interface.pull(t4dataset_id)
            print(f"download t4dataset to {work_dir_path}")

            rosbag_path_old: Path = work_dir_path / dataset_id / "input_bag"
            rosbag_path_new: Path = work_dir_path / dataset_id / f"{rosbag_name}"
            process_bag(rosbag_path_old, rosbag_path_new)
            # 元のディレクトリを"input_bag"から"input_bag_old"に変更
            subprocess.run(
                f"mv {rosbag_path_old} {work_dir_path / dataset_id / 'input_bag_old'}",
                shell=True,
            )
            # 処理後のディレクトリを"input_bag"に変更
            subprocess.run(
                f"mv {rosbag_path_new} {work_dir_path / dataset_id / 'input_bag'}",
                shell=True,
            )
            # 2つのrosbagの内容を比較
            validator = ConvertedRosbagValidator(
                rosbag_path_new=work_dir_path / dataset_id / "input_bag",
                rosbag_path_old=work_dir_path / dataset_id / "input_bag_old",
            )
            if validator.compare():
                print("Validation result: OK")
            else:
                raise ValueError("Validation result: NG")

            # 比較結果が問題なければ、元のディレクトリを削除
            subprocess.run(
                f"rm -r {work_dir_path / dataset_id / 'input_bag_old'}",
                shell=True,
            )
            # t4datasetのディレクトリをWeb.Autoにアップロード
            # webauto_t4dataset_interface.push(work_dir_path / dataset_id, t4dataset_id)

            # アップロード後のディレクトリを削除
            subprocess.run(
                f"rm -r {work_dir_path / dataset_id}",
                shell=True,
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
