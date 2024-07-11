import numpy as np
import yaml
from pathlib import Path
from collections import defaultdict
from typing import NamedTuple
from sensor_msgs.msg import PointCloud2
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message
import ros2_numpy
import rosbag2_py
from convert_pointcloud_types import get_rosbag_options


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
        """
        Creates a dict of topic_name and message_count from the metadata.yaml file
        """
        # load metadata.yaml
        yaml_path = sorted(bag_path.glob("metadata.yaml"))[0]
        with open(yaml_path, "r") as file:
            rosbag_metadata_yaml = yaml.safe_load(file)

        return {
            topic["topic_metadata"]["name"]: topic["message_count"]
            for topic in rosbag_metadata_yaml["rosbag2_bagfile_information"][
                "topics_with_message_count"
            ]
        }

    @staticmethod
    def _topic_infos(reader: rosbag2_py.SequentialReader):
        topic_types = reader.get_all_topics_and_types()

        # create a map for quicker lookup
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
        # check if the number of topics is the same between the old and new rosbags
        if not self.rosbag_new.topic_nums == self.rosbag_old.topic_nums:
            print("Numbers of topic between old rosbag and new rosbag are different")
            return False

        # check if the topic types are the same between the old and new rosbags
        if not self.rosbag_new.topic_type_map == self.rosbag_old.topic_type_map:
            print("Topic types between old rosbag and new rosbag are different")
            return False

        # check if the topic metadata is the same between the old and new rosbags
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

            # intensityの値が、所与のtoleranceの範囲で一緒であるべき
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
