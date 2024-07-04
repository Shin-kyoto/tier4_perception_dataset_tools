import os
from pathlib import Path
import sys
import argparse

from rclpy.serialization import deserialize_message, serialize_message
from rosidl_runtime_py.utilities import get_message
from sensor_msgs.msg import PointCloud2, PointField
import rosbag2_py  # noqa
import numpy as np
import ros2_numpy
import shutil

# import debugpy
# debugpy.listen(5678)
# debugpy.wait_for_client()

if os.environ.get("ROSBAG2_PY_TEST_WITH_RTLD_GLOBAL", None) is not None:
    # This is needed on Linux when compiling with clang/libc++.
    # TL;DR This makes class_loader work when using a python extension compiled with libc++.
    #
    # For the fun RTTI ABI details, see https://whatofhow.wordpress.com/2015/03/17/odr-rtti-dso/.
    sys.setdlopenflags(os.RTLD_GLOBAL | os.RTLD_LAZY)


def get_rosbag_options(path, serialization_format="cdr"):
    storage_options = rosbag2_py.StorageOptions(uri=path, storage_id="sqlite3")

    converter_options = rosbag2_py.ConverterOptions(
        input_serialization_format=serialization_format,
        output_serialization_format=serialization_format,
    )

    return storage_options, converter_options


def create_topic(writer, topic_name, metadata):
    """
    Create a new topic.
    :param writer: writer instance
    :param topic_name:
    :param topic_type:
    :param serialization_format:
    :return:
    """
    topic_name = topic_name
    topic = rosbag2_py.TopicMetadata(
        name=topic_name,
        type=metadata.type,
        serialization_format=metadata.serialization_format,
        offered_qos_profiles=metadata.offered_qos_profiles,
    )

    writer.create_topic(topic)


def is_xyz_layout(msg: PointCloud2):
    if len(msg.fields) == 3:
        return False

    x_field: PointField = msg.fields[0]
    y_field: PointField = msg.fields[1]
    z_field: PointField = msg.fields[2]

    if (
        x_field.name != "x"
        or x_field.offset != 0
        or x_field.datatype != PointField.FLOAT32
        or x_field.count != 1
    ):
        return False

    if (
        y_field.name != "y"
        or y_field.offset != 4
        or y_field.datatype != PointField.FLOAT32
        or y_field.count != 1
    ):
        return False

    if (
        z_field.name != "z"
        or z_field.offset != 8
        or z_field.datatype != PointField.FLOAT32
        or z_field.count != 1
    ):
        return False

    return True


def is_old_xyzi_layout(msg: PointCloud2):
    if len(msg.fields) != 4:
        return False

    x_field: PointField = msg.fields[0]
    y_field: PointField = msg.fields[1]
    z_field: PointField = msg.fields[2]
    i_field: PointField = msg.fields[3]

    if (
        x_field.name != "x"
        or x_field.offset != 0
        or x_field.datatype != PointField.FLOAT32
        or x_field.count != 1
    ):
        return False

    if (
        y_field.name != "y"
        or y_field.offset != 4
        or y_field.datatype != PointField.FLOAT32
        or y_field.count != 1
    ):
        return False

    if (
        z_field.name != "z"
        or z_field.offset != 8
        or z_field.datatype != PointField.FLOAT32
        or z_field.count != 1
    ):
        return False

    if (
        i_field.name != "intensity"
        or i_field.offset != 12
        or i_field.datatype != PointField.FLOAT32
        or i_field.count != 1
    ):
        return False

    return True


def is_old_xyzir_layout(msg: PointCloud2):
    if len(msg.fields) != 5:
        return False

    x_field: PointField = msg.fields[0]
    y_field: PointField = msg.fields[1]
    z_field: PointField = msg.fields[2]
    i_field: PointField = msg.fields[3]
    ring_field: PointField = msg.fields[4]

    if (
        x_field.name != "x"
        or x_field.offset != 0
        or x_field.datatype != PointField.FLOAT32
        or x_field.count != 1
    ):
        return False

    if (
        y_field.name != "y"
        or y_field.offset != 4
        or y_field.datatype != PointField.FLOAT32
        or y_field.count != 1
    ):
        return False

    if (
        z_field.name != "z"
        or z_field.offset != 8
        or z_field.datatype != PointField.FLOAT32
        or z_field.count != 1
    ):
        return False

    if (
        i_field.name != "intensity"
        or i_field.offset != 16
        or i_field.datatype != PointField.FLOAT32
        or i_field.count != 1
    ):
        return False

    if (
        ring_field.name != "ring"
        or ring_field.offset != 20
        or ring_field.datatype != PointField.UINT16
        or ring_field.count != 1
    ):
        return False

    return True


def is_old_xyziradrt_layout(msg: PointCloud2):
    if len(msg.fields) != 9:
        return False

    x_field: PointField = msg.fields[0]
    y_field: PointField = msg.fields[1]
    z_field: PointField = msg.fields[2]
    i_field: PointField = msg.fields[3]
    ring_field: PointField = msg.fields[4]
    azimuth_field: PointField = msg.fields[5]
    distance_field: PointField = msg.fields[6]
    return_type_field: PointField = msg.fields[7]
    time_stamp_field: PointField = msg.fields[8]

    if (
        x_field.name != "x"
        or x_field.offset != 0
        or x_field.datatype != PointField.FLOAT32
        or x_field.count != 1
    ):
        return False

    if (
        y_field.name != "y"
        or y_field.offset != 4
        or y_field.datatype != PointField.FLOAT32
        or y_field.count != 1
    ):
        return False

    if (
        z_field.name != "z"
        or z_field.offset != 8
        or z_field.datatype != PointField.FLOAT32
        or z_field.count != 1
    ):
        return False

    if (
        i_field.name != "intensity"
        or i_field.offset != 16
        or i_field.datatype != PointField.FLOAT32
        or i_field.count != 1
    ):
        return False

    if (
        ring_field.name != "ring"
        or ring_field.offset != 20
        or ring_field.datatype != PointField.UINT16
        or ring_field.count != 1
    ):
        return False

    if (
        azimuth_field.name != "azimuth"
        or azimuth_field.offset != 24
        or azimuth_field.datatype != PointField.FLOAT32
        or azimuth_field.count != 1
    ):
        return False

    if (
        distance_field.name != "distance"
        or distance_field.offset != 28
        or distance_field.datatype != PointField.FLOAT32
        or distance_field.count != 1
    ):
        return False

    if (
        return_type_field.name != "return_type"
        or return_type_field.offset != 32
        or return_type_field.datatype != PointField.UINT8
        or return_type_field.count != 1
    ):
        return False

    if (
        time_stamp_field.name != "time_stamp"
        or time_stamp_field.offset != 40
        or time_stamp_field.datatype != PointField.FLOAT64
        or time_stamp_field.count != 1
    ):
        return False

    return True


def is_new_xyzirc_layout(msg: PointCloud2):
    if len(msg.fields) != 6:
        return False

    x_field: PointField = msg.fields[0]
    y_field: PointField = msg.fields[1]
    z_field: PointField = msg.fields[2]
    i_field: PointField = msg.fields[3]
    return_type_field: PointField = msg.fields[4]
    channel_field: PointField = msg.fields[5]

    if (
        x_field.name != "x"
        or x_field.offset != 0
        or x_field.datatype != PointField.FLOAT32
        or x_field.count != 1
    ):
        return False

    if (
        y_field.name != "y"
        or y_field.offset != 4
        or y_field.datatype != PointField.FLOAT32
        or y_field.count != 1
    ):
        return False

    if (
        z_field.name != "z"
        or z_field.offset != 8
        or z_field.datatype != PointField.FLOAT32
        or z_field.count != 1
    ):
        return False

    if (
        i_field.name != "intensity"
        or i_field.offset != 12
        or i_field.datatype != PointField.UINT8
        or i_field.count != 1
    ):
        return False

    if (
        return_type_field.name != "return_type"
        or return_type_field.offset != 13
        or return_type_field.datatype != PointField.UINT8
        or return_type_field.count != 1
    ):
        return False

    if (
        channel_field.name != "channel"
        or channel_field.offset != 14
        or channel_field.datatype != PointField.UINT16
        or channel_field.count != 1
    ):
        return False

    return True


def is_new_xyzircaedt_layout(msg: PointCloud2):
    if len(msg.fields) != 10:
        return False

    x_field: PointField = msg.fields[0]
    y_field: PointField = msg.fields[1]
    z_field: PointField = msg.fields[2]
    i_field: PointField = msg.fields[3]
    return_type_field: PointField = msg.fields[4]
    channel_field: PointField = msg.fields[5]
    azimuth_field: PointField = msg.fields[6]
    elevation_field: PointField = msg.fields[7]
    distance_field: PointField = msg.fields[8]
    time_stamp_field: PointField = msg.fields[9]

    if (
        x_field.name != "x"
        or x_field.offset != 0
        or x_field.datatype != PointField.FLOAT32
        or x_field.count != 1
    ):
        return False

    if (
        y_field.name != "y"
        or y_field.offset != 4
        or y_field.datatype != PointField.FLOAT32
        or y_field.count != 1
    ):
        return False

    if (
        z_field.name != "z"
        or z_field.offset != 8
        or z_field.datatype != PointField.FLOAT32
        or z_field.count != 1
    ):
        return False

    if (
        i_field.name != "intensity"
        or i_field.offset != 12
        or i_field.datatype != PointField.UINT8
        or i_field.count != 1
    ):
        return False

    if (
        return_type_field.name != "return_type"
        or return_type_field.offset != 13
        or return_type_field.datatype != PointField.UINT8
        or return_type_field.count != 1
    ):
        return False

    if (
        channel_field.name != "channel"
        or channel_field.offset != 14
        or channel_field.datatype != PointField.UINT16
        or channel_field.count != 1
    ):
        return False

    if (
        azimuth_field.name != "azimuth"
        or azimuth_field.offset != 16
        or azimuth_field.datatype != PointField.FLOAT32
        or azimuth_field.count != 1
    ):
        return False

    if (
        elevation_field.name != "elevation"
        or elevation_field.offset != 20
        or elevation_field.datatype != PointField.FLOAT32
        or elevation_field.count != 1
    ):
        return False

    if (
        distance_field.name != "distance"
        or distance_field.offset != 24
        or distance_field.datatype != PointField.FLOAT32
        or distance_field.count != 1
    ):
        return False

    if (
        time_stamp_field.name != "time_stamp"
        or time_stamp_field.offset != 28
        or time_stamp_field.datatype != PointField.UINT32
        or time_stamp_field.count != 1
    ):
        return False

    return True


def convert_xyzi_to_xyzirc(msg: PointCloud2):
    input_array = ros2_numpy.numpify(msg)
    num_points = len(input_array["intensity"])
    if type(input_array) is np.ndarray:
        converted_array = np.zeros(
            (num_points,),
            dtype=[
                ("x", np.float32),
                ("y", np.float32),
                ("z", np.float32),
                ("intensity", np.uint8),
                ("return_type", np.uint8),
                ("channel", np.uint16),
            ],
        )
        converted_array["x"] = input_array["x"]
        converted_array["y"] = input_array["y"]
        converted_array["z"] = input_array["z"]
        converted_array["intensity"] = input_array["intensity"].astype(np.uint8)
    elif "xyz" in input_array:
        converted_array = {
            "xyz": np.zeros((num_points, 3), dtype=np.float32),
            "intensity": np.zeros((num_points, 1), dtype=np.uint8),
            "return_type": np.zeros((num_points, 1), dtype=np.uint8),
            "channel": np.zeros((num_points, 1), dtype=np.uint16),
        }
        converted_array["xyz"] = input_array["xyz"]
        converted_array["intensity"] = input_array["intensity"].astype(np.uint8)
    else:
        raise ValueError("No x, y, z fields found in the input array")

    converted_msg = ros2_numpy.msgify(PointCloud2, converted_array)
    converted_msg.header = msg.header

    assert is_new_xyzirc_layout(converted_msg)

    return converted_msg


def convert_xyzir_to_xyzirc(msg: PointCloud2):
    input_array = ros2_numpy.numpify(msg)
    num_points = len(input_array)

    converted_array = np.zeros(
        (num_points,),
        dtype=[
            ("x", np.float32),
            ("y", np.float32),
            ("z", np.float32),
            ("intensity", np.uint8),
            ("return_type", np.uint8),
            ("channel", np.uint16),
        ],
    )

    converted_array["x"] = input_array["x"]
    converted_array["y"] = input_array["y"]
    converted_array["z"] = input_array["z"]
    converted_array["intensity"] = input_array["intensity"].astype(np.uint8)
    converted_array["channel"] = input_array["ring"]

    converted_msg = ros2_numpy.msgify(PointCloud2, converted_array)
    converted_msg.header = msg.header

    assert is_new_xyzirc_layout(converted_msg)

    return converted_msg


def convert_xyziradrt_to_xyzircaedt(msg: PointCloud2):
    input_array = ros2_numpy.numpify(msg)
    num_points = len(input_array)

    converted_array = np.zeros(
        (num_points,),
        dtype=[
            ("x", np.float32),
            ("y", np.float32),
            ("z", np.float32),
            ("intensity", np.uint8),
            ("return_type", np.uint8),
            ("channel", np.uint16),
            ("azimuth", np.float32),
            ("elevation", np.float32),
            ("distance", np.float32),
            ("time_stamp", np.uint32),
        ],
    )

    converted_array["x"] = input_array["x"]
    converted_array["y"] = input_array["y"]
    converted_array["z"] = input_array["z"]
    converted_array["intensity"] = input_array["intensity"].astype(np.uint8)
    converted_array["channel"] = input_array["ring"]

    converted_array["azimuth"] = input_array["azimuth"] * (np.pi / 18000.0)
    converted_array["distance"] = input_array["distance"]

    # Old timestamps are a double !
    msg_stamp_seconds = msg.header.stamp.sec + 1e-9 * msg.header.stamp.nanosec
    point_rel_seconds = input_array["time_stamp"] - msg_stamp_seconds
    point_rel_nanoseconds = (1e9 * point_rel_seconds).astype(np.uint64)
    assert (point_rel_nanoseconds < np.iinfo(np.uint32).max).all()
    converted_array["time_stamp"] = point_rel_nanoseconds.astype(np.uint32)

    converted_msg = ros2_numpy.msgify(PointCloud2, converted_array)
    converted_msg.header = msg.header

    assert is_new_xyzircaedt_layout(converted_msg)

    return converted_msg


def process_bag(bag_path: Path, output_folder: Path):
    # Bag Reader
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

    # Bag Writer
    output_bag_path = str(output_folder)
    output_storage_options, output_converter_options = get_rosbag_options(
        output_bag_path
    )

    writer = rosbag2_py.SequentialWriter()

    writer.open(output_storage_options, output_converter_options)

    [
        create_topic(writer, tname, metadata)
        for tname, metadata in topic_metadata.items()
    ]

    while reader.has_next():
        (topic, data, t) = reader.read_next()

        if type_map[topic] == "sensor_msgs/msg/PointCloud2":
            msg_type = get_message(type_map[topic])
            msg = deserialize_message(data, msg_type)

            if is_old_xyzi_layout(msg):
                converted_msg = convert_xyzi_to_xyzirc(msg)
            elif is_old_xyzir_layout(msg):
                converted_msg = convert_xyzir_to_xyzirc(msg)
            elif is_old_xyziradrt_layout(msg):
                converted_msg = convert_xyziradrt_to_xyzircaedt(msg)
            elif is_xyz_layout(msg):
                converted_msg = msg
            else:
                print("Unsupported layout !. leaving the pointcloud as is")
                converted_msg = msg

            writer.write(topic, serialize_message(converted_msg), t)
        else:
            writer.write(topic, data, t)

    del writer


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)

    args = parser.parse_args()

    bag_paths = sorted(Path(args.input_path).glob("*.db3"))

    print(f"bags = {bag_paths}")

    Path(args.output_path).mkdir(exist_ok=True)

    for bag_path in bag_paths:
        print(f"Processing: {bag_path}")
        process_bag(Path(bag_path), "tmp_bag")

        # This will create the following file:
        # tmp_bag/tmp_bag_0.db3
        Path("tmp_bag/tmp_bag_0.db3").rename(
            Path(args.output_path) / Path(bag_path).name
        )
        shutil.rmtree("tmp_bag")
