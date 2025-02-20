import argparse
import json
from pathlib import Path
from tqdm import tqdm
import logging


def setup_logger(name, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    return logger


def check_lidar_pts(annotation_files: list[Path], target_value: int) -> bool:
    """すべてのファイルのnum_lidar_ptsが指定の値かどうかを確認する

    Args:
        annotation_files (list[Path]): アノテーションファイルのパスのリスト
        target_value (int): 確認する値

    Returns:
        bool: すべてのファイルのnum_lidar_ptsが指定の値ならTrue
    """
    logger = setup_logger(__name__)

    for file in tqdm(
        annotation_files, desc=f"num_lidar_ptsが{target_value}かチェック中"
    ):
        with open(file, "r") as f:
            data = json.load(f)

        if not all(x["num_lidar_pts"] == target_value for x in data):
            logger.info(f"{file}のnum_lidar_ptsが{target_value}ではありません")
            return False

    logger.info(f"すべてのファイルのnum_lidar_ptsが{target_value}です")
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--fixed_value", type=int, default=100, help="チェックする点群数の値"
    )
    args = parser.parse_args()

    # アノテーションファイルのパスを取得
    root_dir = Path("/home/shin/autoware-ml-latest/autoware-ml/data/t4dataset/db_j6_v5")
    annotation_files = list(root_dir.glob("*/*/annotation/sample_annotation.json"))

    if not annotation_files:
        raise ValueError(f"アノテーションファイルが見つかりません: {root_dir}")

    # 指定された値でチェック
    all_num_lidar_pts_match = check_lidar_pts(annotation_files, args.fixed_value)

    if all_num_lidar_pts_match:
        print(f"すべてのファイルのnum_lidar_ptsが{args.fixed_value}です")
    else:
        print(f"num_lidar_ptsが{args.fixed_value}でないファイルが存在します")


if __name__ == "__main__":
    main()
