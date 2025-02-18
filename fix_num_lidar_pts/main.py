import json
import glob
from pathlib import Path
from tqdm import tqdm
import logging

def setup_logger(name, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    return logger

def check_empty_lidar_pts(annotation_files: list[Path]) -> bool:
    """すべてのファイルのnum_lidar_ptsが0かどうかを確認する

    Args:
        annotation_files (list[Path]): アノテーションファイルのパスのリスト

    Returns:
        bool: すべてのファイルのnum_lidar_ptsが0ならTrue
    """
    logger = setup_logger(__name__)
    
    for file in tqdm(annotation_files, desc="アノテーションファイルをチェック中"):
        with open(file, "r") as f:
            data = json.load(f)
            
        if not all(x["num_lidar_pts"] == 0 for x in data):
            logger.info(f"{file}のnum_lidar_ptsが0ではありません")
            return False
            
    logger.info("すべてのファイルのnum_lidar_ptsが0です")
    return True

def main():
    # アノテーションファイルのパスを取得
    root_dir = Path("/home/shin/autoware-ml-latest/autoware-ml/data/t4dataset/db_j6_v5")
    annotation_files = list(root_dir.glob("*/*/annotation/sample_annotation.json"))
    
    if not annotation_files:
        raise ValueError(f"アノテーションファイルが見つかりません: {root_dir}")
        
    # すべてのnum_lidar_ptsが0かを確認
    all_num_lidar_pts_empty = check_empty_lidar_pts(annotation_files)
    
    print(f"すべてのnum_lidar_ptsが0: {all_num_lidar_pts_empty}")

if __name__ == "__main__":
    main()
