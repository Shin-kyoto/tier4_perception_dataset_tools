import json
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

def update_with_fixed_value(annotation_files: list[Path], fixed_value: int) -> None:
    """各アノテーションファイルのnum_lidar_ptsを固定値で更新する

    Args:
        annotation_files (list[Path]): アノテーションファイルのパスのリスト
        fixed_value (int): 設定する固定の点群数
    """
    logger = setup_logger(__name__)
    
    for file in tqdm(annotation_files, desc=f"num_lidar_ptsを{fixed_value}に設定中"):
        with open(file, "r") as f:
            data = json.load(f)
            
        # すべてのアノテーションの点群数を更新
        for annotation in data:
            annotation["num_lidar_pts"] = fixed_value
            
        # 更新したデータを保存
        with open(file, "w") as f:
            json.dump(data, f, indent=2)
            
        logger.info(f"{file}の点群数を{fixed_value}に更新しました") 