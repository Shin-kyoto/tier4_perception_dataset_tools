import argparse
from pathlib import Path
from check_lidar_pts import check_lidar_pts
from fix_lidar_pts import update_with_fixed_value

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["fixed_value", "calculate"], required=True,
                      help="点群数更新モード（fixed_value: 固定値で更新, calculate: 計算して更新）")
    parser.add_argument("--fixed_value", type=int, default=100,
                      help="fixed_valueモード時に設定する点群数")
    args = parser.parse_args()

    # アノテーションファイルのパスを取得
    root_dir = Path("/home/shin/autoware-ml-latest/autoware-ml/data/t4dataset/db_j6_v5")
    annotation_files = list(root_dir.glob("*/*/annotation/sample_annotation.json"))
    
    if not annotation_files:
        raise ValueError(f"アノテーションファイルが見つかりません: {root_dir}")
        
    # すべてのnum_lidar_ptsが0かを確認
    import pdb; pdb.set_trace()
    all_num_lidar_pts_empty = check_lidar_pts(annotation_files, 0)
    
    if all_num_lidar_pts_empty:
        if args.mode == "fixed_value":
            update_with_fixed_value(annotation_files, args.fixed_value)
        else:
            print("calculateモードは未実装です")
    else:
        print("num_lidar_ptsが0でないファイルが存在するため、処理を中断します")

if __name__ == "__main__":
    main()