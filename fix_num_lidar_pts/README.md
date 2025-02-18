# Fix number of lidar points

## Setup uv environment

```bash
export PATH="$HOME/.local/bin:$PATH"
```

```bash
uv venv --python=python3.11 .fix-num-lidar-pts
```

```bash
uv pip install --editable .
```

```bash
source .fix-num-lidar-pts/bin/activate
```

## Usage

### 固定値で更新する場合

```bash
python main.py --mode fixed_value --fixed_value 100
```

### 点群数を計算して更新する場合（未実装）

```bash
python main.py --mode calculate
```

### 点群数がある固定値になっているかどうかを確認する場合

```bash
python check_lidar_pts.py --fixed_value 100
```

## オプション

- `--mode`: 更新モードを指定（required）
  - `fixed_value`: 固定値で更新
  - `calculate`: 点群数を計算して更新（未実装）
- `--fixed_value`: fixed_valueモード時に設定する点群数（default: 100）

## 処理の流れ

1. アノテーションファイルの`num_lidar_pts`が0であることを確認
2. モードに応じて処理を実行
   - `fixed_value`: 指定された固定値で更新
   - `calculate`: 点群数を計算して更新（未実装）

## ファイル構成

- `main.py`: メインスクリプト
- `check_lidar_pts.py`: アノテーションファイルの点群数チェック機能
- `fix_lidar_pts.py`: アノテーションファイルの点群数更新機能
