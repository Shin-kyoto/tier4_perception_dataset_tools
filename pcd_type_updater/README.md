# pcd_type_updater

## How to use

install `ros2_numpy`

```bash
cd ./lib
git clone git@github.com:Box-Robotics/ros2_numpy.git
colcon build --symlink-install --cmake-args -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
source install/setup.bash
```

run `pcd_type_updater`

```bash
python pcd_type_updater.py --config ./config/t4dataset_ids.yaml
```
