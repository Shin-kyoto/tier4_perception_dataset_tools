# tier4_perception_dataset_tools

## pcd_type_updater

This script helps update pointcloud type from `XYZI` to `XYZIRC`.

Prerequisite:

- Please use Webauto version `refs/tags/v0.35.0`.
- clone `ros2_numpy` and `source ros2_numpy/install/setup.bash`

First, please add t4dataset id in `./config/t4dataset_ids.yaml`.

Second, run the follwing command. You can convert rosbag from old pcd type to new pcd type.

This code compares the intensity in new rosbag from that in old rosbag. If the difference is lower than `0.1`, `pcd_type_updater` outputs `Validation result: OK`.

If you do not want to upload t4 dataset, remove `--upload` option.

```bash
python pcd_type_updater.py --config ./config/t4dataset_ids.yaml --visualize-intensity --upload
```

### vidualize-intensity

You can visualize the intensity comparison results by using following command.

```bash
python pcd_type_updater.py --config ./config/t4dataset_ids.yaml --visualize-intensity
```

You can see the visualized result in `log` directory.

```bash
log/b5853869-782f-466e-afc0-ce58f6f78621
├── intensity_comparison_b5853869-782f-466e-afc0-ce58f6f78621.html
├── intensity_comparison_b5853869-782f-466e-afc0-ce58f6f78621.png
└── pcd_type_updater.log
```
