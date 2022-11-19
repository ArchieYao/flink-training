## RecoverFromCheckpoint

需要先启动flink集群，本地模式：

```shell
./bin/start-cluster local
```

然后修改 `flink-conf.yaml`

```yaml
# 设置重启策略
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 3
restart-strategy.fixed-delay.delay: 2 s

# 设置快照方式及其保存路径
state.backend: filesystem
state.checkpoints.dir: file:///tmp/flink-training
state.savepoints.dir: file:///tmp/flink-training

# checkpoint 最大保存数量 
state.checkpoints.num-retained: 3
state.backend.local-recovery: true
```

编译本地代码，注意不要把flink相关依赖打到jar中。

```shell
# 编译时需要设置 maven 依赖的 scope 为 provide
mvn clean package -Dmaven.test.skip=true
```

先启动作业，不带checkpoint路径，自行重启会自动从checkpoint中恢复数据，第二次启动选择第一次运行作业的快照，然后最终的结果会累加第一次计算结果。

```shell
./bin/flink run -c archieyao.github.io.RecoverFromCheckpoint /home/archieao/workspace/flink-training/checkpoint/target/checkpoint-1.0-SNAPSHOT.jar
./bin/flink run -s /tmp/flink-training/3a16d77da5dbdc952541a7a89b60546d/chk-1799  -c archieyao.github.io.RecoverFromCheckpoint /home/archieao/workspace/flink-training/checkpoint/target/checkpoint-1.0-SNAPSHOT.jar
```

## RecoverFromSavepoint

编译方式同上，由于去除了作业中的 `exception` ，作业会一直运行，在作业运行中，进行一次 `savepoint`，然后停止作业；

```shell
./bin/flink run -d -c archieyao.github.io.RecoverFromSavepoint /home/archieao/workspace/flink-training/checkpoint/target/checkpoint-1.0-SNAPSHOT.jar
./bin/flink cancel -s d101fa3b66d50a0e4a5a3d17f86cced2
```

再次启动时，加上 savepoint 的路径；

```shell
./bin/flink run -d -c archieyao.github.io.RecoverFromSavepoint -s file:///tmp/flink-training/savepoint-876926-f66663a98361 /home/archieao/workspace/flink-training/checkpoint/target/checkpoint-1.0-SNAPSHOT.jar
```

## TopologyChange

1. 启动version1，然后停止时触发一次savepoint
    ```shell
    ./bin/flink run -c archieyao.github.io.TopologyChange /opt/flink-1.14.6/examples/custom/checkpoint-1.0-SNAPSHOT.jar
    ./bin/flink cancel -s 33754a3e19c6f654cbf56ad47e22f8f8
    
    # 生成的savepoint路径
    ls /tmp/flink-training/savepoint-33754a-11127cab305c
    ```

2. 启动version2，从version1生成的savepoint启动
   ``` shell
   ./bin/flink run -s /tmp/flink-training/savepoint-33754a-11127cab305c -c archieyao.github.io.TopologyChange /opt/flink-1.14.6/examples/custom/checkpoint-1.0-SNAPSHOT.jar
   
   Caused by: java.util.concurrent.CompletionException: java.lang.IllegalStateException: 
   Failed to rollback to checkpoint/savepoint file:/tmp/flink-training/savepoint-33754a-11127cab305c. 
   Cannot map checkpoint/savepoint state for operator 20ba6b65f97481d5570070de90e4e791 to the new program, 
   because the operator is not available in the new program. If you want to allow to skip this, 
   you can set the --allowNonRestoredState option on the CLI.
   ```
   启动会报错，需要加上 ` --allowNonRestoredState ` 参数：
   ```shell
   ./bin/flink run -s /tmp/flink-training/savepoint-33754a-11127cab305c --allowNonRestoredState -c archieyao.github.io.TopologyChange /opt/flink-1.14.6/examples/custom/checkpoint-1.0-SNAPSHOT.jar
   ```
   作业可以正常启动，但是不能恢复之前的状态。

3. 启动version3，给算子加上 uid
   ```shell
   ./bin/flink run -c archieyao.github.io.TopologyChange /opt/flink-1.14.6/examples/custom/checkpoint-1.0-SNAPSHOT.jar
   ./bin/flink cancel -s 3fb882db1b2842c5073e3c418f4ddfcd
   ls /tmp/flink-training/savepoint-3fb882-020b9cb0a8f8
   ```
   停止时触发savepoint

4. 启动version4，从savepoint恢复
   ```shell
   ./bin/flink run -c archieyao.github.io.TopologyChange -s/tmp/flink-training/savepoint-3fb882-020b9cb0a8f8 --allowNonRestoredState /opt/flink-1.14.6/examples/custom/checkpoint-1.0-SNAPSHOT.jar
   ```
   从结果中，version4 输出的count 是从之前version3版本中累加的结果，并且时间是从state中获取的，说明 sum 算子加上uid之后，可以从快照中恢复。