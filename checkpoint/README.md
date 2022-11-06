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