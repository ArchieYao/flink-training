package archieyao.github.io;

import archieyao.github.io.operator.MapFunc;
import archieyao.github.io.source.SourceFunc;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoverFromCheckpoint {

    /**
     * mvn clean package -Dmaven.test.skip=true
     * 编译时需要设置 maven 依赖的 scope 为 provide
     * ./bin/flink run -c archieyao.github.io.RecoverFromCheckpoint /home/archieao/workspace/flink-training/checkpoint/target/checkpoint-1.0-SNAPSHOT.jar
     * ./bin/flink run -s /tmp/flink-training/3a16d77da5dbdc952541a7a89b60546d/chk-1799  -c archieyao.github.io.RecoverFromCheckpoint /home/archieao/workspace/flink-training/checkpoint/target/checkpoint-1.0-SNAPSHOT.jar
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(RecoverFromCheckpoint.class);

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        executionEnvironment.enableCheckpointing(20);
//        // 关闭重启
//        executionEnvironment.setRestartStrategy(RestartStrategies.noRestart());

//        executionEnvironment.setStateBackend(new HashMapStateBackend());
//        executionEnvironment.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-training");

        // 保留Checkpoint
        executionEnvironment.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // source
        DataStreamSource<Tuple3<String, Integer, Long>> source = executionEnvironment.addSource(SourceFunc.getSourceFunc(logger));
        // map operator
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = source.map(MapFunc.getMapFuncWithExp(logger));
        // sink
        // operator.keyBy(0).sum(1).print();
        operator.keyBy(v -> v.f0).sum(1).print();
        executionEnvironment.execute("RecoverFromCheckpoint");
    }
}
