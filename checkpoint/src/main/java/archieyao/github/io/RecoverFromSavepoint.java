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

public class RecoverFromSavepoint {

    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(RecoverFromSavepoint.class);

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.enableCheckpointing(1000);
        // 保留Checkpoint
        executionEnvironment.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // source
        DataStreamSource<Tuple3<String, Integer, Long>> source = executionEnvironment.addSource(SourceFunc.getSourceFunc(logger));
        // map operator
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = source.map(MapFunc.getMapFunc(logger));
        // sink
        // operator.keyBy(0).sum(1).print();
        operator.keyBy(v -> v.f0).sum(1).print();
        executionEnvironment.execute("RecoverFromSavepoint");
    }

}
