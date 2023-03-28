package archieyao.github.io;

import archieyao.github.io.operator.MapFunc;
import archieyao.github.io.operator.MapFunctionWithExp;
import archieyao.github.io.operator.StateProcessFunc;
import archieyao.github.io.source.ParallelCheckpointSource;
import archieyao.github.io.source.SourceFunc;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExactlyOnce {

    private static final Logger logger = LoggerFactory.getLogger(ExactlyOnce.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // atMost once
        // atMostOnce(executionEnvironment);

        // at least once
        atLeastOnce(executionEnvironment);

        executionEnvironment.execute("exactly-once");
    }

    private static void atMostOnce(StreamExecutionEnvironment executionEnvironment) {
        executionEnvironment
                .addSource(SourceFunc.getSourceFunc(logger))
                .map(MapFunc.getMapFuncWithExp(logger))
                .keyBy(event -> event.f0)
                .sum(1)
                .print();
    }

    private static void atLeastOnce(StreamExecutionEnvironment executionEnvironment) {
        executionEnvironment.enableCheckpointing(1000);
        executionEnvironment
                .getCheckpointConfig()
                .setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        executionEnvironment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3));

        SingleOutputStreamOperator<Tuple3<String, Long, String>> source1 =
                executionEnvironment
                        .addSource(new ParallelCheckpointSource("source1"))
                        .map(new MapFunctionWithExp(3));

        SingleOutputStreamOperator<Tuple3<String, Long, String>> source2 =
                executionEnvironment
                        .addSource(new ParallelCheckpointSource("source2"))
                        .map(new MapFunctionWithExp(5));

        source1.union(source2).keyBy(event -> event.f0).process(new StateProcessFunc()).print();
    }
}
