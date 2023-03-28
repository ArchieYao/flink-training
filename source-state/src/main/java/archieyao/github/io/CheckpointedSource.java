package archieyao.github.io;

import archieyao.github.io.operator.MapFunc;
import archieyao.github.io.source.NoParallelismSourceFunc;
import archieyao.github.io.source.ParallelismRestoreFromTaskIndexSourceFunc;
import archieyao.github.io.source.ParallelismSourceFunc;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CheckpointedSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(3, Time.seconds(2)));
        executionEnvironment.enableCheckpointing(1000);
        // 并发为1
        //        noParallelism(executionEnvironment);
        //        parallelism(executionEnvironment);
        parallelismRestoreFromTask(executionEnvironment);
        executionEnvironment.execute("CheckpointedSource");
    }

    public static void noParallelism(StreamExecutionEnvironment executionEnvironment) {
        executionEnvironment.setParallelism(1);
        executionEnvironment
                .addSource(new NoParallelismSourceFunc())
                .map(new MapFunc())
                .keyBy(event -> event.f0)
                .sum(1)
                .print();
    }

    public static void parallelism(StreamExecutionEnvironment executionEnvironment) {
        executionEnvironment.setParallelism(2);
        executionEnvironment
                .addSource(new ParallelismSourceFunc())
                .map(new MapFunc())
                .keyBy(event -> event.f0)
                .sum(1)
                .print();
    }

    public static void parallelismRestoreFromTask(StreamExecutionEnvironment executionEnvironment) {
        executionEnvironment.setParallelism(2);
        executionEnvironment
                .addSource(new ParallelismRestoreFromTaskIndexSourceFunc())
                .map(new MapFunc())
                .keyBy(event -> event.f0)
                .sum(1)
                .print();
    }
}
