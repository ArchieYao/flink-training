package archieyao.github.io;

import archieyao.github.io.operator.MapFunc;
import archieyao.github.io.source.SourceFunc;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ArchieYao
 * Created: 2022/3/3 9:11 PM
 * Description:
 */
public class CheckpointFixDelayRestartJob {

    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(CheckpointFixDelayRestartJob.class);

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        // 重启尝试次数 2，每次重启间隔 5 S
        executionEnvironment.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.seconds(5)));
        executionEnvironment.enableCheckpointing(10);

        // source
        DataStreamSource<Tuple3<String, Integer, Long>> source = executionEnvironment.addSource(SourceFunc.getSourceFunc(logger));
        // map operator
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = source.map(MapFunc.getMapFunc(logger));
        // sink
        operator.keyBy(0).sum(1).print();

        executionEnvironment.execute("not-restart");
    }
}
