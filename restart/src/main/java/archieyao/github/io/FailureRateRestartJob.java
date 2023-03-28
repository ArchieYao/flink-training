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

/** @author ArchieYao Created: 2022/3/2 11:22 AM Description: */
public class FailureRateRestartJob {

    public static void main(String[] args) throws Exception {

        Logger logger = LoggerFactory.getLogger(FailureRateRestartJob.class);

        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        // 如果20 S 内，有1次错误，则终止作业，不满足则会重启作业，重启时间间隔为 5 S
        executionEnvironment.setRestartStrategy(
                RestartStrategies.failureRateRestart(1, Time.seconds(20), Time.seconds(5)));

        // source
        DataStreamSource<Tuple3<String, Integer, Long>> source =
                executionEnvironment.addSource(SourceFunc.getSourceFunc(logger));
        // map operator
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator =
                source.map(MapFunc.getMapFunc(logger));
        // sink
        operator.print();

        executionEnvironment.execute("not-restart");
    }
}
