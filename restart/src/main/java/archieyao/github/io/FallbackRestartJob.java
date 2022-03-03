package archieyao.github.io;

import archieyao.github.io.operator.MapFunc;
import archieyao.github.io.source.SourceFunc;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ArchieYao
 * Created: 2022/3/2 11:37 AM
 * Description:
 */
public class FallbackRestartJob {

    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(NoRestartJob.class);

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        // 采用集群的重启策略，如果没有定义其他重启策略，默认选择固定延时重启策略。
        executionEnvironment.setRestartStrategy(RestartStrategies.fallBackRestart());

        // source
        DataStreamSource<Tuple3<String, Integer, Long>> source = executionEnvironment.addSource(SourceFunc.getSourceFunc(logger));
        // map operator
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = source.map(MapFunc.getMapFunc(logger));
        // sink
        operator.print();

        executionEnvironment.execute("not-restart");
    }
}
