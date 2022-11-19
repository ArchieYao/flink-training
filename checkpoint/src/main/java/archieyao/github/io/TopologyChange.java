package archieyao.github.io;

import archieyao.github.io.operator.RandomOutputProcessFunc;
import archieyao.github.io.operator.StatesProcessFuc;
import archieyao.github.io.source.SourceFunc;
import archieyao.github.io.source.SourceFunc2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

public class TopologyChange {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.enableCheckpointing(1000);
        executionEnvironment.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

//        version1(executionEnvironment);
//        version2(executionEnvironment);
//        version3(executionEnvironment);
        version4(executionEnvironment);
        String executionPlan = executionEnvironment.getExecutionPlan();
        System.out.println(executionPlan);
        executionEnvironment.execute("TopologyChange");
    }

    public static void version1(StreamExecutionEnvironment executionEnvironment) throws Exception {
        final OutputTag<Tuple3<String, Integer, Long>> outputTag = new OutputTag
                <Tuple3<String, Integer, Long>>("side-output") {
        };
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> process =
                executionEnvironment.addSource(SourceFunc.getSourceFunc(null))
                        .keyBy(event -> event.f0)
                        .sum(1)
                        .process(new RandomOutputProcessFunc<>(outputTag));
        process.print().name("main-result-output");
        // side output
        process.getSideOutput(outputTag).map((MapFunction<Tuple3<String, Integer, Long>, Object>)
                stringIntegerLongTuple3 -> {
                    stringIntegerLongTuple3.f0 = "side output " + stringIntegerLongTuple3.f0;
                    return stringIntegerLongTuple3;
                }).print().name("random-result-output");
    }

    public static void version2(StreamExecutionEnvironment executionEnvironment) throws Exception {
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> result =
                executionEnvironment.addSource(SourceFunc2.getSourceFunc(null))
                        .keyBy(event -> event.f0)
                        .process(new StatesProcessFuc())
                        .keyBy(event -> event.f0)
                        .sum(1);
        result.print().name("main-result-version2");
        String executionPlan = executionEnvironment.getExecutionPlan();
        System.out.println(executionPlan);
        executionEnvironment.execute("TopologyChange-version2");
    }

    public static void version3(StreamExecutionEnvironment executionEnvironment) throws Exception {
        final OutputTag<Tuple3<String, Integer, Long>> outputTag = new OutputTag
                <Tuple3<String, Integer, Long>>("side-output") {
        };
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> process =
                executionEnvironment.addSource(SourceFunc.getSourceFunc(null))
                        .keyBy(event -> event.f0)
                        .sum(1).uid("topologyChange_sum")
                        .process(new RandomOutputProcessFunc<>(outputTag));
        process.print().name("main-result-output");
        // side output
        process.getSideOutput(outputTag).map((MapFunction<Tuple3<String, Integer, Long>, Object>)
                stringIntegerLongTuple3 -> {
                    stringIntegerLongTuple3.f0 = "side output " + stringIntegerLongTuple3.f0;
                    return stringIntegerLongTuple3;
                }).print().name("random-result-output");
    }


    public static void version4(StreamExecutionEnvironment executionEnvironment) throws Exception {
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> result =
                executionEnvironment.addSource(SourceFunc2.getSourceFunc(null))
                        .keyBy(event -> event.f0)
                        .process(new StatesProcessFuc())
                        .keyBy(event -> event.f0)
                        .sum(1).uid("topologyChange_sum");
        result.print().name("main-result-version2");
        String executionPlan = executionEnvironment.getExecutionPlan();
        System.out.println(executionPlan);
        executionEnvironment.execute("TopologyChange-version2");
    }
}
