package archieyao.github.io.sideoutput;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SideOutputExample {
    public static void main(String[] args) throws Exception {
        //        version0();
        //        version1();
        version2();
    }

    public static void version0() throws Exception {
        // 创建Flink任务运行环境
        final ExecutionEnvironment executionEnvironment =
                ExecutionEnvironment.getExecutionEnvironment();

        // 创建DataSet，数据是一行一行文本
        DataSource<String> text =
                executionEnvironment.fromElements(
                        "Licensed to the Apache Software Foundation (ASF) under one",
                        "or more contributor license agreements.  See the NOTICE file",
                        "distributed with this work for additional information",
                        "regarding copyright ownership.  The ASF licenses this file",
                        "to you under the Apache License, Version 2.0 (the");

        // 通过Flink内置转换函数进行计算
        AggregateOperator<Tuple2<String, Integer>> sum =
                text.flatMap(
                                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                                    @Override
                                    public void flatMap(
                                            String value,
                                            Collector<Tuple2<String, Integer>> collector)
                                            throws Exception {
                                        String[] split = value.split("\\W+");
                                        for (String s : split) {
                                            if (s.length() > 0) {
                                                collector.collect(new Tuple2<>(s, 1));
                                            }
                                        }
                                    }
                                })
                        .groupBy(0)
                        .sum(1);

        // 打印结果
        sum.print();
    }

    public static void version1() throws Exception {
        // 创建Flink任务运行环境
        final ExecutionEnvironment executionEnvironment =
                ExecutionEnvironment.getExecutionEnvironment();
        // 创建DataSet，数据是一行一行文本
        DataSource<String> text =
                executionEnvironment.fromElements(
                        "Licensed to the Apache Software Foundation (ASF) under one",
                        "or more contributor license agreements.  See the NOTICE file",
                        "distributed with this work for additional information",
                        "regarding copyright ownership.  The ASF licenses this file",
                        "to you under the Apache License, Version 2.0 (the");

        // 通过Flink内置转换函数进行计算
        AggregateOperator<Tuple2<String, Integer>> sum =
                text.flatMap(
                                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                                    @Override
                                    public void flatMap(
                                            String value,
                                            Collector<Tuple2<String, Integer>> collector)
                                            throws Exception {
                                        String[] split = value.split("\\W+");
                                        for (String s : split) {
                                            if (s.length() > 0) {
                                                collector.collect(new Tuple2<>(s, 1));
                                            }
                                        }
                                    }
                                })
                        .groupBy(0)
                        .sum(1);

        // 打印结果
        sum.print();

        // 通过Flink内置转换函数进行计算
        FlatMapOperator<String, Tuple2<String, Integer>> map =
                text.flatMap(
                        new FlatMapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public void flatMap(
                                    String value, Collector<Tuple2<String, Integer>> collector)
                                    throws Exception {
                                String[] split = value.split("\\W+");
                                for (String s : split) {
                                    if (s.length() > 5) {
                                        collector.collect(new Tuple2<>(s, 1));
                                    }
                                }
                            }
                        });

        FilterOperator<String> filteredText = text.filter(event -> event.length() > 5);
        filteredText.print();

        // 打印结果
        //        map.print();
    }

    public static void version2() throws Exception {
        // 创建Flink任务运行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        DataStreamSource<String> streamSource =
                streamEnv.fromElements(
                        "Licensed to the Apache Software Foundation (ASF) under one",
                        "or more contributor license agreements.  See the NOTICE file",
                        "distributed with this work for additional information",
                        "regarding copyright ownership.  The ASF licenses this file",
                        "to you under the Apache License, Version 2.0 (the");
        SingleOutputStreamOperator<Tuple2<String, Integer>> process =
                streamSource.process(new SideOutputProcess());

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator =
                process.keyBy(event -> event.f0).sum(1);
        streamOperator.print();

        // side output
        DataStream<String> sideOutput = process.getSideOutput(SideOutputProcess.largeLenWordsTag);
        sideOutput.map(value -> "side output: " + value).print();
        streamEnv.execute("version2");
    }
}
