package archieyao.github.io;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class WordCountStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        //        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.BATCH);

        executionEnvironment
                .fromElements(
                        "Licensed to the Apache Software Foundation (ASF) under one,"
                                + "               or more contributor license agreements.  See the NOTICE file,"
                                + "                distributed with this work for additional information,"
                                + "                regarding copyright ownership.  The ASF licenses this file,"
                                + "                to you under the Apache License, Version 2.0 (the")
                .flatMap(
                        new FlatMapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public void flatMap(
                                    String value, Collector<Tuple2<String, Integer>> collector)
                                    throws Exception {
                                String[] split = value.split("\\W+");
                                for (String s : split) {
                                    if (s.length() > 0) {
                                        collector.collect(new Tuple2<>(s, 1));
                                        TimeUnit.SECONDS.sleep(1);
                                    }
                                }
                            }
                        })
                .keyBy(event -> event.f0)
                .sum(1)
                .print();
        executionEnvironment.execute();
    }
}
