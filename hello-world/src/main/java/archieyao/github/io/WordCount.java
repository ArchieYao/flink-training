package archieyao.github.io;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @author ArchieYao
 * Created: 2022/2/22 5:19 PM
 * Description:
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建Flink任务运行环境
        final ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        // 创建DataSet，数据是一行一行文本
        DataSource<String> text = executionEnvironment.fromElements(
                "Licensed to the Apache Software Foundation (ASF) under one",
                "or more contributor license agreements.  See the NOTICE file",
                "distributed with this work for additional information",
                "regarding copyright ownership.  The ASF licenses this file",
                "to you under the Apache License, Version 2.0 (the"
        );

        // 通过Flink内置转换函数进行计算
        AggregateOperator<Tuple2<String, Integer>> sum = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = value.split("\\W+");
                for (String s : split) {
                    if (s.length() > 0) {
                        collector.collect(new Tuple2<>(s, 1));
//                        TimeUnit.SECONDS.sleep(5);
                    }
                }
            }
        }).groupBy(0).sum(1);

        // 打印结果
        sum.print();
    }
}
