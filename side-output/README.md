# side-output

flink 边路输出，可以对数据进行分流

1. 自定义process function  `ProcessFunction`
2. 实现 `processElement` 方法，对word 进行split；
3. `processElement` 方法 split word 之后，对数据分流；length >5 → side output ; 0 < length ≤5 → collector normal output
4. `main` 方法中， 数据流先进行 自定义 prcess 算子，然后正常进行 keyby sum 算子计算；
5. 对于 process 算子计算结果，获取 side output ，side output 还可以进行其他算子计算；
6. 最后打印 normal output 和 side output

```java
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputProcess extends ProcessFunction<String, Tuple2<String, Integer>> {

    // putput tag 用来唯一标注一个side output
    public static final OutputTag<String> largeLenWordsTag = new OutputTag<String>("lager-len") {
    };

    @Override
    public void processElement(String event,
                               Context context,
                               Collector<Tuple2<String, Integer>> collector) throws Exception {

        String[] tokens = event.split("\\W+");
        for (String token : tokens) {
            if (token.length() > 5) { // word 长度大于5的方法，放到context的output里，并且标注一个 output tag
                context.output(largeLenWordsTag, token);
            } else if (token.length() > 0) {
                collector.collect(new Tuple2<>(token, 1));
            }
        }
    }
}


    // main 
    // 创建Flink任务运行环境
    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        DataStreamSource<String> streamSource = streamEnv.fromElements(
                "Licensed to the Apache Software Foundation (ASF) under one",
                "or more contributor license agreements.  See the NOTICE file",
                "distributed with this work for additional information",
                "regarding copyright ownership.  The ASF licenses this file",
                "to you under the Apache License, Version 2.0 (the");
        // 调用自定义的process方法
        SingleOutputStreamOperator<Tuple2<String, Integer>> process = streamSource
                .process(new SideOutputProcess());

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = process
                .keyBy(event -> event.f0)
                .sum(1);
        streamOperator.print();

        // side output 通过 output tag 获取 side output ，此时的 side output 没有参与 keyBy sum 等算子的计算
        DataStream<String> sideOutput = process.getSideOutput(SideOutputProcess.largeLenWordsTag);
        sideOutput.map(value -> "side output: " + value).print();
        streamEnv.execute("version2");
    }
```