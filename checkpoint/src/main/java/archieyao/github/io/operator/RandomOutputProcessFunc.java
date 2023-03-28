package archieyao.github.io.operator;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class RandomOutputProcessFunc<T> extends ProcessFunction<T, T> {

    private static final Logger logger = LoggerFactory.getLogger(RandomOutputProcessFunc.class);
    private final OutputTag<T> outputTag;

    public RandomOutputProcessFunc(OutputTag<T> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void processElement(
            T event, ProcessFunction<T, T>.Context context, Collector<T> collector)
            throws Exception {
        collector.collect(event);
        if (new Random().nextInt() % 5 == 0) {
            logger.warn("RandomOutputProcessFunc output {}", event);
            context.output(outputTag, event);
        }
    }
}
