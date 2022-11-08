package archieyao.github.io.sink;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class RandomOutputProcessFunc<T> extends ProcessFunction<T,T> {

    private OutputTag<T> outputTag;

    


    @Override
    public void processElement(T t, ProcessFunction<T, T>.Context context, Collector<T> collector) throws Exception {

    }
}
