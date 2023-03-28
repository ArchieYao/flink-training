package archieyao.github.io.sideoutput;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputProcess extends ProcessFunction<String, Tuple2<String, Integer>> {

    public static final OutputTag<String> largeLenWordsTag = new OutputTag<String>("lager-len") {};

    @Override
    public void processElement(
            String event, Context context, Collector<Tuple2<String, Integer>> collector)
            throws Exception {

        String[] tokens = event.split("\\W+");
        for (String token : tokens) {
            if (token.length() > 5) {
                context.output(largeLenWordsTag, token);
            } else if (token.length() > 0) {
                collector.collect(new Tuple2<>(token, 1));
            }
        }
    }
}
