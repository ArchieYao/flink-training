package archieyao.github.io.operator;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class StateProcessFunc
        extends KeyedProcessFunction<
                String, Tuple3<String, Long, String>, Tuple4<String, Long, String, Long>> {

    //    private transient ListState<Tuple3<String,Long,String>>

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void processElement(
            Tuple3<String, Long, String> event,
            KeyedProcessFunction<
                                    String,
                                    Tuple3<String, Long, String>,
                                    Tuple4<String, Long, String, Long>>
                            .Context
                    context,
            Collector<Tuple4<String, Long, String, Long>> collector)
            throws Exception {
        collector.collect(new Tuple4<>(event.f0, event.f1, event.f2, System.currentTimeMillis()));
    }
}
