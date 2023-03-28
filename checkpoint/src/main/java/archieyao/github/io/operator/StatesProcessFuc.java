package archieyao.github.io.operator;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatesProcessFuc
        extends KeyedProcessFunction<
                String, Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatesProcessFuc.class);

    private transient ValueState<Long> indexState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        indexState =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<Long>("indexState", Long.class));
    }

    //    public void processElement(Tuple3<String, Integer, Long> event,
    //                               KeyedProcessFunction<Tuple, Tuple3<String, Integer, Long>,
    //                                       Tuple3<String, Integer, Long>>.Context context,
    //                               Collector<Tuple3<String, Integer, Long>> collector) throws
    // Exception {
    //
    //
    //    }

    @Override
    public void processElement(
            Tuple3<String, Integer, Long> event,
            KeyedProcessFunction<
                                    String,
                                    Tuple3<String, Integer, Long>,
                                    Tuple3<String, Integer, Long>>
                            .Context
                    context,
            Collector<Tuple3<String, Integer, Long>> collector)
            throws Exception {
        Long currentVal = indexState.value();
        if (null == currentVal) {
            LOGGER.warn("Initialize when first run or failover");
            currentVal = 0L;
        }
        indexState.update(currentVal + 1);
        event.f2 = currentVal;
        collector.collect(event);
    }
}
