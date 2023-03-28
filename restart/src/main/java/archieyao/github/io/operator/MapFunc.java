package archieyao.github.io.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;

/** @author ArchieYao Created: 2022/3/1 9:04 PM Description: */
public class MapFunc {

    public static MapFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>> getMapFunc(
            Logger logger) {
        return new MapFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple3<String, Integer, Long> event)
                    throws Exception {
                if (event.f1 % 100 == 0) {
                    logger.error("event.f1 more than 100. value : " + event.f1);
                    throw new RuntimeException("event.f1 more than 100.");
                }
                return new Tuple2<>(event.f0, event.f1);
            }
        };
    }
}
