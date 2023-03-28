package archieyao.github.io.source;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

public class SourceFunc2 {
    public static SourceFunction<Tuple3<String, Integer, Long>> getSourceFunc(Logger logger) {
        return new SourceFunction<Tuple3<String, Integer, Long>>() {
            @Override
            public void run(SourceContext<Tuple3<String, Integer, Long>> sourceContext)
                    throws Exception {
                int index = 1;
                while (true) {
                    sourceContext.collect(new Tuple3<>("key", index++, System.currentTimeMillis()));
                    sourceContext.collect(
                            new Tuple3<>("key1", index++, System.currentTimeMillis()));
                    TimeUnit.MILLISECONDS.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                //                logger.warn("source func cancel.");
            }
        };
    }
}
