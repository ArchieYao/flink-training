package archieyao.github.io.source;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

/** @author ArchieYao Created: 2022/3/1 8:59 PM Description: */
public class SourceFunc {

    public static SourceFunction<Tuple3<String, Integer, Long>> getSourceFunc(Logger logger) {
        return new SourceFunction<Tuple3<String, Integer, Long>>() {
            @Override
            public void run(SourceContext<Tuple3<String, Integer, Long>> sourceContext)
                    throws Exception {
                int index = 1;
                while (true) {
                    sourceContext.collect(new Tuple3<>("key", index++, System.currentTimeMillis()));
                    TimeUnit.MILLISECONDS.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                //                logger.warn("source func cancel.");
            }
        };
    }

    public static void main(String[] args) {
        String[] strings =
                StringUtils.splitByWholeSeparator(
                        "/webApp/auctionHouse/saleList/P20221223A2N90MWVAC", "/");
        for (String string : strings) {
            System.out.println(string);
        }
    }
}
