package archieyao.github.io.operator;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ArchieYao
 * Created: 2022/3/1 9:04 PM
 * Description:
 */
public class MapFunc extends RichMapFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapFunc.class);
    private transient int indexOfSubTask;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        indexOfSubTask = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public Tuple3<String, Long, Long> map(Tuple3<String, Long, Long> event) throws Exception {
        if (event.f1 % 10 == 0) {
            LOGGER.error("event.f1 %10 =0");
            throw new RuntimeException("event.f1 %10 =0");
        }
        return new Tuple3<>(event.f0, event.f1, System.currentTimeMillis());
    }
}
