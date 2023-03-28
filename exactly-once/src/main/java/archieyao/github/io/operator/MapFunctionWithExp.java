package archieyao.github.io.operator;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class MapFunctionWithExp
        extends RichMapFunction<Tuple3<String, Long, String>, Tuple3<String, Long, String>>
        implements CheckpointListener {

    private static final Logger logger = LoggerFactory.getLogger(MapFunctionWithExp.class);

    private long delay;

    private transient volatile boolean needFail = false;

    public MapFunctionWithExp(long delay) {
        this.delay = delay;
    }

    @Override
    public Tuple3<String, Long, String> map(Tuple3<String, Long, String> event) throws Exception {
        TimeUnit.SECONDS.sleep(delay);
        if (needFail) {
            throw new RuntimeException("Error from MapFunctionWithExp");
        }
        return event;
    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {
        this.needFail = true;
        logger.error("checkpoint finished cost {}", l);
    }
}
