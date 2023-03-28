package archieyao.github.io.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.UserCodeClassLoader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

/** @author ArchieYao create at 2022/4/10 3:15 PM */
public class SocketSourceFunction extends RichSourceFunction<RowData>
        implements ResultTypeQueryable<RowData> {

    private final String hostname;
    private final int port;
    private final byte byteDelimiter;
    private final DeserializationSchema<RowData> deserializer;

    private volatile boolean isRunning = true;
    private Socket currentSocket;

    public SocketSourceFunction(
            String hostname,
            int port,
            byte byteDelimiter,
            DeserializationSchema<RowData> deserializer) {
        this.hostname = hostname;
        this.port = port;
        this.byteDelimiter = byteDelimiter;
        this.deserializer = deserializer;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return deserializer.getProducedType();
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        while (isRunning) {
            try (final Socket socket = new Socket()) {
                currentSocket = socket;
                socket.connect(new InetSocketAddress(hostname, port), 10);
                try (InputStream inputStream = socket.getInputStream()) {
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    int b;
                    while ((b = inputStream.read()) >= 0) {
                        if (b != byteDelimiter) {
                            buffer.write(b);
                        } else {
                            ctx.collect(deserializer.deserialize(buffer.toByteArray()));
                            buffer.close();
                        }
                    }
                }
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        try {
            currentSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        deserializer.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return getRuntimeContext().getMetricGroup();
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return (UserCodeClassLoader) getRuntimeContext().getUserCodeClassLoader();
                    }
                });
    }
}
