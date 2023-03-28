package archieyao.github.io.runtime;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/** @author ArchieYao create at 2022/4/10 3:54 PM */
public class ChangelogCsvDeserializer implements DeserializationSchema<RowData> {

    private final List<LogicalType> parseTypes;
    private final DynamicTableSource.DataStructureConverter converter;
    private final TypeInformation<RowData> dataTypeInformation;
    private final String columnDelimiter;

    public ChangelogCsvDeserializer(
            List<LogicalType> parseTypes,
            DynamicTableSource.DataStructureConverter converter,
            TypeInformation<RowData> dataTypeInformation,
            String columnDelimiter) {
        this.parseTypes = parseTypes;
        this.converter = converter;
        this.dataTypeInformation = dataTypeInformation;
        this.columnDelimiter = columnDelimiter;
    }

    @Override
    public void open(InitializationContext context) {
        converter.open(
                RuntimeConverter.Context.create(ChangelogCsvDeserializer.class.getClassLoader()));
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        final String[] columns = new String(message).split(Pattern.quote(columnDelimiter));
        final RowKind rowKind = RowKind.valueOf(columns[0]);
        final Row row = new Row(rowKind, parseTypes.size());
        for (int i = 0; i < parseTypes.size(); i++) {
            row.setField(i, parse(parseTypes.get(i).getTypeRoot(), columns[i + 1]));
        }
        return (RowData) converter.toInternal(row);
    }

    private static Object parse(LogicalTypeRoot root, String value) {
        switch (root) {
            case INTEGER:
                return Integer.parseInt(value);
            case VARCHAR:
                return value;
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return dataTypeInformation;
    }
}
