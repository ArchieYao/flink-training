package archieyao.github.io.runtime;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.util.List;


/**
 * @author ArchieYao create at 2022/4/10 3:42 PM
 */
public class ChangelogCsvFormat implements DecodingFormat<DeserializationSchema<RowData>> {

    private final String columnDelimiter;

    public ChangelogCsvFormat(String columnDelimiter) {
        this.columnDelimiter = columnDelimiter;
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType dataType) {
        final TypeInformation<RowData> producedTypeInfo = context.createTypeInformation(dataType);
        final DataStructureConverter converter = context.createDataStructureConverter(dataType);
        final List<LogicalType> parseTypes = dataType.getLogicalType().getChildren();
        return new ChangelogCsvDeserializer(parseTypes, converter, producedTypeInfo, columnDelimiter);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .build();
    }
}
