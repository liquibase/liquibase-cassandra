package liquibase.ext.cassandra.datatype;

import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.core.CharType;

@DataTypeInfo(
        name = "text",
        aliases = {"java.sql.Types.VARCHAR", "java.lang.String", "text", "cassandra text"},
        minParameters = 0,
        maxParameters = 1,
        priority = 2
)

public class TextDataType extends CharType {
}
