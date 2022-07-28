package cn.com.bluemoon.reader.neo4j;

import com.alibaba.datax.common.spi.ErrorCode;

/* loaded from: neo4jReader-1.0-SNAPSHOT.jar:cn/com/bluemoon/reader/neo4j/Neo4jReaderErrorCode.class */
public enum Neo4jReaderErrorCode implements ErrorCode {
    REQUIRED_VALUE("Neo4jReader-00", "暂时不支持该数据类型，请联系开发人员.");
    
    private final String code;
    private final String description;

    Neo4jReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public String getCode() {
        return this.code;
    }

    public String getDescription() {
        return this.description;
    }

    @Override // java.lang.Enum, java.lang.Object
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code, this.description);
    }
}
