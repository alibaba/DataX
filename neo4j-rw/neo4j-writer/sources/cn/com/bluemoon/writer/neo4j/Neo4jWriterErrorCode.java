package cn.com.bluemoon.writer.neo4j;

import com.alibaba.datax.common.spi.ErrorCode;

/* loaded from: neo4jWriter-1.0-SNAPSHOT.jar:cn/com/bluemoon/writer/neo4j/Neo4jWriterErrorCode.class */
public enum Neo4jWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("Neo4jWriter-00", "您缺失了必须填写的参数值."),
    CONF_ERROR("WriterErrCode-00", "您的配置错误."),
    WRITE_DATA_ERROR("DBUtilErrorCode-05", "往您配置的写入表中写入数据时失败.");
    
    private final String code;
    private final String description;

    Neo4jWriterErrorCode(String code, String description) {
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
