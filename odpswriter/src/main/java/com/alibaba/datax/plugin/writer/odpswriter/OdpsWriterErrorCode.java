package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.util.MessageSource;

public enum OdpsWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("OdpsWriter-00", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.required_value")),
    ILLEGAL_VALUE("OdpsWriter-01", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.illegal_value")),
    UNSUPPORTED_COLUMN_TYPE("OdpsWriter-02", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.unsupported_column_type")),

    TABLE_TRUNCATE_ERROR("OdpsWriter-03", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.table_truncate_error")),
    CREATE_MASTER_UPLOAD_FAIL("OdpsWriter-04", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.create_master_upload_fail")),
    GET_SLAVE_UPLOAD_FAIL("OdpsWriter-05", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.get_slave_upload_fail")),
    GET_ID_KEY_FAIL("OdpsWriter-06", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.get_id_key_fail")),
    GET_PARTITION_FAIL("OdpsWriter-07", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.get_partition_fail")),

    ADD_PARTITION_FAILED("OdpsWriter-08", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.add_partition_failed")),
    WRITER_RECORD_FAIL("OdpsWriter-09", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.writer_record_fail")),

    COMMIT_BLOCK_FAIL("OdpsWriter-10", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.commit_block_fail")),
    RUN_SQL_FAILED("OdpsWriter-11", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.run_sql_failed")),
    CHECK_IF_PARTITIONED_TABLE_FAILED("OdpsWriter-12", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.check_if_partitioned_table_failed")),

    RUN_SQL_ODPS_EXCEPTION("OdpsWriter-13", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.run_sql_odps_exception")),

    ACCOUNT_TYPE_ERROR("OdpsWriter-30", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.account_type_error")),

    PARTITION_ERROR("OdpsWriter-31", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.partition_error")),

    COLUMN_NOT_EXIST("OdpsWriter-32", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.column_not_exist")),

    ODPS_PROJECT_NOT_FOUNT("OdpsWriter-100", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.odps_project_not_fount")),  //ODPS-0420111: Project not found

    ODPS_TABLE_NOT_FOUNT("OdpsWriter-101", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.odps_table_not_fount")), // ODPS-0130131:Table not found

    ODPS_ACCESS_KEY_ID_NOT_FOUND("OdpsWriter-102", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.odps_access_key_id_not_found")), //ODPS-0410051:Invalid credentials - accessKeyId not found

    ODPS_ACCESS_KEY_INVALID("OdpsWriter-103", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.odps_access_key_invalid")), //ODPS-0410042:Invalid signature value - User signature dose not match;

    ODPS_ACCESS_DENY("OdpsWriter-104", MessageSource.loadResourceBundle(OdpsWriterErrorCode.class).message("errorcode.odps_access_deny")) //ODPS-0420095: Access Denied - Authorization Failed [4002], You doesn't exist in project

    ;

    private final String code;
    private final String description;

    private OdpsWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
