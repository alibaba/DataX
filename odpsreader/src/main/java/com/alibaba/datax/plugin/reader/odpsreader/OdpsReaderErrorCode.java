package com.alibaba.datax.plugin.reader.odpsreader;

import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.util.MessageSource;

public enum OdpsReaderErrorCode implements ErrorCode {
    REQUIRED_VALUE("DATAX_R_ODPS_001", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_001"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_001")),
    ILLEGAL_VALUE("DATAX_R_ODPS_002", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_002"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_002")),
    CREATE_DOWNLOADSESSION_FAIL("DATAX_R_ODPS_003", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_003"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_003")),
    GET_DOWNLOADSESSION_FAIL("DATAX_R_ODPS_004", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_004"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_004")),
    READ_DATA_FAIL("DATAX_R_ODPS_005", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_005"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_005")),
    GET_ID_KEY_FAIL("DATAX_R_ODPS_006", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_006"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_006")),

    ODPS_READ_EXCEPTION("DATAX_R_ODPS_007", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_007"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_007")),
    OPEN_RECORD_READER_FAILED("DATAX_R_ODPS_008", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_008"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_008")),

    ODPS_PROJECT_NOT_FOUNT("DATAX_R_ODPS_009", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_009"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_009")),  //ODPS-0420111: Project not found

    ODPS_TABLE_NOT_FOUNT("DATAX_R_ODPS_010", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_010"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_010")), // ODPS-0130131:Table not found

    ODPS_ACCESS_KEY_ID_NOT_FOUND("DATAX_R_ODPS_011", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_011"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_011")), //ODPS-0410051:Invalid credentials - accessKeyId not found

    ODPS_ACCESS_KEY_INVALID("DATAX_R_ODPS_012", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_012"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_012")), //ODPS-0410042:Invalid signature value - User signature dose not match

    ODPS_ACCESS_DENY("DATAX_R_ODPS_013", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_013"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_013")), //ODPS-0420095: Access Denied - Authorization Failed [4002], You doesn't exist in project



    SPLIT_MODE_ERROR("DATAX_R_ODPS_014", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_014"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_014")),

    ACCOUNT_TYPE_ERROR("DATAX_R_ODPS_015", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_015"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_015")),

    VIRTUAL_VIEW_NOT_SUPPORT("DATAX_R_ODPS_016", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_016"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_016")),

    PARTITION_ERROR("DATAX_R_ODPS_017", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_017"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_017")),

    PARTITION_NOT_EXISTS_ERROR("DATAX_R_ODPS_018", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_018"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_018")),

    RUN_SQL_FAILED("DATAX_R_ODPS_019", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_019"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_019")),

    RUN_SQL_ODPS_EXCEPTION("DATAX_R_ODPS_020", MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("description.DATAX_R_ODPS_020"),MessageSource.loadResourceBundle(OdpsReaderErrorCode.class).message("solution.DATAX_R_ODPS_020")),
    ;
    private final String code;
    private final String description;
    private final String solution;

    private OdpsReaderErrorCode(String code, String description,String solution) {
        this.code = code;
        this.description = description;
        this.solution = solution;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    public String getSolution() {
        return solution;
    }

    @Override
    public String toString() {
        return String.format("Code:%s:%s, Solution:[%s]. ", this.code,this.description,this.solution);
    }
}
