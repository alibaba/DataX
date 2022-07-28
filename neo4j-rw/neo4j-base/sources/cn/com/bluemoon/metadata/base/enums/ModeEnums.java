package cn.com.bluemoon.metadata.base.enums;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/enums/ModeEnums.class */
public enum ModeEnums {
    COMPARE("COMPARE"),
    MERGE("MERGE"),
    CREATE("CREATE");
    
    private String code;

    public String getCode() {
        return this.code;
    }

    ModeEnums(String code) {
        this.code = code;
    }
}
