package cn.com.bluemoon.metadata.base.dto;

import java.util.List;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/dto/DbMetaDataBaseSyncParam.class */
public abstract class DbMetaDataBaseSyncParam {
    private String type;
    private String name;
    private String sourceQuerySql;
    private String sourcePassword;
    private String sourceUsername;
    private String sourceUrl;
    private String targetUrl;
    private String targetUsername;
    private String targetPassword;
    private List<String> columns;
    private Integer seq;
    private String workName;
    private String collectTime = "";
    private Integer incrementMode;
    private String readerName;
    private String importName;
    private String envCn;
    private String sourceLabel;

    public String toString() {
        return "DbMetaDataBaseSyncParam(type=" + getType() + ", name=" + getName() + ", sourceQuerySql=" + getSourceQuerySql() + ", sourcePassword=" + getSourcePassword() + ", sourceUsername=" + getSourceUsername() + ", sourceUrl=" + getSourceUrl() + ", targetUrl=" + getTargetUrl() + ", targetUsername=" + getTargetUsername() + ", targetPassword=" + getTargetPassword() + ", columns=" + getColumns() + ", seq=" + getSeq() + ", workName=" + getWorkName() + ", collectTime=" + getCollectTime() + ", incrementMode=" + getIncrementMode() + ", readerName=" + getReaderName() + ", importName=" + getImportName() + ", envCn=" + getEnvCn() + ", sourceLabel=" + getSourceLabel() + ")";
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSourceQuerySql(String sourceQuerySql) {
        this.sourceQuerySql = sourceQuerySql;
    }

    public void setSourcePassword(String sourcePassword) {
        this.sourcePassword = sourcePassword;
    }

    public void setSourceUsername(String sourceUsername) {
        this.sourceUsername = sourceUsername;
    }

    public void setSourceUrl(String sourceUrl) {
        this.sourceUrl = sourceUrl;
    }

    public void setTargetUrl(String targetUrl) {
        this.targetUrl = targetUrl;
    }

    public void setTargetUsername(String targetUsername) {
        this.targetUsername = targetUsername;
    }

    public void setTargetPassword(String targetPassword) {
        this.targetPassword = targetPassword;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public void setSeq(Integer seq) {
        this.seq = seq;
    }

    public void setWorkName(String workName) {
        this.workName = workName;
    }

    public void setCollectTime(String collectTime) {
        this.collectTime = collectTime;
    }

    public void setIncrementMode(Integer incrementMode) {
        this.incrementMode = incrementMode;
    }

    public void setReaderName(String readerName) {
        this.readerName = readerName;
    }

    public void setImportName(String importName) {
        this.importName = importName;
    }

    public void setEnvCn(String envCn) {
        this.envCn = envCn;
    }

    public void setSourceLabel(String sourceLabel) {
        this.sourceLabel = sourceLabel;
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof DbMetaDataBaseSyncParam)) {
            return false;
        }
        DbMetaDataBaseSyncParam other = (DbMetaDataBaseSyncParam) o;
        if (!other.canEqual(this)) {
            return false;
        }
        Object this$type = getType();
        Object other$type = other.getType();
        if (this$type == null) {
            if (other$type != null) {
                return false;
            }
        } else if (!this$type.equals(other$type)) {
            return false;
        }
        Object this$name = getName();
        Object other$name = other.getName();
        if (this$name == null) {
            if (other$name != null) {
                return false;
            }
        } else if (!this$name.equals(other$name)) {
            return false;
        }
        Object this$sourceQuerySql = getSourceQuerySql();
        Object other$sourceQuerySql = other.getSourceQuerySql();
        if (this$sourceQuerySql == null) {
            if (other$sourceQuerySql != null) {
                return false;
            }
        } else if (!this$sourceQuerySql.equals(other$sourceQuerySql)) {
            return false;
        }
        Object this$sourcePassword = getSourcePassword();
        Object other$sourcePassword = other.getSourcePassword();
        if (this$sourcePassword == null) {
            if (other$sourcePassword != null) {
                return false;
            }
        } else if (!this$sourcePassword.equals(other$sourcePassword)) {
            return false;
        }
        Object this$sourceUsername = getSourceUsername();
        Object other$sourceUsername = other.getSourceUsername();
        if (this$sourceUsername == null) {
            if (other$sourceUsername != null) {
                return false;
            }
        } else if (!this$sourceUsername.equals(other$sourceUsername)) {
            return false;
        }
        Object this$sourceUrl = getSourceUrl();
        Object other$sourceUrl = other.getSourceUrl();
        if (this$sourceUrl == null) {
            if (other$sourceUrl != null) {
                return false;
            }
        } else if (!this$sourceUrl.equals(other$sourceUrl)) {
            return false;
        }
        Object this$targetUrl = getTargetUrl();
        Object other$targetUrl = other.getTargetUrl();
        if (this$targetUrl == null) {
            if (other$targetUrl != null) {
                return false;
            }
        } else if (!this$targetUrl.equals(other$targetUrl)) {
            return false;
        }
        Object this$targetUsername = getTargetUsername();
        Object other$targetUsername = other.getTargetUsername();
        if (this$targetUsername == null) {
            if (other$targetUsername != null) {
                return false;
            }
        } else if (!this$targetUsername.equals(other$targetUsername)) {
            return false;
        }
        Object this$targetPassword = getTargetPassword();
        Object other$targetPassword = other.getTargetPassword();
        if (this$targetPassword == null) {
            if (other$targetPassword != null) {
                return false;
            }
        } else if (!this$targetPassword.equals(other$targetPassword)) {
            return false;
        }
        Object this$columns = getColumns();
        Object other$columns = other.getColumns();
        if (this$columns == null) {
            if (other$columns != null) {
                return false;
            }
        } else if (!this$columns.equals(other$columns)) {
            return false;
        }
        Object this$seq = getSeq();
        Object other$seq = other.getSeq();
        if (this$seq == null) {
            if (other$seq != null) {
                return false;
            }
        } else if (!this$seq.equals(other$seq)) {
            return false;
        }
        Object this$workName = getWorkName();
        Object other$workName = other.getWorkName();
        if (this$workName == null) {
            if (other$workName != null) {
                return false;
            }
        } else if (!this$workName.equals(other$workName)) {
            return false;
        }
        Object this$collectTime = getCollectTime();
        Object other$collectTime = other.getCollectTime();
        if (this$collectTime == null) {
            if (other$collectTime != null) {
                return false;
            }
        } else if (!this$collectTime.equals(other$collectTime)) {
            return false;
        }
        Object this$incrementMode = getIncrementMode();
        Object other$incrementMode = other.getIncrementMode();
        if (this$incrementMode == null) {
            if (other$incrementMode != null) {
                return false;
            }
        } else if (!this$incrementMode.equals(other$incrementMode)) {
            return false;
        }
        Object this$readerName = getReaderName();
        Object other$readerName = other.getReaderName();
        if (this$readerName == null) {
            if (other$readerName != null) {
                return false;
            }
        } else if (!this$readerName.equals(other$readerName)) {
            return false;
        }
        Object this$importName = getImportName();
        Object other$importName = other.getImportName();
        if (this$importName == null) {
            if (other$importName != null) {
                return false;
            }
        } else if (!this$importName.equals(other$importName)) {
            return false;
        }
        Object this$envCn = getEnvCn();
        Object other$envCn = other.getEnvCn();
        if (this$envCn == null) {
            if (other$envCn != null) {
                return false;
            }
        } else if (!this$envCn.equals(other$envCn)) {
            return false;
        }
        Object this$sourceLabel = getSourceLabel();
        Object other$sourceLabel = other.getSourceLabel();
        return this$sourceLabel == null ? other$sourceLabel == null : this$sourceLabel.equals(other$sourceLabel);
    }

    protected boolean canEqual(Object other) {
        return other instanceof DbMetaDataBaseSyncParam;
    }

    public int hashCode() {
        Object $type = getType();
        int result = (1 * 59) + ($type == null ? 43 : $type.hashCode());
        Object $name = getName();
        int result2 = (result * 59) + ($name == null ? 43 : $name.hashCode());
        Object $sourceQuerySql = getSourceQuerySql();
        int result3 = (result2 * 59) + ($sourceQuerySql == null ? 43 : $sourceQuerySql.hashCode());
        Object $sourcePassword = getSourcePassword();
        int result4 = (result3 * 59) + ($sourcePassword == null ? 43 : $sourcePassword.hashCode());
        Object $sourceUsername = getSourceUsername();
        int result5 = (result4 * 59) + ($sourceUsername == null ? 43 : $sourceUsername.hashCode());
        Object $sourceUrl = getSourceUrl();
        int result6 = (result5 * 59) + ($sourceUrl == null ? 43 : $sourceUrl.hashCode());
        Object $targetUrl = getTargetUrl();
        int result7 = (result6 * 59) + ($targetUrl == null ? 43 : $targetUrl.hashCode());
        Object $targetUsername = getTargetUsername();
        int result8 = (result7 * 59) + ($targetUsername == null ? 43 : $targetUsername.hashCode());
        Object $targetPassword = getTargetPassword();
        int result9 = (result8 * 59) + ($targetPassword == null ? 43 : $targetPassword.hashCode());
        Object $columns = getColumns();
        int result10 = (result9 * 59) + ($columns == null ? 43 : $columns.hashCode());
        Object $seq = getSeq();
        int result11 = (result10 * 59) + ($seq == null ? 43 : $seq.hashCode());
        Object $workName = getWorkName();
        int result12 = (result11 * 59) + ($workName == null ? 43 : $workName.hashCode());
        Object $collectTime = getCollectTime();
        int result13 = (result12 * 59) + ($collectTime == null ? 43 : $collectTime.hashCode());
        Object $incrementMode = getIncrementMode();
        int result14 = (result13 * 59) + ($incrementMode == null ? 43 : $incrementMode.hashCode());
        Object $readerName = getReaderName();
        int result15 = (result14 * 59) + ($readerName == null ? 43 : $readerName.hashCode());
        Object $importName = getImportName();
        int result16 = (result15 * 59) + ($importName == null ? 43 : $importName.hashCode());
        Object $envCn = getEnvCn();
        int result17 = (result16 * 59) + ($envCn == null ? 43 : $envCn.hashCode());
        Object $sourceLabel = getSourceLabel();
        return (result17 * 59) + ($sourceLabel == null ? 43 : $sourceLabel.hashCode());
    }

    public String getType() {
        return this.type;
    }

    public String getName() {
        return this.name;
    }

    public String getSourceQuerySql() {
        return this.sourceQuerySql;
    }

    public String getSourcePassword() {
        return this.sourcePassword;
    }

    public String getSourceUsername() {
        return this.sourceUsername;
    }

    public String getSourceUrl() {
        return this.sourceUrl;
    }

    public String getTargetUrl() {
        return this.targetUrl;
    }

    public String getTargetUsername() {
        return this.targetUsername;
    }

    public String getTargetPassword() {
        return this.targetPassword;
    }

    public List<String> getColumns() {
        return this.columns;
    }

    public Integer getSeq() {
        return this.seq;
    }

    public String getWorkName() {
        return this.workName;
    }

    public String getCollectTime() {
        return this.collectTime;
    }

    public Integer getIncrementMode() {
        return this.incrementMode;
    }

    public String getReaderName() {
        return this.readerName;
    }

    public String getImportName() {
        return this.importName;
    }

    public String getEnvCn() {
        return this.envCn;
    }

    public String getSourceLabel() {
        return this.sourceLabel;
    }
}
