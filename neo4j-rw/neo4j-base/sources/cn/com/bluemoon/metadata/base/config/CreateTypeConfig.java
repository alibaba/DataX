package cn.com.bluemoon.metadata.base.config;

import java.util.List;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/config/CreateTypeConfig.class */
public class CreateTypeConfig {
    private String nodeName;
    private String label;
    private String compareId;
    private List<String> columns;
    private String createType;
    private String mode;
    private String srcLabel;
    private String targetLabel;
    private String relLabel;
    private Long collectTime;
    private String incrementMode;
    private String catalogCN;
    private String envCn;
    private String srcDegreeDir = "-";
    private String targetDegreeDir = "->";
    private Integer nodeTotalCount = 0;
    private Integer relTotalCount = 0;

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setCompareId(String compareId) {
        this.compareId = compareId;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public void setCreateType(String createType) {
        this.createType = createType;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public void setSrcDegreeDir(String srcDegreeDir) {
        this.srcDegreeDir = srcDegreeDir;
    }

    public void setTargetDegreeDir(String targetDegreeDir) {
        this.targetDegreeDir = targetDegreeDir;
    }

    public void setSrcLabel(String srcLabel) {
        this.srcLabel = srcLabel;
    }

    public void setTargetLabel(String targetLabel) {
        this.targetLabel = targetLabel;
    }

    public void setRelLabel(String relLabel) {
        this.relLabel = relLabel;
    }

    public void setCollectTime(Long collectTime) {
        this.collectTime = collectTime;
    }

    public void setIncrementMode(String incrementMode) {
        this.incrementMode = incrementMode;
    }

    public void setCatalogCN(String catalogCN) {
        this.catalogCN = catalogCN;
    }

    public void setEnvCn(String envCn) {
        this.envCn = envCn;
    }

    public void setNodeTotalCount(Integer nodeTotalCount) {
        this.nodeTotalCount = nodeTotalCount;
    }

    public void setRelTotalCount(Integer relTotalCount) {
        this.relTotalCount = relTotalCount;
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof CreateTypeConfig)) {
            return false;
        }
        CreateTypeConfig other = (CreateTypeConfig) o;
        if (!other.canEqual(this)) {
            return false;
        }
        Object this$nodeName = getNodeName();
        Object other$nodeName = other.getNodeName();
        if (this$nodeName == null) {
            if (other$nodeName != null) {
                return false;
            }
        } else if (!this$nodeName.equals(other$nodeName)) {
            return false;
        }
        Object this$label = getLabel();
        Object other$label = other.getLabel();
        if (this$label == null) {
            if (other$label != null) {
                return false;
            }
        } else if (!this$label.equals(other$label)) {
            return false;
        }
        Object this$compareId = getCompareId();
        Object other$compareId = other.getCompareId();
        if (this$compareId == null) {
            if (other$compareId != null) {
                return false;
            }
        } else if (!this$compareId.equals(other$compareId)) {
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
        Object this$createType = getCreateType();
        Object other$createType = other.getCreateType();
        if (this$createType == null) {
            if (other$createType != null) {
                return false;
            }
        } else if (!this$createType.equals(other$createType)) {
            return false;
        }
        Object this$mode = getMode();
        Object other$mode = other.getMode();
        if (this$mode == null) {
            if (other$mode != null) {
                return false;
            }
        } else if (!this$mode.equals(other$mode)) {
            return false;
        }
        Object this$srcDegreeDir = getSrcDegreeDir();
        Object other$srcDegreeDir = other.getSrcDegreeDir();
        if (this$srcDegreeDir == null) {
            if (other$srcDegreeDir != null) {
                return false;
            }
        } else if (!this$srcDegreeDir.equals(other$srcDegreeDir)) {
            return false;
        }
        Object this$targetDegreeDir = getTargetDegreeDir();
        Object other$targetDegreeDir = other.getTargetDegreeDir();
        if (this$targetDegreeDir == null) {
            if (other$targetDegreeDir != null) {
                return false;
            }
        } else if (!this$targetDegreeDir.equals(other$targetDegreeDir)) {
            return false;
        }
        Object this$srcLabel = getSrcLabel();
        Object other$srcLabel = other.getSrcLabel();
        if (this$srcLabel == null) {
            if (other$srcLabel != null) {
                return false;
            }
        } else if (!this$srcLabel.equals(other$srcLabel)) {
            return false;
        }
        Object this$targetLabel = getTargetLabel();
        Object other$targetLabel = other.getTargetLabel();
        if (this$targetLabel == null) {
            if (other$targetLabel != null) {
                return false;
            }
        } else if (!this$targetLabel.equals(other$targetLabel)) {
            return false;
        }
        Object this$relLabel = getRelLabel();
        Object other$relLabel = other.getRelLabel();
        if (this$relLabel == null) {
            if (other$relLabel != null) {
                return false;
            }
        } else if (!this$relLabel.equals(other$relLabel)) {
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
        Object this$catalogCN = getCatalogCN();
        Object other$catalogCN = other.getCatalogCN();
        if (this$catalogCN == null) {
            if (other$catalogCN != null) {
                return false;
            }
        } else if (!this$catalogCN.equals(other$catalogCN)) {
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
        Object this$nodeTotalCount = getNodeTotalCount();
        Object other$nodeTotalCount = other.getNodeTotalCount();
        if (this$nodeTotalCount == null) {
            if (other$nodeTotalCount != null) {
                return false;
            }
        } else if (!this$nodeTotalCount.equals(other$nodeTotalCount)) {
            return false;
        }
        Object this$relTotalCount = getRelTotalCount();
        Object other$relTotalCount = other.getRelTotalCount();
        return this$relTotalCount == null ? other$relTotalCount == null : this$relTotalCount.equals(other$relTotalCount);
    }

    protected boolean canEqual(Object other) {
        return other instanceof CreateTypeConfig;
    }

    public int hashCode() {
        Object $nodeName = getNodeName();
        int result = (1 * 59) + ($nodeName == null ? 43 : $nodeName.hashCode());
        Object $label = getLabel();
        int result2 = (result * 59) + ($label == null ? 43 : $label.hashCode());
        Object $compareId = getCompareId();
        int result3 = (result2 * 59) + ($compareId == null ? 43 : $compareId.hashCode());
        Object $columns = getColumns();
        int result4 = (result3 * 59) + ($columns == null ? 43 : $columns.hashCode());
        Object $createType = getCreateType();
        int result5 = (result4 * 59) + ($createType == null ? 43 : $createType.hashCode());
        Object $mode = getMode();
        int result6 = (result5 * 59) + ($mode == null ? 43 : $mode.hashCode());
        Object $srcDegreeDir = getSrcDegreeDir();
        int result7 = (result6 * 59) + ($srcDegreeDir == null ? 43 : $srcDegreeDir.hashCode());
        Object $targetDegreeDir = getTargetDegreeDir();
        int result8 = (result7 * 59) + ($targetDegreeDir == null ? 43 : $targetDegreeDir.hashCode());
        Object $srcLabel = getSrcLabel();
        int result9 = (result8 * 59) + ($srcLabel == null ? 43 : $srcLabel.hashCode());
        Object $targetLabel = getTargetLabel();
        int result10 = (result9 * 59) + ($targetLabel == null ? 43 : $targetLabel.hashCode());
        Object $relLabel = getRelLabel();
        int result11 = (result10 * 59) + ($relLabel == null ? 43 : $relLabel.hashCode());
        Object $collectTime = getCollectTime();
        int result12 = (result11 * 59) + ($collectTime == null ? 43 : $collectTime.hashCode());
        Object $incrementMode = getIncrementMode();
        int result13 = (result12 * 59) + ($incrementMode == null ? 43 : $incrementMode.hashCode());
        Object $catalogCN = getCatalogCN();
        int result14 = (result13 * 59) + ($catalogCN == null ? 43 : $catalogCN.hashCode());
        Object $envCn = getEnvCn();
        int result15 = (result14 * 59) + ($envCn == null ? 43 : $envCn.hashCode());
        Object $nodeTotalCount = getNodeTotalCount();
        int result16 = (result15 * 59) + ($nodeTotalCount == null ? 43 : $nodeTotalCount.hashCode());
        Object $relTotalCount = getRelTotalCount();
        return (result16 * 59) + ($relTotalCount == null ? 43 : $relTotalCount.hashCode());
    }

    public String toString() {
        return "CreateTypeConfig(nodeName=" + getNodeName() + ", label=" + getLabel() + ", compareId=" + getCompareId() + ", columns=" + getColumns() + ", createType=" + getCreateType() + ", mode=" + getMode() + ", srcDegreeDir=" + getSrcDegreeDir() + ", targetDegreeDir=" + getTargetDegreeDir() + ", srcLabel=" + getSrcLabel() + ", targetLabel=" + getTargetLabel() + ", relLabel=" + getRelLabel() + ", collectTime=" + getCollectTime() + ", incrementMode=" + getIncrementMode() + ", catalogCN=" + getCatalogCN() + ", envCn=" + getEnvCn() + ", nodeTotalCount=" + getNodeTotalCount() + ", relTotalCount=" + getRelTotalCount() + ")";
    }

    public String getNodeName() {
        return this.nodeName;
    }

    public String getLabel() {
        return this.label;
    }

    public String getCompareId() {
        return this.compareId;
    }

    public List<String> getColumns() {
        return this.columns;
    }

    public String getCreateType() {
        return this.createType;
    }

    public String getMode() {
        return this.mode;
    }

    public String getSrcDegreeDir() {
        return this.srcDegreeDir;
    }

    public String getTargetDegreeDir() {
        return this.targetDegreeDir;
    }

    public String getSrcLabel() {
        return this.srcLabel;
    }

    public String getTargetLabel() {
        return this.targetLabel;
    }

    public String getRelLabel() {
        return this.relLabel;
    }

    public Long getCollectTime() {
        return this.collectTime;
    }

    public String getIncrementMode() {
        return this.incrementMode;
    }

    public String getCatalogCN() {
        return this.catalogCN;
    }

    public String getEnvCn() {
        return this.envCn;
    }

    public Integer getNodeTotalCount() {
        return this.nodeTotalCount;
    }

    public Integer getRelTotalCount() {
        return this.relTotalCount;
    }
}
