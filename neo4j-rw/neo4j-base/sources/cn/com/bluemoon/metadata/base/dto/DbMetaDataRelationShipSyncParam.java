package cn.com.bluemoon.metadata.base.dto;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/dto/DbMetaDataRelationShipSyncParam.class */
public class DbMetaDataRelationShipSyncParam extends DbMetaDataBaseSyncParam {
    private String srcLabel;
    private String targetLabel;
    private String relationShipLabel;

    public void setSrcLabel(String srcLabel) {
        this.srcLabel = srcLabel;
    }

    public void setTargetLabel(String targetLabel) {
        this.targetLabel = targetLabel;
    }

    public void setRelationShipLabel(String relationShipLabel) {
        this.relationShipLabel = relationShipLabel;
    }

    @Override // cn.com.bluemoon.metadata.base.dto.DbMetaDataBaseSyncParam
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof DbMetaDataRelationShipSyncParam)) {
            return false;
        }
        DbMetaDataRelationShipSyncParam other = (DbMetaDataRelationShipSyncParam) o;
        if (!other.canEqual(this)) {
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
        Object this$relationShipLabel = getRelationShipLabel();
        Object other$relationShipLabel = other.getRelationShipLabel();
        return this$relationShipLabel == null ? other$relationShipLabel == null : this$relationShipLabel.equals(other$relationShipLabel);
    }

    @Override // cn.com.bluemoon.metadata.base.dto.DbMetaDataBaseSyncParam
    protected boolean canEqual(Object other) {
        return other instanceof DbMetaDataRelationShipSyncParam;
    }

    @Override // cn.com.bluemoon.metadata.base.dto.DbMetaDataBaseSyncParam
    public int hashCode() {
        Object $srcLabel = getSrcLabel();
        int result = (1 * 59) + ($srcLabel == null ? 43 : $srcLabel.hashCode());
        Object $targetLabel = getTargetLabel();
        int result2 = (result * 59) + ($targetLabel == null ? 43 : $targetLabel.hashCode());
        Object $relationShipLabel = getRelationShipLabel();
        return (result2 * 59) + ($relationShipLabel == null ? 43 : $relationShipLabel.hashCode());
    }

    @Override // cn.com.bluemoon.metadata.base.dto.DbMetaDataBaseSyncParam
    public String toString() {
        return "DbMetaDataRelationShipSyncParam(srcLabel=" + getSrcLabel() + ", targetLabel=" + getTargetLabel() + ", relationShipLabel=" + getRelationShipLabel() + ")";
    }

    public String getSrcLabel() {
        return this.srcLabel;
    }

    public String getTargetLabel() {
        return this.targetLabel;
    }

    public String getRelationShipLabel() {
        return this.relationShipLabel;
    }
}
