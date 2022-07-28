package cn.com.bluemoon.metadata.base.dto;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/dto/DbMetaDataNodeSyncParam.class */
public class DbMetaDataNodeSyncParam extends DbMetaDataBaseSyncParam {
    private String nodeLabel;
    private String nodeName;

    public void setNodeLabel(String nodeLabel) {
        this.nodeLabel = nodeLabel;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    @Override // cn.com.bluemoon.metadata.base.dto.DbMetaDataBaseSyncParam
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof DbMetaDataNodeSyncParam)) {
            return false;
        }
        DbMetaDataNodeSyncParam other = (DbMetaDataNodeSyncParam) o;
        if (!other.canEqual(this)) {
            return false;
        }
        Object this$nodeLabel = getNodeLabel();
        Object other$nodeLabel = other.getNodeLabel();
        if (this$nodeLabel == null) {
            if (other$nodeLabel != null) {
                return false;
            }
        } else if (!this$nodeLabel.equals(other$nodeLabel)) {
            return false;
        }
        Object this$nodeName = getNodeName();
        Object other$nodeName = other.getNodeName();
        return this$nodeName == null ? other$nodeName == null : this$nodeName.equals(other$nodeName);
    }

    @Override // cn.com.bluemoon.metadata.base.dto.DbMetaDataBaseSyncParam
    protected boolean canEqual(Object other) {
        return other instanceof DbMetaDataNodeSyncParam;
    }

    @Override // cn.com.bluemoon.metadata.base.dto.DbMetaDataBaseSyncParam
    public int hashCode() {
        Object $nodeLabel = getNodeLabel();
        int result = (1 * 59) + ($nodeLabel == null ? 43 : $nodeLabel.hashCode());
        Object $nodeName = getNodeName();
        return (result * 59) + ($nodeName == null ? 43 : $nodeName.hashCode());
    }

    @Override // cn.com.bluemoon.metadata.base.dto.DbMetaDataBaseSyncParam
    public String toString() {
        return "DbMetaDataNodeSyncParam(nodeLabel=" + getNodeLabel() + ", nodeName=" + getNodeName() + ")";
    }

    public String getNodeLabel() {
        return this.nodeLabel;
    }

    public String getNodeName() {
        return this.nodeName;
    }
}
