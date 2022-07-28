package cn.com.bluemoon.metadata.base.dto;

import io.swagger.annotations.ApiModelProperty;
import java.util.List;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/dto/DataSecuritySyncParam.class */
public class DataSecuritySyncParam {
    @ApiModelProperty("映射字段")
    private String mapColumns;
    @ApiModelProperty("目标表")
    private String targetTable;
    @ApiModelProperty("统计数量sql")
    private String countCql;
    @ApiModelProperty("neo4j查询cql")
    private String cql;
    @ApiModelProperty("标签")
    private String tag;
    @ApiModelProperty("结束后的查询sql")
    private List<String> postSqlList;
    @ApiModelProperty("序列")
    private Integer seq;

    public void setMapColumns(String mapColumns) {
        this.mapColumns = mapColumns;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public void setCountCql(String countCql) {
        this.countCql = countCql;
    }

    public void setCql(String cql) {
        this.cql = cql;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public void setPostSqlList(List<String> postSqlList) {
        this.postSqlList = postSqlList;
    }

    public void setSeq(Integer seq) {
        this.seq = seq;
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof DataSecuritySyncParam)) {
            return false;
        }
        DataSecuritySyncParam other = (DataSecuritySyncParam) o;
        if (!other.canEqual(this)) {
            return false;
        }
        Object this$mapColumns = getMapColumns();
        Object other$mapColumns = other.getMapColumns();
        if (this$mapColumns == null) {
            if (other$mapColumns != null) {
                return false;
            }
        } else if (!this$mapColumns.equals(other$mapColumns)) {
            return false;
        }
        Object this$targetTable = getTargetTable();
        Object other$targetTable = other.getTargetTable();
        if (this$targetTable == null) {
            if (other$targetTable != null) {
                return false;
            }
        } else if (!this$targetTable.equals(other$targetTable)) {
            return false;
        }
        Object this$countCql = getCountCql();
        Object other$countCql = other.getCountCql();
        if (this$countCql == null) {
            if (other$countCql != null) {
                return false;
            }
        } else if (!this$countCql.equals(other$countCql)) {
            return false;
        }
        Object this$cql = getCql();
        Object other$cql = other.getCql();
        if (this$cql == null) {
            if (other$cql != null) {
                return false;
            }
        } else if (!this$cql.equals(other$cql)) {
            return false;
        }
        Object this$tag = getTag();
        Object other$tag = other.getTag();
        if (this$tag == null) {
            if (other$tag != null) {
                return false;
            }
        } else if (!this$tag.equals(other$tag)) {
            return false;
        }
        Object this$postSqlList = getPostSqlList();
        Object other$postSqlList = other.getPostSqlList();
        if (this$postSqlList == null) {
            if (other$postSqlList != null) {
                return false;
            }
        } else if (!this$postSqlList.equals(other$postSqlList)) {
            return false;
        }
        Object this$seq = getSeq();
        Object other$seq = other.getSeq();
        return this$seq == null ? other$seq == null : this$seq.equals(other$seq);
    }

    protected boolean canEqual(Object other) {
        return other instanceof DataSecuritySyncParam;
    }

    public int hashCode() {
        Object $mapColumns = getMapColumns();
        int result = (1 * 59) + ($mapColumns == null ? 43 : $mapColumns.hashCode());
        Object $targetTable = getTargetTable();
        int result2 = (result * 59) + ($targetTable == null ? 43 : $targetTable.hashCode());
        Object $countCql = getCountCql();
        int result3 = (result2 * 59) + ($countCql == null ? 43 : $countCql.hashCode());
        Object $cql = getCql();
        int result4 = (result3 * 59) + ($cql == null ? 43 : $cql.hashCode());
        Object $tag = getTag();
        int result5 = (result4 * 59) + ($tag == null ? 43 : $tag.hashCode());
        Object $postSqlList = getPostSqlList();
        int result6 = (result5 * 59) + ($postSqlList == null ? 43 : $postSqlList.hashCode());
        Object $seq = getSeq();
        return (result6 * 59) + ($seq == null ? 43 : $seq.hashCode());
    }

    public String toString() {
        return "DataSecuritySyncParam(mapColumns=" + getMapColumns() + ", targetTable=" + getTargetTable() + ", countCql=" + getCountCql() + ", cql=" + getCql() + ", tag=" + getTag() + ", postSqlList=" + getPostSqlList() + ", seq=" + getSeq() + ")";
    }

    public String getMapColumns() {
        return this.mapColumns;
    }

    public String getTargetTable() {
        return this.targetTable;
    }

    public String getCountCql() {
        return this.countCql;
    }

    public String getCql() {
        return this.cql;
    }

    public String getTag() {
        return this.tag;
    }

    public List<String> getPostSqlList() {
        return this.postSqlList;
    }

    public Integer getSeq() {
        return this.seq;
    }

    public DataSecuritySyncParam() {
    }

    public DataSecuritySyncParam(String mapColumns, String targetTable, String countCql, String cql, List<String> postSqlList, String tag, Integer seq) {
        this.mapColumns = mapColumns;
        this.targetTable = targetTable;
        this.countCql = countCql;
        this.cql = cql;
        this.postSqlList = postSqlList;
        this.tag = tag;
        this.seq = seq;
    }
}
