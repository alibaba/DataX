package cn.com.bluemoon.metadata.base.dto;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/dto/QueryMetaDataTemplateDto.class */
public class QueryMetaDataTemplateDto {
    private String getSchemas;
    private String getTables;
    private String getTableColumn;
    private String getSQLIndexes;

    public String toString() {
        return "QueryMetaDataTemplateDto(getSchemas=" + getGetSchemas() + ", getTables=" + getGetTables() + ", getTableColumn=" + getGetTableColumn() + ", getSQLIndexes=" + getGetSQLIndexes() + ")";
    }

    public void setGetSchemas(String getSchemas) {
        this.getSchemas = getSchemas;
    }

    public void setGetTables(String getTables) {
        this.getTables = getTables;
    }

    public void setGetTableColumn(String getTableColumn) {
        this.getTableColumn = getTableColumn;
    }

    public void setGetSQLIndexes(String getSQLIndexes) {
        this.getSQLIndexes = getSQLIndexes;
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof QueryMetaDataTemplateDto)) {
            return false;
        }
        QueryMetaDataTemplateDto other = (QueryMetaDataTemplateDto) o;
        if (!other.canEqual(this)) {
            return false;
        }
        Object this$getSchemas = getGetSchemas();
        Object other$getSchemas = other.getGetSchemas();
        if (this$getSchemas == null) {
            if (other$getSchemas != null) {
                return false;
            }
        } else if (!this$getSchemas.equals(other$getSchemas)) {
            return false;
        }
        Object this$getTables = getGetTables();
        Object other$getTables = other.getGetTables();
        if (this$getTables == null) {
            if (other$getTables != null) {
                return false;
            }
        } else if (!this$getTables.equals(other$getTables)) {
            return false;
        }
        Object this$getTableColumn = getGetTableColumn();
        Object other$getTableColumn = other.getGetTableColumn();
        if (this$getTableColumn == null) {
            if (other$getTableColumn != null) {
                return false;
            }
        } else if (!this$getTableColumn.equals(other$getTableColumn)) {
            return false;
        }
        Object this$getSQLIndexes = getGetSQLIndexes();
        Object other$getSQLIndexes = other.getGetSQLIndexes();
        return this$getSQLIndexes == null ? other$getSQLIndexes == null : this$getSQLIndexes.equals(other$getSQLIndexes);
    }

    protected boolean canEqual(Object other) {
        return other instanceof QueryMetaDataTemplateDto;
    }

    public int hashCode() {
        Object $getSchemas = getGetSchemas();
        int result = (1 * 59) + ($getSchemas == null ? 43 : $getSchemas.hashCode());
        Object $getTables = getGetTables();
        int result2 = (result * 59) + ($getTables == null ? 43 : $getTables.hashCode());
        Object $getTableColumn = getGetTableColumn();
        int result3 = (result2 * 59) + ($getTableColumn == null ? 43 : $getTableColumn.hashCode());
        Object $getSQLIndexes = getGetSQLIndexes();
        return (result3 * 59) + ($getSQLIndexes == null ? 43 : $getSQLIndexes.hashCode());
    }

    public String getGetSchemas() {
        return this.getSchemas;
    }

    public String getGetTables() {
        return this.getTables;
    }

    public String getGetTableColumn() {
        return this.getTableColumn;
    }

    public String getGetSQLIndexes() {
        return this.getSQLIndexes;
    }
}
