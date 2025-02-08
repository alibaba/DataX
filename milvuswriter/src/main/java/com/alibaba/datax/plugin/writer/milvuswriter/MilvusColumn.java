package com.alibaba.datax.plugin.writer.milvuswriter;

import io.milvus.v2.common.DataType;

import java.util.Arrays;

/**
 * @author ziming(子茗)
 * @date 12/27/24
 * @description
 */
public class MilvusColumn {
    private String name;
    private String type;
    private DataType milvusTypeEnum;
    private Boolean isPrimaryKey;
    private Integer dimension;
    private Boolean isPartitionKey;
    private Integer maxLength;
    private Boolean isAutoId;
    private Integer maxCapacity;
    private String elementType;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
        for (DataType item : DataType.values()) {
            if (item.name().equalsIgnoreCase(type)) {
                this.milvusTypeEnum = item;
                break;
            }
        }
        if (this.milvusTypeEnum == null) {
            throw new RuntimeException("Unsupported type: " + type + " supported types: " + Arrays.toString(DataType.values()));
        }
    }

    public Integer getDimension() {
        return dimension;
    }

    public void setDimension(Integer dimension) {
        this.dimension = dimension;
    }

    public Integer getMaxLength() {
        return maxLength;
    }

    public void setMaxLength(Integer maxLength) {
        this.maxLength = maxLength;
    }

    public Boolean getPrimaryKey() {
        return isPrimaryKey;
    }

    public Boolean getPartitionKey() {
        return isPartitionKey;
    }

    public void setPartitionKey(Boolean partitionKey) {
        isPartitionKey = partitionKey;
    }

    public void setPrimaryKey(Boolean primaryKey) {
        isPrimaryKey = primaryKey;
    }

    public Boolean getAutoId() {
        return isAutoId;
    }

    public void setAutoId(Boolean autoId) {
        isAutoId = autoId;
    }

    public Integer getMaxCapacity() {
        return maxCapacity;
    }

    public void setMaxCapacity(Integer maxCapacity) {
        this.maxCapacity = maxCapacity;
    }

    public String getElementType() {
        return elementType;
    }

    public void setElementType(String elementType) {
        this.elementType = elementType;
    }

    public DataType getMilvusTypeEnum() {
        return milvusTypeEnum;
    }

    public void setMilvusTypeEnum(DataType milvusTypeEnum) {
        this.milvusTypeEnum = milvusTypeEnum;
    }
}
