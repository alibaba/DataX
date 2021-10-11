package com.alibaba.datax.plugin.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableColumnMetaData implements Serializable {

    private String name;

    private String type;

    private String comment;

    private Boolean primaryKey;

    private Boolean nullable;

    private Object defaultValue;

    private String encoding;

    private String compression;

    private BigDecimal blockSize;

}
