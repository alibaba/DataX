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

    private String field;
    private String typeName;
    private int type;

}
