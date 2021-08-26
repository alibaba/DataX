package com.alibaba.datax.plugin.domain;

import com.alibaba.datax.plugin.Converter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DataConverter implements Serializable {

    private static final long serialVersionUID = 1829286600375132047L;

    private String type;

    private Map<String, Object> param;

    private Converter converter;

}
