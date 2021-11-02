package com.alibaba.datax.plugin.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Map;

@Slf4j
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SaPlugin implements Serializable {

    private static final long serialVersionUID = 7544899907011021972L;

    private String name;
    private String className;
    private Map<String,Object> param;
}
