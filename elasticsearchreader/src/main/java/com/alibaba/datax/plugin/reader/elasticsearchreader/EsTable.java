package com.alibaba.datax.plugin.reader.elasticsearchreader;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kesc
 * @date 2020-05-11 10:06
 */
@Getter
@Setter
public class EsTable {
    private String name;
    private String nameCase;
    private String filter;
    private String deleteFilterKey;
    private List<EsField> column = new ArrayList<>();
}
