package com.alibaba.datax.plugin.writer.tdenginewriter;

public class TableMeta {
    TableType tableType;
    String tbname;
    int columns;
    int tags;
    int tables;
    String stable_name;

    @Override
    public String toString() {
        return "TableMeta{" +
                "tableType=" + tableType +
                ", tbname='" + tbname + '\'' +
                ", columns=" + columns +
                ", tags=" + tags +
                ", tables=" + tables +
                ", stable_name='" + stable_name + '\'' +
                '}';
    }
}
