package com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.index;


/**
 * @类名: IndexOrder
 * @说明: 
 *
 * @author   leehom
 * @Date	 2022年4月19日 下午12:06:13
 * 修改记录：
 *
 * @see 	 
 */
public enum IndexOrder {
	
    ASC("ASC"), DESC("DESC");

    String value;

    private IndexOrder(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }

    public String getValue() {
        return this.value;
    }
}
