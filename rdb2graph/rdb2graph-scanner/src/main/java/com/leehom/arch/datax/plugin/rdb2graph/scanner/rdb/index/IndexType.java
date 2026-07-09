package com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.index;


/**
 * @类名: IndexMode
 * @说明: 
 *
 * @author   leehom
 * @Date	 2022年4月19日 下午12:06:13
 * 修改记录：
 *
 * @see 	 
 */

//public static final short tableIndexStatistic = 0;
//
//public static final short tableIndexClustered = 1;
//
//public static final short tableIndexHashed = 2;
//
//public static final short tableIndexOther = 3;

public enum IndexType {
	
    STATISTIC("statistic"), CLUSTERED("clustered"), HASHED("hashed"), OTHER("other");

    private String value;

    private IndexType(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }

    public String getValue() {
        return this.value;
    }
}
