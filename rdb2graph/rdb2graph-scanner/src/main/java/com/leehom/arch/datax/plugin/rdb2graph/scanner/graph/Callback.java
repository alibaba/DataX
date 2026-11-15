/**
 * %datax-graph%
 * %v1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner.graph;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.graph.Graph.Edge;

/**
 * @类名: Callback
 * @说明: 遍历回调
 *
 * @author   leehom
 * @Date	 2022年4月29日 下午5:28:00
 * 修改记录：
 *
 * @see 	 
 */
public interface Callback<T, R> {

	public R execute(Edge<T> e);
	
}
