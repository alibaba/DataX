/**
 * %datax-graph%
 * %v1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.graph.Graph;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.graph.VertexRelationship;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.graph.Graph.Vertex;

/**
 * @类名: GraphTest
 * @说明: 图测试
 *
 * @author   leehom
 * @Date	 2022年4月18日 上午10:50:36
 * 修改记录：
 *
 * @see 	 
 */
public class GraphTest {
	/**
	 测试图：
	 a->b
	  ->c
	 d->e
	 f
	 */
	public static void main(String[] args) {		
		// 构建顶点
		Vertex<String> va = new Vertex<>("a"); // 0
		Vertex<String> vb = new Vertex<>("b"); // 1
		Vertex<String> vc = new Vertex<>("c"); // 2
		Vertex<String> vd = new Vertex<>("d"); // 3
		Vertex<String> ve = new Vertex<>("e"); // 4
		Vertex<String> vf = new Vertex<>("f"); // 5
		// 构建关系
		VertexRelationship vr1 = new VertexRelationship(0, 1);
		VertexRelationship vr2 = new VertexRelationship(0, 2);
		VertexRelationship vr3 = new VertexRelationship(3, 4);
		// 
	    Graph g = Graph.buildGraph(new Vertex[] {va,vb,vc,vd,ve,vf}, new VertexRelationship[] {vr1,vr2,vr3});
	    // 打印
	    g.print();
		
	}	
 
}
