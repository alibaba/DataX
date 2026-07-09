/**
 * %datax-graph%
 * %v1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner.graph;

import lombok.Data;

/**
 * @类名: Graph
 * @说明: 图，邻接表
 *
 * @author   leehom
 * @Date	 2022年4月18日 上午10:32:17
 * 修改记录：
 *
 * @see 	 
 */
public class Graph {

	@SuppressWarnings("rawtypes")
	Vertex[] vexs; // 顶点数组
	
	/* 顶点*/
	@Data
	public static class Vertex<T> {
		
		public Vertex() {
			
		}
		public Vertex(T data) {
			this.data = data;
		}
		@SuppressWarnings("rawtypes")
		Edge first;// 该点指向的第一条边
		boolean selfRef; // 自关联
		T data; // 节点信息
	}
	
	/* 边*/
	@Data
	public static class Edge<V> {
		
		public Edge(int adjVex) {
			this.adjVex = adjVex;
		}
		public Edge(int adjVex, V data) {
			this.adjVex = adjVex;
			this.data = data;
		}

		int adjVex; // 指向的顶点
		Edge<V> next; // 指向下一个边节点
		V data;
	}

	// 增加顶点连接
	public void link(Vertex v, Edge e) {
		if(v.first==null) {
			v.first = e;
			return;
		}
		Edge l = v.first;
		while (l.next != null) {
			l = l.next;
		}
		l.next = e;
	}

	/*
	 * 创建图 vexs 顶点；vrs 边 
	 */
	public static <T, V> Graph buildGraph(Vertex<T>[] vexs, VertexRelationship<V>[] vrs) {
		Graph g = new Graph();
		g.vexs = vexs;
		for(VertexRelationship<V> vr : vrs) {
			Edge<V> e = new Edge<V>(vr.getTo(), vr.getData());
			g.link(vexs[vr.getFrom()], e);
		}
		return g;
	}

	// 遍历，集成guava event
	public <X, R> void traversal(Callback<X, R> callback) {
		for (int i = 0; i < vexs.length; i++) {
			// 顶点
			Edge e = vexs[i].first;
			while (e != null) {
				// 
				callback.execute(e);
				e = e.next;
			}
		}
	}
	 
	// 打印
	public void print() {
		System.out.printf("Graph:\n");
		for (int i = 0; i < vexs.length; i++) {
			// 顶点
			System.out.printf("%d(%s) ", i, vexs[i].data);
			Edge e = vexs[i].first;
			while (e != null) {
				System.out.printf("->%d(%s)", e.adjVex, vexs[e.adjVex].data);
				e = e.next;
			}
			System.out.printf("\n");
		}
	}

}