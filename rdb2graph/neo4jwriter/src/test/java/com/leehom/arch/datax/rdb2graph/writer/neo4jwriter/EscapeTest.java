package com.leehom.arch.datax.rdb2graph.writer.neo4jwriter;

import com.leehom.arch.datax.plugin.rdb2graph.writer.neo4jwriter.Utils;

public class EscapeTest {

	public static void main(String[] args) {		
		String escapeStr = "xx\\Gxx";
		String ed =Utils.strFieldEscape(escapeStr);
		System.out.print(ed);
		
	}	
}
