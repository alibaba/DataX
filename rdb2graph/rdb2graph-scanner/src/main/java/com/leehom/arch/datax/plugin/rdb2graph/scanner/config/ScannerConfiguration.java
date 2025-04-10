package com.leehom.arch.datax.plugin.rdb2graph.scanner.config;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.AbstractDbScanner;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.MysqlScanner;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.OracleScanner;

class ScannerConfiguration {

	AbstractDbScanner oracleScanner() {
		return new OracleScanner();
	}
	
	AbstractDbScanner mysqlScanner() {
		return new MysqlScanner();
	}
	
}
