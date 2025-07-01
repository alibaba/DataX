package com.leehom.arch.datax.plugin.rdb2graph.scanner.config;

import java.net.URI;

import lombok.Data;

@Data
public class Neo4jDriverProperties {

	/**
	 * The uri this driver should connect to. The driver supports bolt or neo4j as schemes.
	 */
	private URI uri;
	private String database;
	/**
	 * The login of the user connecting to the database.
	 */
	private String username;

	/**
	 * The password of the user connecting to the database.
	 */
	private String password;
}
