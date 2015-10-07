package edu.buffalo.cse562.record;

import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class DB {
	private HashMap<String, Table> tables = new HashMap<String, Table>();
	
	public void addTable(String path, String t, List<ColumnDefinition> fields) {
		Table tb = new Table(path, fields);
		tables.put(t, tb);
	}
	
	public Table getTable(String t) {
		return tables.get(t);
	}
	
	public HashMap<String, Table> getTables() {
		return tables;
	}
}
