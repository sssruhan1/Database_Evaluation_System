package edu.buffalo.cse562.evalRA.evaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import edu.buffalo.cse562.record.Record;
import edu.buffalo.cse562.record.Row;
import edu.buffalo.cse562.record.Table;

public class Env {
	public HashMap<String, LinkedHashMap<String, Integer>> schemas = new HashMap<String, LinkedHashMap<String, Integer>>();
	public HashMap<String, LinkedHashMap<String, String>> types = new HashMap<String, LinkedHashMap<String, String>>();
	public HashMap<String, Record> bindings = new HashMap<String, Record>();
	public HashMap<String, Table> tables = new HashMap<String, Table>();
	public HashMap<String, HashMap<String, Integer>> indexes = new HashMap<String, HashMap<String, Integer>>();
	public HashMap<String, String> column_agg_op = new HashMap<String, String>();
	public HashMap<String, HashMap<String, String>> table_alias = new HashMap<String, HashMap<String, String>>();
	public HashMap<String, Boolean> column_agg_distinct = new HashMap<String, Boolean>();
	public boolean group_flag = false;
	public Env() {}
	public Env(String table_name, LinkedHashMap<String, Integer> schema, LinkedHashMap<String, String> type) {
		this.addTable(table_name, schema, type);
	}
	
	public void addTable(String table_name, LinkedHashMap<String, Integer> schema, LinkedHashMap<String, String> type) {
		schemas.put(table_name, schema);
		types.put(table_name, type);
	}
	
	public void addTables(HashMap<String, Table> tables) {
		this.tables.putAll(tables);
	}
	
	public void addTable(String table_name, Table tbl) {
		this.tables.put(table_name,  tbl);
	}
	
	public void addTable(String table_name, LinkedHashMap<String, Integer> schema, LinkedHashMap<String, String> type, ArrayList<Row> rows) {
		
	}
	
	public Table getTable(String table_name) {
		return tables.get(table_name);
	}
	
	public LinkedHashMap<String, Integer> getTableSchema(String table_name) {
		return schemas.get(table_name);
	}
	
	public LinkedHashMap<String, String> getTableTypes(String table_name) {
		return types.get(table_name);
	}	
	
	public Record getVariable(String varName) {
		return bindings.get(varName);
	}
	
	public Env mergeEnvironment(Env env) {
		Env e = new Env();
		e.schemas.putAll(this.schemas);
		e.schemas.putAll(env.schemas);
		e.types.putAll(this.types);
		e.types.putAll(env.types);
		e.bindings.putAll(this.bindings);
		e.bindings.putAll(env.bindings);
		e.tables.putAll(env.tables);
		e.tables.putAll(this.tables);
		e.column_agg_op.putAll(this.column_agg_op);
		e.column_agg_op.putAll(env.column_agg_op);
		e.group_flag = env.group_flag;
		e.table_alias.putAll(env.table_alias);
		e.table_alias.putAll(this.table_alias);
		e.column_agg_distinct.putAll(env.column_agg_distinct);
		e.column_agg_distinct.putAll(this.column_agg_distinct);
		return e;
	}
	
	public void addTableAlias(String original_table, String original_field, String new_field) {
		HashMap<String, String> ta = table_alias.get(original_table);
		if (ta == null) {
			ta = new HashMap<String, String>();
			ta.put(original_field, new_field);
			table_alias.put(original_table, ta);
		} else {
			ta.put(original_field, new_field);
		}
	}
	
	public String aliasLookUp(String original_table, String original_field) {
		HashMap<String, String> ta = table_alias.get(original_table);

		if (ta == null) {
			return original_field;
		} 
		
		if (ta.get(original_field)!=null) {
			return ta.get(original_field);
		}
		
		return original_field;
	}
}
