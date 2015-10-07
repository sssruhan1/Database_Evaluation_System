package edu.buffalo.cse562.sql2RA;

import net.sf.jsqlparser.expression.Expression;

public class IndexScan extends OpSelectCondition {

	public String tableName;
	public String indexName;
	public Expression e;
	
	public IndexScan(){
		super("IndexScan");
	}
	public void setTabelName(String _tn){
		tableName = _tn;
	}
	public String getTableName(){
		return tableName;
	}
	
	public void setIndexName(String _in){
		indexName = _in;
	}
	public String getIndexName(){
		return indexName;
	}

}
