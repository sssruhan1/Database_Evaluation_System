package edu.buffalo.cse562.sql2RA;

import net.sf.jsqlparser.expression.Expression;

public class IndexRangeScan extends OpSelectCondition {
	

	public String tableName;
	public String indexName;
	public Expression e;
	public String rangeLow = Integer.toString(Integer.MIN_VALUE);// whatif it's date()?
	public String rangeHigh = Integer.toString(Integer.MAX_VALUE);
	public boolean lowInclusive = false;
	public boolean highInclusive = false;
	
	public IndexRangeScan(){
		super("IndexRangeScan");
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
