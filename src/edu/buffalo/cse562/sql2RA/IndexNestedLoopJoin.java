package edu.buffalo.cse562.sql2RA;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class IndexNestedLoopJoin extends OpJoin {

	public String tableName1;
	public String tableName2;
	public String indexName1;
	public String columnName2;
	public boolean isLeft = false;
	public IndexNestedLoopJoin(){
		super("IndexNestedLoopJoin");
	}
	
	public void setTableNames(String tn1, String tn2){
		tableName1 = tn1;
		tableName2 = tn2;
	}
	public String getTableName1(){
		return tableName1;
	}
	public String getTableName2(){
		return tableName2;
	}
	public void setColumnNames(String in1, String cn2){
		indexName1 = in1;
		columnName2 = cn2;
	}
	public Column getIndexedColumn(){
		Table t = new Table();
		t.setName(this.tableName1);
		Column c = new Column();
		c.setTable(t);
		c.setColumnName(indexName1);
		return c;
	}
	public Column getNonIndexedColumn(){
		Table t = new Table();
		t.setName(this.tableName2);
		Column c = new Column();
		c.setTable(t);
		c.setColumnName(columnName2);
		return c;
	}

}
