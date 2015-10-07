package edu.buffalo.cse562.sql2RA;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class IndexJoin extends OpJoin {
	
	public String tableName1;
	public String tableName2;
	public String indexName1;
	public String indexName2;
	public Expression e;
	
	
	public IndexJoin(){
		super("IndexJoin");
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
	public void setIndexNames(String in1, String in2){
		indexName1 = in1;
		indexName2 = in2;
	}
	public String getIndexName1(){
		return indexName1;
	}
	public String getIndexName2(){
		return indexName2;
	}
	public Column getLeftExpression(){
		Table tl = new Table();
		tl.setName(tableName1);
		Column l = new Column();
		l.setColumnName(indexName1);
		l.setTable(tl);
		return l;
	}
	public Column getRightExpression(){
		Table tr = new Table();
		tr.setName(tableName2);
		Column r = new Column();
		r.setColumnName(indexName2);
		r.setTable(tr);
		return r;
	}
}
