package edu.buffalo.cse562.sql2RA;


import java.util.HashMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

public class OpTable extends Operation {

	private String name;// alias
	private int size; // original size of the table
	private HashMap<String, String> index; // key: index value, value: index name
	
	public OpTable(){
		super("table");
		index = new HashMap<String, String>();
	}
	
	public void print(){
		System.out.println("");
		System.out.println("SOURCE: " + this.getAlias());
		if(this.getLeft() != null)
			this.getLeft().print();
		if(this.getRight() != null)
			this.getRight().print();
	}
	
	@Override
	public void setExpressionList(ExpressionList el) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void setExpression(Expression e) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public Expression getExpression() {
		// TODO Auto-generated method stub
		return null;
	}

	public int getSize(){
		return size;
	}
	public void setSize(int _size){
		size = _size;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
		String upperCase = name.toUpperCase();
		if(upperCase.compareTo("LINEITEM") == 0){
			// INDEX
			index.put("shipdate", "shipdate");
			// primary key
			index.put("orderkey", "orderkey");
			index.put("linenumber", "linenumber");
			size = 2;
		}else if(upperCase.compareTo("ORDERS") == 0){
			index.put("orderdate", "orderdate");
			//
			index.put("orderkey", "orderkey");
			size = 5;	
		}else if(upperCase.compareTo("PART") == 0){
			//
			index.put("partkey", "partkey");
			size = 3;
		}else if(upperCase.compareTo("CUSTOMER") == 0){
			//
			index.put("custkey", "custkey");
			size = 8;
		}else if(upperCase.compareTo("SUPPLIER") == 0){
			//
			index.put("suppkey", "suppkey");
			size = 1;
		}else if(upperCase.compareTo("PARTSUPP") == 0){
			index.put("suppkey", "suppkey");
			//
			index.put("partkey", "partkey");
			index.put("suppkey", "suppkey");
			size = 4;
		}else if(upperCase.compareTo("NATION") == 0){
			index.put("name", "name");
			//
			//index.put("nationkey", "nationkey");
			if (this.getAlias() != null){
				if( this.getAlias().compareTo("n1") == 0)
					size = 1;
				else
					size = 6;
			}
			else
			    size = 6;
		}else if(upperCase.compareTo("REGION") == 0){
			index.put("name", "name");
			//
			//index.put("regionkey", "regionkey");
			size = 7;
		}
	}
	public void setIndex(String attributeName, String indexName){
		index.put(attributeName, indexName);
	}
	public HashMap<String, String> getIndex(){
		return index;
	}
	
	
}
