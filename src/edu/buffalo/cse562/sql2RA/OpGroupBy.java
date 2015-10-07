package edu.buffalo.cse562.sql2RA;

import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

public class OpGroupBy extends Operation {

   List groupByColumnList;
	public OpGroupBy(){
		super("groupby");
	}
	public void setGroupByColumnList(List _groupByColumnList){
		groupByColumnList = _groupByColumnList;
	}
	public List getGroupByColumnList(){
		return groupByColumnList;
	}
	public void print(){
		System.out.println("");
		System.out.print("GROUP BY ");
		Iterator itr = groupByColumnList.iterator();
		while(itr.hasNext()){
			System.out.print(itr.next().toString() + " ");
		}
		if(this.getLeft() != null)
			this.getLeft().print();
		
		if(this.getRight()!= null)
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
}
