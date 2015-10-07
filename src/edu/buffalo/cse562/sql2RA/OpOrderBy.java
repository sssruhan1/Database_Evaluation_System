package edu.buffalo.cse562.sql2RA;

import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

public class OpOrderBy extends Operation {

	private List orderByList;
	public OpOrderBy(){
		super("orderby");
	}
	public void setOrderByList(List _orderByList){
		orderByList = _orderByList;
	}
	public List getOrderByList(){
		return orderByList;
	}
	
	public void print(){
		System.out.println("");
		System.out.print("ORDER BY ");
		Iterator itr = orderByList.iterator();
		while(itr.hasNext()){
			System.out.print(itr.next().toString() + " ");
		}
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
}
