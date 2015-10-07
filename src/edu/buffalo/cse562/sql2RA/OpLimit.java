package edu.buffalo.cse562.sql2RA;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.Limit;

public class OpLimit extends Operation {

	private Limit limit;
	
	public OpLimit(){
		super("limit");
	}
	public void setLimit(Limit _limit){
		limit = _limit;
	}
	public Limit getLimit(){
		return limit;
	}
	public void print(){
		if(limit != null)
			System.out.println("LIMIT " + Long.toString(limit.getRowCount()));
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

