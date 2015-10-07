package edu.buffalo.cse562.sql2RA;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

public class OpCross extends Operation {

	
	public OpCross(){
		super("cross");
	}
	public void print(){
		System.out.println("(" + this.getOpName() + ")");
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
