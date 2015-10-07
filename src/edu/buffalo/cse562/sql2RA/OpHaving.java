package edu.buffalo.cse562.sql2RA;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

public class OpHaving extends Operation {

	private Expression having;
	public OpHaving(){
		super("having");
	}
	
	public void print(){
		System.out.println("");
		if(having != null)
			System.out.print("HAVING " + having.toString());
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
		having = e;
		
	}

	@Override
	public Expression getExpression() {
		return having;
	}
	
}
