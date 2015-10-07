package edu.buffalo.cse562.sql2RA;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

public class OpSelectCondition extends Operation {

	private Expression condition;
	
	public OpSelectCondition(){
		super("selectcondition");
	}
	public OpSelectCondition(String n){
		super(n);
	}
	public Expression getExpression(){
		return condition;
	}
	public void print(){
		System.out.println("");
		if(condition != null)
			System.out.println("CONDIITON: " + condition.toString());
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
		condition = e;
		
	}
	
}
