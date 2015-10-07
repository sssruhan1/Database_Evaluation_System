package edu.buffalo.cse562.sql2RA;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

public class OpJoin extends Operation {

	Expression condition;
	public OpJoin(){
		super("Join");
	}
	public OpJoin(String n){
		super(n);
	}
	@Override
	public void print() {
		// TODO Auto-generated method stub

	}

	@Override
	public void setExpressionList(ExpressionList el) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setExpression(Expression e) {
		condition = e;
	}

	@Override
	public Expression getExpression() {
		return condition;
	}

}
