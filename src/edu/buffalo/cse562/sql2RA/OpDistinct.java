package edu.buffalo.cse562.sql2RA;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.SelectItem;

public class OpDistinct extends Operation {

	private Distinct distinct;
	public OpDistinct(){
		super("distinct");
	}
	public void setDistinct(Distinct _distinct){
		distinct = _distinct;
	}
	public Distinct getDistinct(){
		return distinct;
	}
	
	public void print(){
		System.out.println();
		System.out.print(this.getOpName() + "on [" );
		
		List<SelectItem> list = distinct.getOnSelectItems();
		for( SelectItem i: list){
			System.out.print(i.toString() + " ");
		}
		System.out.print("]");
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
	public Expression getExpression() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void setExpression(Expression e) {
		// TODO Auto-generated method stub
		
	}
}
