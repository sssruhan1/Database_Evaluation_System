package edu.buffalo.cse562.sql2RA;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.SelectItem;

public class OpTarget extends Operation {

	private ArrayList<SelectItem> targetList;
	
	public OpTarget(){
		super("Target");
	}
	public void setTargetList(List l){
		targetList = (ArrayList<SelectItem>) l;
	}
	public List getTargetList(){
		return targetList;
	}
	public void print(){
		System.out.println("");
		System.out.print("TARGET: ");
		int i = 0;
		for(SelectItem itr: targetList){
			System.out.print(Integer.toString(i++) + ": ");
			System.out.print( itr.toString()+ " ");
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
	
	public void setExpression(SelectItem e){
		if(targetList == null){
			targetList = new ArrayList<SelectItem>();
		}
		targetList.add(e);
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
