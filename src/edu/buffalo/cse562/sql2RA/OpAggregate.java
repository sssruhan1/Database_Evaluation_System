package edu.buffalo.cse562.sql2RA;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

public class OpAggregate extends Operation {

	private Function function;
	private Expression e;
	
	public OpAggregate(){
		super("aggregate");
	}
	
	public void setFunction(Function _function){
		function = _function;
	}
	
	@Override
	public void print() {
		System.out.println("");
		System.out.print("AGG: ALIAS: ");
		if(this.getAlias() != null){
			System.out.print(this.getAlias());
		}
		System.out.print(" FUNC: ");
		if(function != null){
			System.out.print(function.getName() + "(");
			ExpressionList el = function.getParameters();
			if(el!= null){
				List<Expression> es = function.getParameters().getExpressions();
				for( Expression e: es){
					System.out.print(e.toString() + " ");
				}
			}
			else{
				System.out.print("*");
			}
			
			System.out.print(") ");
		}
		else if( this.e != null){
			System.out.print(this.e.toString() + " ");
		}
		
		if(this.getLeft()!= null)
			this.getLeft().print();
		if(this.getRight()!=null)
			this.getRight().print();
	}
	@Override
	public void setExpressionList(ExpressionList el) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setExpression(Expression e) {
		this.e =  e;

	}
	@Override
	public Expression getExpression() {
		Expression e = function == null? this.e: function;
		return e;
	}

}
