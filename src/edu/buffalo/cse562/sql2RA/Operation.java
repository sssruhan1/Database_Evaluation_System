package edu.buffalo.cse562.sql2RA;

import edu.buffalo.cse562.RAOptimizer.Annotation;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

public abstract class Operation {
	private String opName;
	private String alias;
	private Operation parent;
	private Operation left;
	private Operation right;
	private Annotation annotation;
	
	public Operation(){
	}
	
	public Operation(String _opName){
		opName = _opName;
	}
	public String getOpName(){
		return opName;
	}
	public Operation getLeft(){
		return left;
	}
	public Operation getRight(){
		return right;
	}
	public Operation getParent(){
		return parent;
	}
	// for the purpose of rebuilding tree
	public void setKidNull(){
		left = null;
		right = null;
	}
	public void setKid(Operation _kid){
		if(right == null){
			right = _kid;
			_kid.parent = this;
		}
		else if( left == null){
			left = _kid;
			_kid.parent = this;
		}
		//this is bad!!!
		else{
			Operation op = null;
			if(opName.equals("cross")){
				op = new OpCross();
			}
			else if (opName.equals("union")){
				op = new OpUnion();
			}
			op.right = this.left;
			this.left.parent = op;
			this.left = op;
			op.parent = this;
			op.left = _kid;
			_kid.parent = op;
		}	
	}
	public void setParent(Operation _parent){
		_parent.setKid(this);
	}
	public Operation getRoot(){
		Operation root = this;
		while(root.parent!= null){
			root = root.parent;
		}
		return root;
	}
	public void setAlias(String _alias){
		alias = _alias;
	}
	public String getAlias(){
		return alias;
	}
	public void setAnnotation(Annotation a){
		annotation = a;
	}
	public Annotation getAnnotation(){
		return annotation;
	}
	public abstract void print();
	public abstract void setExpressionList(ExpressionList el);
	public abstract void setExpression(Expression e);
	public abstract Expression getExpression();
}
