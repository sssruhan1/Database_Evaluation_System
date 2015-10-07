package edu.buffalo.cse562.RAOptimizer;

import edu.buffalo.cse562.sql2RA.Operation;

public abstract class OpPlusInterface implements OperationInterface {
	public Operation op;
	public OpPlusInterface parent;
	public OpPlusInterface left;
	public OpPlusInterface right;
	
	public OpPlusInterface(Operation _op){
		op = _op;
	}
	public Operation getOperation(){
		return op;
	}
	public void setLeft(OpPlusInterface _left){
		left = _left;
	}
	public OpPlusInterface getLeft(){
		return left;
	}
	public void setRight(OpPlusInterface _right){
		right = _right;
	}
	public OpPlusInterface getRight(){
		return right;
	}
	public void setParent(OpPlusInterface _parent){
		parent = _parent;
	}
	public OpPlusInterface getParent(){
		return parent;
	}
}
