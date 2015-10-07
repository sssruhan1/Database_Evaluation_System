package edu.buffalo.cse562.RAOptimizer;

import edu.buffalo.cse562.sql2RA.OpLimit;
import edu.buffalo.cse562.sql2RA.Operation;

public class Limit extends OpPlusInterface {

	
	public Limit(Operation _op) {
		super((OpLimit)_op);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void accept(RAVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}
}
