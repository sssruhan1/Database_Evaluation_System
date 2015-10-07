package edu.buffalo.cse562.RAOptimizer;

import edu.buffalo.cse562.sql2RA.OpTarget;
import edu.buffalo.cse562.sql2RA.Operation;

public class Target extends OpPlusInterface {

	
	public Target(Operation _op) {
		super((OpTarget)_op);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void accept(RAVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}

}
