package edu.buffalo.cse562.RAOptimizer;

import edu.buffalo.cse562.sql2RA.OpUnion;
import edu.buffalo.cse562.sql2RA.Operation;

public class Union extends OpPlusInterface {

	public Union(Operation _op) {
		super((OpUnion)_op);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void accept(RAVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}
}
