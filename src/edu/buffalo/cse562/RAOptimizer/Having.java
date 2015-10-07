package edu.buffalo.cse562.RAOptimizer;

import edu.buffalo.cse562.sql2RA.OpHaving;
import edu.buffalo.cse562.sql2RA.Operation;

public class Having extends OpPlusInterface{

	public Having(Operation _op) {
		super((OpHaving)_op);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void accept(RAVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}
}
