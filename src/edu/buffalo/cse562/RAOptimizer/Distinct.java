package edu.buffalo.cse562.RAOptimizer;

import edu.buffalo.cse562.sql2RA.OpDistinct;
import edu.buffalo.cse562.sql2RA.Operation;

public class Distinct extends OpPlusInterface {

	public Distinct(Operation _op) {
		super((OpDistinct)_op);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void accept(RAVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}

}
