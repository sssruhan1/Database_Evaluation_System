package edu.buffalo.cse562.RAOptimizer;

import edu.buffalo.cse562.sql2RA.OpJoin;
import edu.buffalo.cse562.sql2RA.Operation;

public class Join extends OpPlusInterface {

	
	public Join(Operation _op) {
		super((OpJoin)_op);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void accept(RAVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}

}
