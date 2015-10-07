package edu.buffalo.cse562.RAOptimizer;

import edu.buffalo.cse562.sql2RA.OpCross;
import edu.buffalo.cse562.sql2RA.Operation;

public class Cross extends OpPlusInterface {

	public Cross(Operation _op) {
		super((OpCross)_op);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void accept(RAVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}
}
