package edu.buffalo.cse562.RAOptimizer;

import edu.buffalo.cse562.sql2RA.OpSelectCondition;
import edu.buffalo.cse562.sql2RA.Operation;

public class SelectCondition extends OpPlusInterface {

	public SelectCondition(Operation _op) {
		super((OpSelectCondition)_op);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void accept(RAVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}
}
