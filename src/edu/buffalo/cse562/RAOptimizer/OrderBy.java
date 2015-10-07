package edu.buffalo.cse562.RAOptimizer;

import edu.buffalo.cse562.sql2RA.OpOrderBy;
import edu.buffalo.cse562.sql2RA.Operation;

public class OrderBy extends OpPlusInterface {

	public OrderBy(Operation _op) {
		super((OpOrderBy)_op);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void accept(RAVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}
}
