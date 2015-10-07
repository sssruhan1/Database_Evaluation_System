package edu.buffalo.cse562.RAOptimizer;

import edu.buffalo.cse562.sql2RA.OpGroupBy;
import edu.buffalo.cse562.sql2RA.Operation;

public class GroupBy extends OpPlusInterface {

	public GroupBy(Operation _op) {
		super((OpGroupBy)_op);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void accept(RAVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}
	
	
}
