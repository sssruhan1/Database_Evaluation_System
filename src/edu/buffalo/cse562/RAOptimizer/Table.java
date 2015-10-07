package edu.buffalo.cse562.RAOptimizer;

import edu.buffalo.cse562.sql2RA.OpTable;
import edu.buffalo.cse562.sql2RA.Operation;

public class Table extends OpPlusInterface {

	public Table(Operation _op) {
		super((OpTable)_op);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void accept(RAVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}
	
}
