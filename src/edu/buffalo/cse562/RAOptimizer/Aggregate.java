package edu.buffalo.cse562.RAOptimizer;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import edu.buffalo.cse562.sql2RA.OpAggregate;
import edu.buffalo.cse562.sql2RA.Operation;

public class Aggregate extends OpPlusInterface {

	public Aggregate(Operation _op) {
		super((OpAggregate)_op);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void accept(RAVisitor visitor) {
		// TODO Auto-generated method stub
		visitor.visit(this);
	}
}
