package edu.buffalo.cse562.RAOptimizer;

import edu.buffalo.cse562.sql2RA.Operation;

public interface OperationInterface {
	 void accept(RAVisitor visitor);
}
