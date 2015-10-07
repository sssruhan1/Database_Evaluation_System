package edu.buffalo.cse562.RAOptimizer;

import edu.buffalo.cse562.sql2RA.*;

public interface RAVisitor {
	
	void visit(Aggregate agg);
	void visit(Cross cross);
	void visit(Distinct distinct);
	void visit(GroupBy groupby);
	void visit(Having having);
	void visit(Join join);
	void visit(Limit limit);
	void visit(OrderBy orderby);
	void visit(SelectCondition selectcondition);
	void visit(Table table);
	void visit(Target target);
	void visit(Union union);
}
