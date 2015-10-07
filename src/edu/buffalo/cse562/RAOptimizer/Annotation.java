package edu.buffalo.cse562.RAOptimizer;

import net.sf.jsqlparser.expression.Expression;

public class Annotation {
	public Expression exp; 
	public String idx; // index id
	public String idxType; // index type, e.g. B+, Hash, HashJoin, IndexNestedLoopJoin
	public String idx2; // for hash joins. the other idx.
	public String tableName1;
	public String tableName2;
	public String rangeLow;
	public String rangeHigh;
	public boolean lowInclusive;
	public boolean highInclusive;
	public boolean isLeft; // for index nlj only
	public boolean isRight;// index in rhs
	public boolean filterInLeft;
	public boolean filterInRight;
}
