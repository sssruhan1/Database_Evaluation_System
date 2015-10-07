package edu.buffalo.cse562.RAOptimizer;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import edu.buffalo.cse562.configurations.GlobalConfiguration;
import edu.buffalo.cse562.sql2RA.IndexJoin;
import edu.buffalo.cse562.sql2RA.IndexRangeScan;
import edu.buffalo.cse562.sql2RA.OpSelectCondition;
import edu.buffalo.cse562.sql2RA.OpTable;
import edu.buffalo.cse562.sql2RA.Operation;

public class Where2RangeScan {
	public static Operation tryConvert(OpSelectCondition op, OpTable opt) {
		Expression exp = op.getExpression();
		
		if (!(exp instanceof AndExpression)) {
			return op;
		}
		
		AndExpression opAnd = (AndExpression) exp;
		Expression left = opAnd.getLeftExpression();
		Expression right = opAnd.getRightExpression();
		
		String upperBound = "";
		String lowerBound = "";
		boolean upperInclusive = false;
		boolean lowerInclusive = false;
		String item = "";
		
		if (left instanceof MinorThan) {
			item = ((MinorThan) left).getLeftExpression().toString();
			upperBound = ((MinorThan) left).getRightExpression().toString();
		}
		
		if (left instanceof MinorThanEquals) {
			item = ((MinorThanEquals) left).getLeftExpression().toString();
			upperBound = ((MinorThanEquals) left).getRightExpression().toString();
			upperInclusive = true;
		}
		
		if (left instanceof GreaterThan) {
			item = ((GreaterThan) left).getLeftExpression().toString();
			lowerBound = ((GreaterThan) left).getRightExpression().toString();
		}
		
		if (left instanceof GreaterThanEquals) {
			item = ((GreaterThanEquals) left).getLeftExpression().toString();
			lowerBound = ((GreaterThanEquals) left).getRightExpression().toString();
			lowerInclusive = true;
		}
		
		if (right instanceof MinorThan) {
			item = ((MinorThan) right).getLeftExpression().toString();
			upperBound = ((MinorThan) right).getRightExpression().toString();
		}
		
		if (right instanceof MinorThanEquals) {
			item = ((MinorThanEquals) right).getLeftExpression().toString();
			upperBound = ((MinorThanEquals) right).getRightExpression().toString();
			upperInclusive = true;
		}
		
		if (right instanceof GreaterThan) {
			item = ((GreaterThan) right).getLeftExpression().toString();
			lowerBound = ((GreaterThan) right).getRightExpression().toString();
		}
		
		if (right instanceof GreaterThanEquals) {
			item = ((GreaterThanEquals) right).getLeftExpression().toString();
			lowerBound = ((GreaterThanEquals) right).getRightExpression().toString();
			lowerInclusive = true;
		}
		
		IndexRangeScan rs = new IndexRangeScan();
		rs.tableName = opt.getAlias();
		rs.highInclusive = upperInclusive;
		rs.lowInclusive = lowerInclusive;
		rs.indexName = item;
		rs.rangeLow = lowerBound;
		rs.rangeHigh = upperBound;
		
		rs.setKid(opt);
		if (GlobalConfiguration.debug)
			System.out.println("LOW: " + lowerBound + " HIGH: " + upperBound + " ITEM: " + item);
		return rs;
	}
	
	public static Operation tryMoveDownSelectCondition(Operation op) {
		Operation cur_op = op;
		boolean find_condition = false;
		while (!(cur_op instanceof OpTable)) {
			if ((cur_op instanceof OpSelectCondition) &&
			    !(cur_op instanceof IndexRangeScan)) {
				find_condition = true;
				break;
			}
				
			cur_op = cur_op.getRight();
		}
		
		if (!find_condition) 
			return op;
		
		OpSelectCondition cond = (OpSelectCondition) cur_op;
		Expression exp = cond.getExpression();
		if (exp instanceof AndExpression) {
			Expression left = ((AndExpression) exp).getLeftExpression();
			while (left instanceof AndExpression)
				left = ((AndExpression) left).getLeftExpression();
			
			if (cur_op.getRight() instanceof IndexJoin &&
			    cur_op.getRight().getRight() instanceof IndexRangeScan) {
				System.out.println("Found and: " + left);
			}
			
		}
		
		return op;
	}

}
