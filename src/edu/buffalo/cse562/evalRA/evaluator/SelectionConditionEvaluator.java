package edu.buffalo.cse562.evalRA.evaluator;

import net.sf.jsqlparser.expression.Expression;
import edu.buffalo.cse562.record.BoundedConcurrentLinkedQueue;
import edu.buffalo.cse562.record.DB;
import edu.buffalo.cse562.record.Row;
import edu.buffalo.cse562.record.Table;
import edu.buffalo.cse562.sql2RA.OpSelectCondition;
import edu.buffalo.cse562.sql2RA.Operation;

public class SelectionConditionEvaluator extends RAEvaluator {

	public SelectionConditionEvaluator(DB db, Operation op,
			BoundedConcurrentLinkedQueue output) {
		super(db, op, output);
		// TODO Auto-generated constructor stub
	}

	RAEvaluator rae;
	Env rae_env;
	Table tbl;
	Expression exp;
	public void prepare() {
		OpSelectCondition sc = (OpSelectCondition) op;
		rae = RAEvaluator.getEvaluator(this.db, op.getRight(), this.input);
		rae.prepare();
		rae_env = rae.getEnv();
		exp = sc.getExpression();
		this.resultTable = genTableName();		
		tbl = rae_env.getTable(rae.resultTable);
		this.env.addTable(this.resultTable, tbl);
		this.env = this.env.mergeEnvironment(rae.env);
	}
	
	public void run() {
		rae.start();
		Row r = null;
		
		while((r=input.poll())!=null || rae.isAlive()) {
			if (r == null)
				continue;
			
			Boolean b = (Boolean)evalExpression(exp, r, tbl, env);
			
			if (b.booleanValue()) {	
				output.add(r);
			}	
		}
		
		while((r=input.poll())!=null) {
			Boolean b = (Boolean)evalExpression(exp, r, tbl, env);
		
			if (b.booleanValue()) {	
				output.add(r);
			}
		}
		
		Thread.currentThread().interrupt();
	}

}
