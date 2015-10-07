package edu.buffalo.cse562.evalRA.evaluator;

import edu.buffalo.cse562.configurations.GlobalConfiguration;
import edu.buffalo.cse562.record.BoundedConcurrentLinkedQueue;
import edu.buffalo.cse562.record.DB;
import edu.buffalo.cse562.record.Row;
import edu.buffalo.cse562.record.Table;
import edu.buffalo.cse562.sql2RA.OpLimit;
import edu.buffalo.cse562.sql2RA.Operation;

public class LimitEvaluator extends RAEvaluator {

	public LimitEvaluator(DB db, Operation op, BoundedConcurrentLinkedQueue output) {
		super(db, op, output);
	}

	long nrows;
	RAEvaluator rae;
	public void prepare() {
		OpLimit op = (OpLimit)this.op;
		rae = RAEvaluator.getEvaluator(db, op.getRight(), input);
		rae.prepare();
		nrows = op.getLimit().getRowCount();
		
		Env rae_env = this.getEnv();
		
		this.resultTable = genTableName();		
		Table tbl = rae_env.getTable(rae.resultTable);
		this.env.addTable(this.resultTable, tbl);
		this.env = this.env.mergeEnvironment(rae.env);
	}
	
	public void run() {
		rae.start();
		int i = 0;
		Row r = null;		
		while((r=input.poll())!=null || rae.isAlive()) {
			if (r == null)
				continue;
			if (i >= nrows) {
				GlobalConfiguration.set_limit();
				if (GlobalConfiguration.debug) {
					System.out.println("Limit reached!");
					Thread.currentThread().interrupt();
				}
				break;
			}
				
			i++;
			output.add(r);
		}
		
		while((r=input.poll())!=null) {			
			if (i >= nrows) {
				Thread.currentThread().interrupt();
				break;
			}
				
			i++;
			output.add(r);
		}
		
		Thread.currentThread().interrupt();
	}
}
