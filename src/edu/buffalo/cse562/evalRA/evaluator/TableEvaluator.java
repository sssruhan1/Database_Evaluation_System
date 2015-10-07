package edu.buffalo.cse562.evalRA.evaluator;

import edu.buffalo.cse562.configurations.GlobalConfiguration;
import edu.buffalo.cse562.record.BoundedConcurrentLinkedQueue;
import edu.buffalo.cse562.record.DB;
import edu.buffalo.cse562.record.Row;
import edu.buffalo.cse562.record.Table;
import edu.buffalo.cse562.sql2RA.OpTable;
import edu.buffalo.cse562.sql2RA.Operation;

public class TableEvaluator extends RAEvaluator{
	
	public TableEvaluator(DB db, Operation op, BoundedConcurrentLinkedQueue output) {
		super(db, op, output);
	}

	String tableName = "";
	Table tbl;
	public void prepare() {
		OpTable op = (OpTable) this.op;
		tableName = op.getAlias();
		tbl = this.env.getTable(op.getAlias());
		if (tbl == null) {
			String tbl_name = GlobalConfiguration.getTableAlias(op.getAlias());
			tableName = tbl_name;
			tbl = this.env.getTable(tbl_name).clone();
			this.env.addTable(op.getAlias(), tbl);
		}
		this.resultTable = op.getAlias();
	}
	@Override
	public void run() {
		Thread t = tbl.AsyncRead(input);
		t.start();
		Row r = null;
		while ((r=input.poll())!=null||t.isAlive()) {
			if (r == null)
				continue;
			
			output.add(r);
		}
	
		while ((r=input.poll())!=null) {
			output.add(r);
		}
		
		Thread.currentThread().interrupt();
	}
}
