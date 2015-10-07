package edu.buffalo.cse562.evalRA.evaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.record.BoundedConcurrentLinkedQueue;
import edu.buffalo.cse562.record.DB;
import edu.buffalo.cse562.record.Record;
import edu.buffalo.cse562.record.Row;
import edu.buffalo.cse562.record.Table;
import edu.buffalo.cse562.sql2RA.OpTarget;
import edu.buffalo.cse562.sql2RA.Operation;

public class TargetEvaluator extends RAEvaluator {
	
	public TargetEvaluator(DB db, Operation op,
			BoundedConcurrentLinkedQueue output) {
		super(db, op, output);
	}
	
	RAEvaluator rae;
	Table tbl;
	List<SelectExpressionItem> items;
	public void prepare() {
		OpTarget op = (OpTarget) this.op;
		rae = RAEvaluator.getEvaluator(this.db, op.getRight(), this.input);
		rae.prepare();
		String tblName = rae.getTableName();
		Env rae_env = rae.getEnv();
		tbl = rae_env.getTable(tblName);
		
		items = op.getTargetList();
		ArrayList<String> nsms = new ArrayList<String>();
		for (SelectExpressionItem itm:items) {
			String col_name = itm.getAlias();
			if (col_name == null) {
				col_name = expressionName(itm.getExpression());
			}
			nsms.add(col_name);
		}
		
		Table t = new Table(nsms);
		for (SelectExpressionItem itm:items) {
			String col_name = itm.getAlias();
			if (col_name == null) {
				col_name = expressionName(itm.getExpression());
			}
			t.setType(col_name, tbl.colType(col_name));
		}
		
		this.resultTable = genTableName();
		this.env.addTable(this.resultTable, t);
		this.env = this.env.mergeEnvironment(rae.getEnv());
		
	}
	@Override
	public void run() {
		HashMap<String, Integer> schema = tbl.getSchema();
		rae.start();
		
		Row r = null;
		while((r = input.poll())!=null || rae.isAlive()) {
			if (r == null) 
				continue;
			
			Row nr = new Row();
			
			for (SelectExpressionItem itm:items) {
				Expression e = itm.getExpression();
				if (e instanceof Column) {
					Column c = (Column) e;					
					String sc = expressionName(c);
					int nc = schema.get(sc);
					nr.addRecord(r.getRecord(nc));
				} else {
					if (e instanceof Expression) {
						nr.addRecord((Record) evalExpression(e, r, tbl, env));
					} 
				}
			}

			output.add(nr);
		}		
		
		while((r = input.poll())!=null) {			
			Row nr = new Row();
			
			for (SelectExpressionItem itm:items) {
				Expression e = itm.getExpression();
				if (e instanceof Column) {
					Column c = (Column) e;					
					String sc = expressionName(c);
					int nc = schema.get(sc);
					nr.addRecord(r.getRecord(nc));
				} else {
					if (e instanceof Expression) {
						nr.addRecord((Record) evalExpression(e, r, tbl, env));
					} 
				}
			}

			output.add(nr);
		}	
		
		Thread.currentThread().interrupt();
	}
}
