package edu.buffalo.cse562.evalRA.evaluator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.statement.select.OrderByElement;
import edu.buffalo.cse562.configurations.GlobalConfiguration;
import edu.buffalo.cse562.record.BigArrayList;
import edu.buffalo.cse562.record.BoundedConcurrentLinkedQueue;
import edu.buffalo.cse562.record.DB;
import edu.buffalo.cse562.record.Record;
import edu.buffalo.cse562.record.Row;
import edu.buffalo.cse562.record.Table;
import edu.buffalo.cse562.sql2RA.OpOrderBy;
import edu.buffalo.cse562.sql2RA.Operation;

public class OrderbyEvaluator extends RAEvaluator {

	class OrderbyComparator implements Comparator<Row> {
		private HashMap<String, Integer> schema;
		private List<OrderByElement> obe;
		
		public OrderbyComparator(HashMap<String, Integer> schema, List<OrderByElement> l) {
			this.schema = schema;
			this.obe = l;
		}
	    @Override
	    public int compare(Row r1, Row r2) {
	    	for (OrderByElement o:obe) {
	    		String itm = o.toString().split(" ")[0];
	    		int i = itm.indexOf(".");
	    		
	    		if (i != -1) {
	    			itm = itm.substring(i+1);
	    		}
	    		
	    		int nc = schema.get(itm);
	    		Record m = (Record) r1.getRecord(nc);
	    		Record n = (Record) r2.getRecord(nc);
	    		int cmp = m.compareTo(n);
	    		
	    		if (cmp == 0)
	    			continue;

	    		
	    		if (!o.isAsc()) {
	    			cmp = -cmp;
	    		}

	    		//System.out.println(m + " " + n + " " + cmp);
	    		return cmp;
	    	}
	        return 0;
	    }
	}
	
	public OrderbyEvaluator(DB db, Operation op,
			BoundedConcurrentLinkedQueue output) {
		super(db, op, output);
	}
	
	RAEvaluator rae;
	List<OrderByElement> order_by_elements;
	Table tbl;
	public void prepare() {
		OpOrderBy op = (OpOrderBy)this.op;
		op.getOrderByList();
		rae = RAEvaluator.getEvaluator(db, op.getRight(), input);
		rae.prepare();
		
		order_by_elements = op.getOrderByList();
		Env rae_env = rae.getEnv();		
		this.resultTable = genTableName();
		tbl = rae_env.getTable(rae.resultTable); 
		
		ArrayList<String> al = new ArrayList<String>();
		
		for (String k: tbl.getSchema().keySet()) {
			al.add(k);
		}
				
		Table t = new Table(al);
		for (String k: tbl.getSchema().keySet()) {
			t.setType(k, tbl.colType(k));			
		}
		
		this.env.addTable(resultTable, tbl);
		this.env = this.env.mergeEnvironment(rae_env);
	}
	
	public void run() {
	
		if (GlobalConfiguration.needs_swap) {
			ArrayList<Integer> compares = new ArrayList<Integer>();
			ArrayList<Boolean> ascs = new ArrayList<Boolean>();
			
			for (OrderByElement o : order_by_elements) {
				String itm = o.toString().split(" ")[0];
				int i = itm.indexOf(".");
    		
				if (i != -1) {
					itm = itm.substring(i+1);
				}
    		

				int nc = tbl.getSchema().get(itm);
				ascs.add(o.isAsc());
				compares.add(nc);
			}
			BigArrayList rows = new BigArrayList(true, compares, ascs);
			
			rae.run();
			Row r = null;
			while ((r=input.poll())!=null || rae.isAlive()) {
				if (r == null)
					continue;
				rows.add(r);
			}
			
			while ((r=input.poll())!=null) {
				rows.add(r);
			}
			
			rows.flush();
			
			rows.preparePageIterators();
			Row rr = rows.pullSorted();
			while(rr != null) {
				output.add(rr);
				rr = rows.pullSorted();
			}
			return;
		}
	
		ArrayList<Row> rows = new ArrayList<Row>();
		
		rae.start();
		Row r = null;
		while ((r=input.poll())!=null || rae.isAlive()) {
			if (r == null)
				continue;
			rows.add(r);
		}
		
		while ((r=input.poll())!=null) {
			rows.add(r);
		}
		
		Collections.sort(rows, new OrderbyComparator(tbl.getSchema(), order_by_elements));
		
		for (Row rr:rows) {
			output.add(rr);
		}
		
		Thread.currentThread().interrupt();
	}

}
