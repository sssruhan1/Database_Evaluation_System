package edu.buffalo.cse562.sql2RA;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

public class MyStatementVisitor implements StatementVisitor {
	
	static boolean debug = false;
	private HashMap<String, List> tables;
	private Operation root;
	private HashMap<String, Expression> insertItems; // column name as key, expression as item.
	private String insertTableName;
	private String deleteTableName;
	private Expression deleteWhere;
	private String updateTableName;
	private Expression updateWhere;
	private HashMap<String, Expression> updateItems;

	@Override
	public void visit(Select arg0) {
		// TODO Auto-generated method stub
		//System.out.println(arg0.toString());
		Sql2RA trans = new Sql2RA();
		root = trans.getRA(arg0);
		//root.print();
		
		if (debug) {
			RADebug debug = new RADebug("RA.png");
			debug.debugRATree(root);
		}
	}

	@Override
	public void visit(Delete arg0) {
		// TODO Auto-generated method stub
		deleteTableName = arg0.getTable().getName();
		deleteWhere = arg0.getWhere();
	}

	public String getDelTableName(){
		return deleteTableName;
	}
	public Expression getDelWhere(){
		return deleteWhere;
	}
	
	
	@Override
	public void visit(Update arg0) {
		// TODO Auto-generated method stub
		updateTableName = arg0.getTable().getName();
		updateWhere = arg0.getWhere();
		setUpdateItems(arg0);
	}

	public void setUpdateItems(Update arg0){
		updateItems = new HashMap<String, Expression>();
		List<Column> cols = arg0.getColumns();
		Iterator itrc = cols.iterator();
		List el = arg0.getExpressions();
		if (el != null){
			Iterator itri = el.iterator();
			insertItems = new HashMap<String, Expression>();
			while(itrc.hasNext() && itri.hasNext()){
				Column c = (Column) itrc.next();
				Expression e = (Expression) itri.next();
				updateItems.put(c.getColumnName(), e);
			}
		}
	}
	public HashMap<String, Expression> getUpdateItems(){
		return updateItems;
	}
	public String getUpdateTableName(){
		return updateTableName;
	}
	public Expression getUpdateWhere(){
		return updateWhere;
	}
	
	public class MyItemsListVisitor implements ItemsListVisitor{
		private ExpressionList el;
		private Operation op;
		private boolean isEL = false;

		@Override
		public void visit(SubSelect arg0) {
			// TODO Auto-generated method stub
			Sql2RA trans = new Sql2RA();
			op = trans.getRASubSelect(arg0);
		}
		@Override
		public void visit(ExpressionList arg0) {
			isEL = true;
			el = arg0;
		}
		public Operation getPlan(){
			if(isEL){
				OpSelectCondition op = new OpSelectCondition();
				op.setExpressionList(el);
				return op;
			}
			else{
				return this.op;
			}
			
		}
		private Operation getSubSelect(){
			return op;
		}
		private ExpressionList getExpressionList(){
			return el;
		}
		
	}
	@Override
	public void visit(Insert arg0) {
		// TODO Auto-generated method stub
		setInsertItems(arg0);
		insertTableName = arg0.getTable().getName();
	}
	public String getInsertTableName(){
		return insertTableName;
	}
	public void setInsertItems(Insert arg0){
		List<Column> cols = arg0.getColumns();
		Iterator itrc = cols.iterator();
		ItemsList itms = arg0.getItemsList();
		MyItemsListVisitor mlv = new MyItemsListVisitor();
		itms.accept(mlv);
		ExpressionList el = mlv.getExpressionList();
		if (el != null){
			Iterator itri = el.getExpressions().iterator();
			insertItems = new LinkedHashMap<String, Expression>();
			while(itrc.hasNext() && itri.hasNext()){
				Column c = (Column) itrc.next();
				Expression e = (Expression) itri.next();
				insertItems.put(c.getColumnName(), e);
			}
		}
	}
	public HashMap<String, Expression> getInsertItems(){
		return insertItems;
	}

	@Override
	public void visit(Replace arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Drop arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Truncate arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(CreateTable arg0) {
		// TODO Auto-generated method stub
		//System.out.println(arg0.toString());
		if(tables == null){
			tables = new HashMap<String, List>();
		}
		// put the schema ( Table) into hashmap to create the links between table name and schema.
		tables.put(arg0.getTable().getName(), arg0.getColumnDefinitions());	
	}
	
	public HashMap<String, List> getTables() {
		return tables;
	}
	
	public Operation getOpRoot() {
		return this.root;
	}

}
