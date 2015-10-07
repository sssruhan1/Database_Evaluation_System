package edu.buffalo.cse562.sql2RA;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import edu.buffalo.cse562.configurations.GlobalConfiguration;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

public class Sql2RA implements SelectVisitor, FromItemVisitor,
ExpressionVisitor, ItemsListVisitor, SelectItemVisitor {
	Operation currentNode; // for tree building
	OpTarget currentSelect; // for adding target items to the current select (inner most select)
	SelectExpressionItem currentSelectItem; // for getting the most current alias
	int subselectflag = 0;
	static String currentSubSelectAlias = new String();
	BinaryExpression currentBinary = null;
	int aggNum = 0 ;
	Expression left, right;
	//public ArrayList<Table> sortedTables;
	
	// table Alias as key, OpTable obj as value, which contains
	// size information, which will be used to sort.
	public static HashMap<String, OpTable> allTables;  
	
	private class ExpressionLR{
		private Expression l;
		private Expression r;
		private void setL(Expression _l){
			this.l = _l;
		}
		private void setR(Expression _r){
			this.r = _r;
		}
		private Expression getl(){
			return l;
		}
		private Expression getr(){
			return r;
		}
	}

	public Operation getRA(Select select){
		//sortedTables = new ArrayList<Table>();
		allTables = new HashMap<String, OpTable>();
		select.getSelectBody().accept(this);
		return currentNode.getRoot();
	}
	
	public Operation getRASubSelect(SubSelect ss){
		allTables = new HashMap<String, OpTable>();
		ss.getSelectBody().accept(this);
		return currentNode.getRoot();
	}


	@Override
	public void visit(ExpressionList arg0) {
		List<Expression> expressionList = arg0.getExpressions();
		for(Expression exp: expressionList){
			exp.accept(this);
		}

	}

	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Function arg0) {
		//TODO 
		OpAggregate agg = new OpAggregate();
		if(currentSelectItem.getAlias() == null){
			String alias = "AGG"+ Integer.toString(++aggNum);
			agg.setAlias(alias);
		}
		else{
			agg.setAlias(currentSelectItem.getAlias());
		}
		agg.setFunction(arg0);
		if(currentNode != null){
			agg.setParent(currentNode);
		}
		currentNode = agg;
		SelectExpressionItem e = new SelectExpressionItem();
		Column c = new Column();
		c.setColumnName(agg.getAlias());
		Table t = new Table();
		t.setName(agg.getAlias());
		c.setTable(t);
		e.setExpression(c);
		currentSelectItem = e;
	}

	//TODO
	@Override
	public void visit(InverseExpression arg0) {
		arg0.getExpression().accept(this);
	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DoubleValue arg0) {

	}

	@Override
	public void visit(LongValue arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Parenthesis arg0) {
		arg0.getExpression().accept(this);
	}

	@Override
	public void visit(StringValue arg0) {

	}

	public void setLeftRightForSelect(BinaryExpression arg0){
		SelectItem prev = currentSelectItem;
		arg0.getLeftExpression().accept(this);
		if(prev != null && prev.equals(currentSelectItem)){ // left expression is basic expression
			left = arg0.getLeftExpression();
		}
		else{ // left expression has an alias (e.g. AGG )
			left = new StringValue(currentSelectItem.getAlias());
		}
		prev = currentSelectItem;
		arg0.getRightExpression().accept(this);
		if(prev != null && prev.equals(currentSelectItem)){ // right expression is basic expression
			right = arg0.getRightExpression();
		}
		else{ // left expression has an alias (e.g. AGG )
			right = new StringValue(currentSelectItem.getAlias());
		}
	}
	public void setLeft(BinaryExpression e, Expression l){
		e.setLeftExpression(l);
	}
	public void setRight(BinaryExpression e, Expression r){
		e.setRightExpression(r);
	}

	@Override
	public void visit(Addition arg0) {

		if(currentSelectItem != null){
			Addition e = new Addition();
			visitBinaryExpressionForSelect(arg0, e);
		}
	}


	@Override
	public void visit(Division arg0) {

		if(currentSelectItem != null){
			Division e = new Division();
			visitBinaryExpressionForSelect(arg0, e);

		}
	}

	@Override
	public void visit(Multiplication arg0) {
		if(currentSelectItem != null){
			Multiplication e = new Multiplication();
			visitBinaryExpressionForSelect(arg0, e);
		}
	}

	@Override
	public void visit(Subtraction arg0) {
		if(currentSelectItem != null){
			Subtraction e = new Subtraction();
			visitBinaryExpressionForSelect(arg0, e);
		}
	}
	
	//binary expression shouln't be treated as aggregation
	public void visitBinaryExpressionForSelect(BinaryExpression arg0, BinaryExpression n){
		//setLeftRightForSelect(arg0);
		//setLeft(n, left);
		//setRight(n, right);
	}

	
	
//	// the following visitBin.. function is to treat binary expression as agg.
//	public void visitBinaryExpressionForSelect(BinaryExpression arg0, BinaryExpression n){
//		setLeftRightForSelect(arg0);
//		OpAggregate agg = new OpAggregate();
//		agg.setAlias(currentSelectItem.getAlias());
//		setLeft(n, left);
//		setRight(n, right);
//		agg.setExpression(n);
//		if(currentNode != null){
//			agg.setParent(currentNode);
//		}
//		currentNode = agg;
//		SelectExpressionItem s = new SelectExpressionItem();
//		Column c = new Column();
//		c.setColumnName(agg.getAlias());
//		Table t = new Table();
//		t.setName(agg.getAlias());
//		c.setTable(t);
//		s.setExpression(c);
//		currentSelectItem = s;
//	}
	
	@Override
	public void visit(AndExpression arg0) {

	if(currentSelectItem != null){
			//situation in Select Item clause
			setLeftRightForSelect(arg0);
			AndExpression e = new AndExpression(left, right);
			currentNode.setExpression(e);
		}
		else{ //situation in where clause
			Operation whereClause = currentNode; // keep track of where node
			AndExpression l = arg0;
			Expression c = null;
			Expression r = l.getRightExpression();
			while( true ){
				r.accept(this);
				
				if(c == null)
					c = currentBinary;
				else
					c = new AndExpression(c, currentBinary);
				
				if( l.getLeftExpression() instanceof AndExpression){
					l = (AndExpression) l.getLeftExpression();
					r = l.getRightExpression();
				}
				else
					break;
			}
			l.getLeftExpression().accept(this);
			c = new AndExpression(c, currentBinary);
			currentBinary = (BinaryExpression) c;
			whereClause.setExpression(c);
		}
	}

	@Override
	public void visit(OrExpression arg0) {
		if(currentSelectItem != null){
			//situation in Select Item clause
			setLeftRightForSelect(arg0);
			OrExpression e = new OrExpression(left, right);
			currentNode.setExpression(e);
		}
		else{ //situation in where clause
			Operation whereClause = currentNode; // keep track of where node
			OrExpression l = arg0;
			Expression c = null;
			Expression r = l.getRightExpression();
			while( true ){
				r.accept(this);
				if(c == null)
					c = currentBinary;
				else
					c = new OrExpression(c, currentBinary);
				
				if( l.getLeftExpression() instanceof OrExpression){
					l = (OrExpression) l.getLeftExpression();
					r = l.getRightExpression();
				}
				else
					break;
			}
			l.getLeftExpression().accept(this);
			c = new OrExpression(c, currentBinary);
			currentBinary = (BinaryExpression) c;
			whereClause.setExpression(c);
		}

	}

	//TODO ...DON'T KNOW BETWEEN...
	@Override
	public void visit(Between arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getBetweenExpressionStart().accept(this);
		arg0.getBetweenExpressionEnd().accept(this);
	}
	
	


	public ExpressionLR setLeftRight(BinaryExpression arg0){
		ExpressionLR lr = new ExpressionLR();
		
		if(arg0.getLeftExpression() instanceof SubSelect){
			arg0.getLeftExpression().accept(this);
			Column c = new Column();
			int idx = currentSubSelectAlias.indexOf('.');
			String tn = currentSubSelectAlias.substring(0, idx);
			c.setColumnName(tn);
			Table t = new Table();
			t.setName(tn);
			c.setTable(t);
			lr.setL(c);
		}
		else{
			lr.setL(arg0.getLeftExpression());
		}
		
		if(arg0.getRightExpression() instanceof SubSelect){
			arg0.getRightExpression().accept(this);
			Column c = new Column();
			int idx = currentSubSelectAlias.indexOf('.');
			String tn = currentSubSelectAlias.substring(0, idx);
			c.setColumnName(tn);
			Table t = new Table();
			t.setName(tn);
			c.setTable(t);
			lr.setR(c);
		}
		else{
			lr.setR(arg0.getRightExpression());
		}
		return lr;
	}
	@Override
	public void visit(EqualsTo arg0) {

		if(currentSelectItem != null){
			setLeftRightForSelect(arg0);
			EqualsTo e = new EqualsTo();
			e.setLeftExpression(left);
			e.setRightExpression(right);
			currentNode.setExpression(e);
		}
		else{
			ExpressionLR lr= setLeftRight(arg0);
			currentBinary = new EqualsTo();
			currentBinary.setLeftExpression(lr.getl());
			currentBinary.setRightExpression(lr.getr());
			currentNode.setExpression(currentBinary);
		}
	}

	@Override
	public void visit(GreaterThan arg0) {
		if(currentSelectItem != null){
			setLeftRightForSelect(arg0);
			GreaterThan e = new GreaterThan();
			e.setLeftExpression(left);
			e.setRightExpression(right);
			currentNode.setExpression(e);
		}
		else{
			ExpressionLR lr= setLeftRight(arg0);
			currentBinary = new GreaterThan();
			currentBinary.setLeftExpression(lr.getl());
			currentBinary.setRightExpression(lr.getr());
			currentNode.setExpression(currentBinary);
		}
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		if(currentSelectItem != null){
			setLeftRightForSelect(arg0);
			GreaterThanEquals e = new GreaterThanEquals();
			e.setLeftExpression(left);
			e.setRightExpression(right);
			currentNode.setExpression(e);
		}
		else{
			ExpressionLR lr= setLeftRight(arg0);
			currentBinary = new GreaterThanEquals();
			currentBinary.setLeftExpression(lr.getl());
			currentBinary.setRightExpression(lr.getr());
			currentNode.setExpression(currentBinary);
		}
	}

	//TODO 
	@Override
	public void visit(InExpression arg0) {
		currentNode.setExpression(arg0);
	}

	@Override
	public void visit(IsNullExpression arg0) {
		//TODO
		currentNode.setExpression(arg0);
	}

	//TODO...
	@Override
	public void visit(LikeExpression arg0) {
		if(currentSelectItem != null){
			setLeftRightForSelect(arg0);
			LikeExpression e = new LikeExpression();
			e.setLeftExpression(left);
			e.setRightExpression(right);
			currentNode.setExpression(e);
		}
		else{
			ExpressionLR lr= setLeftRight(arg0);
			currentBinary = new LikeExpression();
			currentBinary.setLeftExpression(lr.getl());
			currentBinary.setRightExpression(lr.getr());
			currentNode.setExpression(currentBinary);
		}
	}

	@Override
	public void visit(MinorThan arg0) {
		if(currentSelectItem != null){
			setLeftRightForSelect(arg0);
			MinorThan e = new MinorThan();
			e.setLeftExpression(left);
			e.setRightExpression(right);
			currentNode.setExpression(e);
		}
		else{
			ExpressionLR lr= setLeftRight(arg0);
			currentBinary = new MinorThan();
			currentBinary.setLeftExpression(lr.getl());
			currentBinary.setRightExpression(lr.getr());
			currentNode.setExpression(currentBinary);
		}

	}

	@Override
	public void visit(MinorThanEquals arg0) {
		if(currentSelectItem != null){
			setLeftRightForSelect(arg0);
			MinorThanEquals e = new MinorThanEquals();
			e.setLeftExpression(left);
			e.setRightExpression(right);
			currentNode.setExpression(e);
		}
		else{
			ExpressionLR lr= setLeftRight(arg0);
			currentBinary = new MinorThanEquals();
			currentBinary.setLeftExpression(lr.getl());
			currentBinary.setRightExpression(lr.getr());
			currentNode.setExpression(currentBinary);
		}

	}

	@Override
	public void visit(NotEqualsTo arg0) {
		if(currentSelectItem != null){
			setLeftRightForSelect(arg0);
			NotEqualsTo e = new NotEqualsTo();
			e.setLeftExpression(left);
			e.setRightExpression(right);
			currentNode.setExpression(e);
		}
		else{
			ExpressionLR lr= setLeftRight(arg0);
			currentBinary = new NotEqualsTo();
			currentBinary.setLeftExpression(lr.getl());
			currentBinary.setRightExpression(lr.getr());
			currentNode.setExpression(currentBinary);
		}
	}

	//TODO
	@Override
	public void visit(Column arg0) {
	}

	//TODO
	@Override
	public void visit(CaseExpression arg0) {

	}

	//TODO
	@Override
	public void visit(WhenClause arg0) {

	}

	@Override
	public void visit(ExistsExpression arg0) {
		SelectExpressionItem prev = currentSelectItem;
		arg0.getRightExpression().accept(this);
		if(prev.equals(currentSelectItem)){ // basic expression
			right = arg0.getRightExpression();
		}
		else{
			right= new StringValue(currentSelectItem.getAlias());
		}
		ExistsExpression e = new ExistsExpression();
		e.setRightExpression(right);
		currentNode.setExpression(e);
	}

	@Override
	public void visit(AllComparisonExpression arg0) {

		arg0.getSubSelect().getSelectBody().accept(this);

	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		arg0.getSubSelect().getSelectBody().accept(this);

	}

	@Override
	public void visit(Concat arg0) {
		//TODO
	}

	@Override
	public void visit(Matches arg0) {
		//TODO

	}

	@Override
	public void visit(BitwiseAnd arg0) {
		//TODO

	}

	@Override
	public void visit(BitwiseOr arg0) {
		//TODO
	}

	@Override
	public void visit(BitwiseXor arg0) {
		//TODO
	}

	@Override
	public void visit(Table arg0) {
		OpTable t = new OpTable();
		if(arg0.getAlias() != null)
			t.setAlias(arg0.getAlias());
		else{
			t.setAlias(arg0.getName());
		}
		
		if (arg0.getName() != null) {
			String tn = arg0.getName();
			t.setName(tn);
			GlobalConfiguration.addTableAlias(t.getAlias(), t.getName());
		}

		//  adding to allTables for sorting 
		//  all the tables inside doesn't have any node infor. such as parent
		//  or child node.
		allTables.put(t.getAlias(), t);
		if(currentNode != null)
			t.setParent(currentNode);
	}

	@Override
	public void visit(SubSelect arg0) {
		subselectflag++;
		arg0.getSelectBody().accept(this);
		if(arg0.getAlias() != null){
			currentSubSelectAlias = arg0.getAlias();
		}
		subselectflag--;
	}

	@Override
	public void visit(SubJoin arg0) {
		arg0.getLeft().accept(this);
		arg0.getJoin().getRightItem().accept(this);
	}


	@Override
	public void visit(PlainSelect arg0) {

		if(arg0.getLimit() != null){
			OpLimit limit = new OpLimit();
			limit.setLimit(arg0.getLimit());
			if(currentNode != null){
				limit.setParent(currentNode);
			}
			currentNode = limit;
		}

		if(arg0.getDistinct() != null){
			OpDistinct distinct = new OpDistinct();
			distinct.setDistinct(arg0.getDistinct());
			if(currentNode != null){
				distinct.setParent(currentNode);
			}
			currentNode = distinct;
		}

		
		
		if(arg0.getHaving() != null){
			OpHaving having = new OpHaving();
			having.setExpression(arg0.getHaving());
			if(currentNode != null){
				having.setParent(currentNode);
			}
			currentNode = having;
		}

		if(arg0.getOrderByElements() != null){
			OpOrderBy orderby = new OpOrderBy();
			orderby.setOrderByList(arg0.getOrderByElements());
			if(currentNode != null){
				orderby.setParent(currentNode);
			}
			currentNode = orderby;
		}
		
		if(arg0.getGroupByColumnReferences() != null){
			OpGroupBy groupby = new OpGroupBy();
			groupby.setGroupByColumnList(arg0.getGroupByColumnReferences());
			if(currentNode != null){
				groupby.setParent(currentNode);
			}
			currentNode = groupby;
		}

		//select target from ...
		OpTarget target = new OpTarget();
		if(currentNode != null){
			target.setParent(currentNode);
		}
		currentNode = target;
		//set target list
		//aggregation...
		currentSelect = target;
		List<SelectItem> selectItemList = (List<SelectItem>) arg0.getSelectItems();
		for(SelectItem i: selectItemList){
			i.accept(this);
			currentSelect.setExpression(currentSelectItem);
		}
		currentSelect = null; 
		if(subselectflag > 0){
			currentSubSelectAlias = currentSelectItem.toString();
		}
		currentSelectItem = null;

		// where clause
		if(arg0.getWhere() != null){
			OpSelectCondition condition = new OpSelectCondition();
			if(currentNode != null){
				condition.setParent(currentNode);
			}
			currentNode = condition;
			arg0.getWhere().accept(this);
		}
		
// ======
//		this part is for sorting tables
//		which is now moved to PlanGenerator
// ======
		
//		sortedTables.clear();
//		if (arg0.getJoins() != null) {
//			List<Join> joins = arg0.getJoins();
//			if( arg0.getFromItem() instanceof Table){
//				sortedTables.add((Table) arg0.getFromItem());
//				for (int i=joins.size()-1; i>=0; i--) {
//					Join join = joins.get(i);
//					if(join.getRightItem() instanceof Table){
//						Table t = (Table)join.getRightItem();
//						//sortedTables.add(t);
//					}
//				}
//				sortTables(sortedTables);
//				for( int i = 0 ; i < sortedTables.size()-1; i++){
//					Table t = sortedTables.get(i);
//					OpCross c = new OpCross();
//					if(currentNode != null){
//						c.setParent(currentNode);
//					}
//					currentNode = c;
//					t.accept(this);
//				}
//				sortedTables.get(sortedTables.size()-1).accept(this);
//				
//			}
//			else{
//				for (int i=joins.size()-1; i>=0; i--) {
//					Join join = joins.get(i);
//					if(join.getRightItem() instanceof Table){
//						Table t = (Table)join.getRightItem();
//						sortedTables.add(t);
//					}
//				}
//				sortTables(sortedTables);
//				for( int i = 0 ; i < sortedTables.size()-1; i++){
//					Table t = sortedTables.get(i);
//					OpCross c = new OpCross();
//					if(currentNode != null){
//						c.setParent(currentNode);
//					}
//					currentNode = c;
//					t.accept(this);
//				}
//				sortedTables.get(sortedTables.size()-1).accept(this);
//				arg0.getFromItem().accept(this);
//			}
//		}
//		else{
//			arg0.getFromItem().accept(this);
//		}
		
		
		if (arg0.getJoins() != null) {
			List<Join> joins = arg0.getJoins();	
			

//			for (Join join: joins) {
//				OpCross c = new OpCross();
//				if(currentNode != null){
//					c.setParent(currentNode);
//				}
//				currentNode = c;
//				join.getRightItem().accept(this);
//			}
			
			for (int i=joins.size()-1; i>=0; i--) {
				Join join = joins.get(i);
				OpCross c = new OpCross();
				if(currentNode != null){
					c.setParent(currentNode);
				}
				currentNode = c;
				join.getRightItem().accept(this);
			}
		}

		arg0.getFromItem().accept(this);
	}

	@Override
	public void visit(Union arg0) {
		List<PlainSelect> plainSelects = arg0.getPlainSelects();
		for( PlainSelect selectStmt: plainSelects){
			OpUnion union = new OpUnion();
			if(currentNode != null){
				union.setParent(currentNode);
			}
			currentNode = union;
			visit(selectStmt);
		}
	}

	//TODO
	@Override
	public void visit(AllColumns arg0) {
		arg0.accept(this);
	}
	// TODO
	@Override
	public void visit(AllTableColumns arg0) {
		arg0.getTable().accept(this);
	}

	//TODO , MAKE USE OF ALIAS
	@Override
	public void visit(SelectExpressionItem arg0) {
		currentSelectItem = arg0;
		currentSelectItem.setAlias(arg0.getAlias());
		arg0.getExpression().accept(this);
		
	}
//	public void sortTables(ArrayList<Table> tables){
//		if(tables != null){
//			for(int i = 0; i < tables.size(); i++){
//				Table t1 = tables.get(i);
//				String tn1 = t1.getName();
//				int i1 = checkTable(tn1);
//				allTableNames.add(tn1);
//				for(int j = i+1; j < tables.size(); j++){
//					Table t2 = tables.get(j);
//					String tn2 = t2.getName();
//					int i2 = checkTable(tn2);
//					if( i1 < i2){
//						tables.set(i, t2);
//						tables.set(j, t1);
//					}
//				}
//			}
//		}
//	}
//	
//	public int checkTable(String tn){
//		if(tn != null){
//			for(int i = 0; i < tableNames.size(); i++){
//				if(tn.toUpperCase().compareTo(tableNames.get(i)) == 0)
//					return i;
//			}
//			return Integer.MAX_VALUE;
//		}
//		return Integer.MAX_VALUE;
//	}
//
	
	public static HashMap<String, OpTable> getAllTables(){
		return allTables;
	}
	
}
