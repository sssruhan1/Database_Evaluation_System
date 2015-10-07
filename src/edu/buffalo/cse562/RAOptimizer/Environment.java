package edu.buffalo.cse562.RAOptimizer;

import java.util.ArrayList;
import java.util.HashMap;

import edu.buffalo.cse562.sql2RA.Operation;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;

public class Environment implements RAVisitor {
	HashMap<EqualsTo, Integer> equiJoin;
	HashMap<String, ArrayList<BinaryExpression>> preSelect;
	HashMap<Expression, Integer> otherConditions;
	
	private OpPlusInterface root;
	private boolean ran = false;
	
	public Environment(OpPlusInterface _root){
		root = _root;
		equiJoin = new HashMap<EqualsTo, Integer>();
		preSelect = new HashMap<String, ArrayList<BinaryExpression>>();
		otherConditions = new HashMap<Expression, Integer>();
	}
	
	private void run(){
		if(ran){
			return;
		}
		else{
			ran = true;
			root.accept(this);
		}
	}
	
	public HashMap<EqualsTo, Integer> getEquiJoin(){
		run();
		return equiJoin;
	}
	
	public HashMap<String, ArrayList<BinaryExpression>> getPreSelect(){
		run();
		return preSelect;
	}
	public HashMap<Expression, Integer> getOtherCondition(){
		run();
		return otherConditions;
	}

	
	@Override
	public void visit(Aggregate agg) {
		// TODO Auto-generated method stub
		if(root.left != null)
			root.left.accept(this);
		if(root.right != null)
			root.right.accept(this);

	}

	@Override
	public void visit(Cross cross) {
		// TODO Auto-generated method stub
		if(root.left != null)
			root.left.accept(this);
		if(root.right != null)
			root.right.accept(this);
	}

	@Override
	public void visit(Distinct distinct) {
		// TODO Auto-generated method stub
		if(root.left != null)
			root.left.accept(this);
		if(root.right != null)
			root.right.accept(this);

	}

	@Override
	public void visit(GroupBy groupby) {
		// TODO Auto-generated method stub
		if(root.left != null)
			root.left.accept(this);
		if(root.right != null)
			root.right.accept(this);

	}

	@Override
	public void visit(Having having) {
		// TODO Auto-generated method stub
		if(root.left != null)
			root.left.accept(this);
		if(root.right != null)
			root.right.accept(this);

	}

	@Override
	public void visit(Join join) {
		// TODO Auto-generated method stub
		if(root.left != null)
			root.left.accept(this);
		if(root.right != null)
			root.right.accept(this);

	}

	@Override
	public void visit(Limit limit) {
		// TODO Auto-generated method stub
		if(root.left != null)
			root.left.accept(this);
		if(root.right != null)
			root.right.accept(this);

	}

	@Override
	public void visit(OrderBy orderby) {
		// TODO Auto-generated method stub
		if(root.left != null)
			root.left.accept(this);
		if(root.right != null)
			root.right.accept(this);

	}

	public void equalsToProc(EqualsTo r){
		
		String tn = getExclusiveEquiTableName(r);
		if( tn == null){ // EquiJoin
			equiJoin.put((EqualsTo) r, 1);
		}
		else{
			
			ArrayList<BinaryExpression> al = preSelect.get(tn);
			if (al == null){
				al = new ArrayList<BinaryExpression>();
				al.add(r);
				preSelect.put(tn, al);
			}
			else{
				al.add(r);
			}
		}
	}
	
	public String getExclusiveEquiTableName(BinaryExpression e){
		Expression l = e.getLeftExpression();
		Expression r =  e.getRightExpression();
		
		Boolean lIsTable = l instanceof Column;//? Table? or Column
		Boolean rIsTable = r instanceof Column;
		
		if ( lIsTable && rIsTable ){
			String tableName = lIsTable? ((Column)l).getTable().getAlias(): ((Column)r).getTable().getAlias();
			return tableName;
		}
		else{
			return null;
		}
	}
	public void orProc(OrExpression e){
		Expression r = e.getRightExpression();
		String tn = null;
		if( r instanceof EqualsTo){
			//assuming left and right expression of OrExpression should constraint the same attribute
			tn = getExclusiveEquiTableName((BinaryExpression) r);
			if( tn != null){
				ArrayList<BinaryExpression> al = preSelect.get(tn);
				if (al == null){
					al = new ArrayList<BinaryExpression>();
					al.add(e);
					preSelect.put(tn, al);
					return;
				}
				else{
					al.add(e);
					return;
				}
			}
		}
	}
	
	@Override
	public void visit(SelectCondition selectcondition) {
		Expression e = selectcondition.getOperation().getExpression();
		if( e instanceof AndExpression){
			BinaryExpression l = (BinaryExpression)e;
			Expression r = l.getRightExpression();
			while( true ){
				if(r instanceof EqualsTo){
					equalsToProc((EqualsTo) r);
				}
				else if(r instanceof OrExpression){
					orProc((OrExpression)r);
				}
				
				if( l.getLeftExpression() instanceof AndExpression ){
					l = (AndExpression) l.getLeftExpression();
					r = l.getRightExpression();
				}
				else{
					if( l instanceof EqualsTo){
						equalsToProc((EqualsTo)l);
					}
					else if (l instanceof OrExpression){
						orProc((OrExpression)l);
					}
					break;
				}
			}
		}
		else if ( e instanceof OrExpression){
			orProc((OrExpression)e);
		}
		else if( e instanceof EqualsTo){
			equalsToProc((EqualsTo)e);
		}
		
		if(root.left != null)
			root.left.accept(this);
		if(root.right != null)
			root.right.accept(this);
	}

	@Override
	public void visit(Table table) {
		// TODO Auto-generated method stub
		if(root.left != null)
			root.left.accept(this);
		if(root.right != null)
			root.right.accept(this);

	}

	@Override
	public void visit(Target target) {
		// TODO Auto-generated method stub
		if(root.left != null)
			root.left.accept(this);
		if(root.right != null)
			root.right.accept(this);

	}

	@Override
	public void visit(Union union) {
		// TODO Auto-generated method stub
		if(root.left != null)
			root.left.accept(this);
		if(root.right != null)
			root.right.accept(this);

	}

}
