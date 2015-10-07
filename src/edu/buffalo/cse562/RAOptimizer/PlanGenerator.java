package edu.buffalo.cse562.RAOptimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.configurations.GlobalConfiguration;
import edu.buffalo.cse562.sql2RA.*;

public class PlanGenerator implements RAVisitor {
	private boolean debug = true;
	private Operation dumb;
	private OpPlusInterface opiDumbRoot;
	private HashMap<String, HashMap<String, ArrayList<EqualsTo>>> equiJoinConditions;
	private HashMap<String, ArrayList<BinaryExpression>> preSelect;
	private Expression leftovers;
	private Operation currentNode;
	//private OpJoin lastNode;// this is not used. but it needs to cleared out systematically, which I really sick of doing
	private boolean reOrganized = false;

	private class Plan{
		public Operation root;

	}

	private ArrayList<Plan> plans;

	public PlanGenerator(Operation op){
		plans = new ArrayList<Plan>();
		dumb = op;
		addPlan(dumb);
		equiJoinConditions = new HashMap<String, HashMap<String, ArrayList<EqualsTo>>>();
		preSelect = new HashMap<String, ArrayList<BinaryExpression>>();

	}

	public void addPlan(Operation root){
		Plan p = new Plan();
		p.root = root;
		plans.add(p);
		CostEstimator ce = new CostEstimator();
		int cost = ce.getCost(p.root);


	}

	public Operation getOptimal(){


		if( true){
			Porter p = new Porter();
			opiDumbRoot = p.opToVPC(dumb);
			opiDumbRoot.accept(this);

			Operation or = currentNode.getRoot();
			addPlan(or);
			// TODO
			if (true) {
				for (int i = 0; i < plans.size(); i++){
					Operation op = plans.get(i).root;
					if (GlobalConfiguration.debug) {
						RADebug debuggy = new RADebug("RA" + Integer.toString(i) + ".png");
						debuggy.debugRATree(op);
						//op.print();
					}
				}
				return plans.get(1).root;
			}			
		}
		else{

			if (debug) {
				for (int i = 0; i < plans.size(); i++){
					Operation op = plans.get(i).root;
					if (GlobalConfiguration.debug) {
						RADebug debuggy = new RADebug("RA" + Integer.toString(i) + ".png");
						debuggy.debugRATree(op);
						//op.print();
					}
				}
			}
			// TODO
			return null;
		}

		return null;

	}


	public void copyAndContinue(OpPlusInterface opi){
		currentNode.setAlias(opi.op.getAlias());
		currentNode.setExpression(opi.op.getExpression());
		if(opi.getRight()!= null)
			opi.getRight().accept(this);
		if(opi.getLeft() != null)
			opi.getLeft().accept(this);
	}

	@Override
	public void visit(Aggregate agg) {
		if(currentNode == null){
			currentNode = new OpAggregate();
		}
		else{
			OpAggregate op = new OpAggregate();
			op.setParent(currentNode);
			currentNode = op;
		}
		copyAndContinue(agg);
	}

	public Expression getExpression(String tn){
		Expression e = null;
		if(equiJoinConditions.containsKey(tn)){
			HashMap<String, ArrayList<EqualsTo>> sub = equiJoinConditions.get(tn);
			Collection<String> keyset = sub.keySet();
			Iterator<String> itr = keyset.iterator();
			String tn2 = null;
			String prev = null;
			while(itr.hasNext()){
				tn2 = itr.next();
				ArrayList<EqualsTo> vals = sub.get(tn2);
				for(EqualsTo et: vals){
					if( e == null){
						e = et;
					}
					else{
						e = new AndExpression(e, et);
					}
				}
				if( prev != null){
					removeFromHashHash(tn, prev);
					prev = tn2;
				}
				else{
					prev = tn2;
				}
			}
			if( prev != null){
				removeFromHashHash(tn, prev);
				prev = tn2;
			}
		}
		return e;
	}

	public Expression isJoinable(OpTable t){
		Expression e = null;
		String tn = t.getAlias();
		if(equiJoinConditions.containsKey(tn)){
			e = getExpression(tn);
		}
		return e;
	}
	public void setJoinAnnotation(Expression e, Operation op, boolean isTwoTable, OpTable t){
		if ( e instanceof EqualsTo){ // these if statements are just for those "in case"s...
			Expression lhs = ((EqualsTo)e).getLeftExpression();
			Expression rhs = ((EqualsTo)e).getRightExpression();
			if( lhs instanceof Column && rhs instanceof Column){ // again..
				OpTable t1 = new OpTable();
				String tn1 = ((Column)lhs).getTable().getName();
				t1.setName(tn1);
				OpTable t2 = new OpTable();
				String tn2 = ((Column)rhs).getTable().getName();
				t2.setName(tn2);
				String idx1 = isIndex(((Column)lhs).getColumnName(), t1);
				String idx2 =  isIndex(((Column)rhs).getColumnName(), t2);
				String cn1 = ((Column)lhs).getColumnName();
				String cn2 = ((Column)rhs).getColumnName();

				if ( idx1 != null && idx2 != null){
					Annotation a = new Annotation();
					a.exp = e;
					a.idx = idx1;
					a.idx2 = idx2;
					a.idxType = "HashJoin";
					a.tableName1=tn1;
					a.tableName2 = tn2;
					if( !isTwoTable){
						if( t.getName().compareTo(tn1) == 0){
							a.isLeft = true;
						}
						if (t.getName().compareTo(tn2) == 0){
							a.isRight = true;
						}
					}
					op.setAnnotation(a);
				}else if(idx1!= null && idx2 == null){
					Annotation a = new Annotation();
					a.exp = e;
					a.idx = idx1;
					a.idx2 = cn2;
					a.idxType = "IndexNestedLoopJoin";
					a.tableName1 = tn1;
					a.tableName2 = tn2;
					a.isLeft = true;
					if( !isTwoTable){
						if( t.getName().compareTo(tn1) == 0){
							a.filterInRight = true;
						}
						else if (t.getName().compareTo(tn2) == 0){
							a.filterInLeft = true;
						}
					}
					op.setAnnotation(a);
				}else if(idx2 != null && idx1 == null){
					Annotation a = new Annotation();
					a.exp = e;
					a.idx = idx2;
					a.idx2 = cn1;
					a.idxType = "IndexNestedLoopJoin";
					a.tableName1 = tn2;
					a.tableName2 = tn1;
					a.isLeft = false;
					if( !isTwoTable){
						if( t.getName().compareTo(tn1) == 0){
							a.filterInRight = true;
						}
						else if (t.getName().compareTo(tn2) == 0){
							a.filterInLeft = true;
						}
					}
					op.setAnnotation(a);
				}
			}
		}
	}
	public void crossHelp(boolean isReturn, boolean isLast,boolean goLeft, OpTable t, Cross cross){
		Expression e = isJoinable(t);
		if( e != null){ // is joinable 
			OpJoin oj = new OpJoin();
			oj.setExpression(e);
			// AS FAR AS CHECKPOINT 3 IS CONCERNED, EQUI JOINS ONLY HAVE ONE EXPRESSION.
			// THIS IS A BAD ASSUMPTION. BUT ... YEAH.... YOU CAN'T BLAME ME FOR THIS. 
			// JUST TRYING MY BEST TO GET INTO THE BILL BOARD! IT'S MUCH GLAMOROUS THAN 
			// YOU'D EXPECT. 
			// SO I'M JUST GONNA ASSUME THAT IF LHS AND RHS BOTH HAVE INDEXES, THEN IT'S A 
			// HASH JOIN. 
			setJoinAnnotation(e, oj, false, t);
			Operation tish = insertPreSelection(t);

			if (oj.getAnnotation() != null){
				Annotation a = oj.getAnnotation();
				if(GlobalConfiguration.isPushSelect){
					if(tish.getOpName().compareTo("OpTable") != 0){ // there's preselect for table
						a.filterInRight = true;
					}
				}
				if( a.idxType.compareTo("HashJoin") == 0){
					IndexJoin ij = new IndexJoin();
					ij.setExpression(e);
					ij.setIndexNames(a.idx, a.idx2);
					ij.setTableNames(a.tableName1, a.tableName2);
					ij.setKid(tish);
					if(currentNode == null){
						currentNode = ij;
					}
					else{
						ij.setParent(currentNode);
					}
					currentNode = ij;
					if(!isReturn){
						if(goLeft){
							cross.getLeft().accept(this);
						}
						else{
							cross.getRight().accept(this);
						}
					}

					//					IndexFilterJoin ifj = new IndexFilterJoin();
					//					if(a.filterInLeft){ 
					//						ifj.filterInLeft = true;
					//					}
					//					if(a.filterInRight){
					//						ifj.filterInRight = true;
					//					}
					//					ifj.setExpression(e);
					//					ifj.setIndexNames(a.idx, a.idx2);
					//					ifj.setTableNames(a.tableName1, a.tableName2);
					//					ifj.setKid(tish);
					//					if(currentNode == null){
					//						currentNode = ifj;
					//					}
					//					else{
					//						ifj.setParent(currentNode);
					//					}
					//					currentNode = ifj;
					//					if(!isReturn){
					//						if(goLeft){
					//							cross.getLeft().accept(this);
					//						}
					//						else{
					//							cross.getRight().accept(this);
					//						}
					//					}
				}else if(a.idxType.compareTo("IndexNestedLoopJoin") == 0){
					IndexNestedLoopJoin inlj = new IndexNestedLoopJoin();
					inlj.isLeft = a.isLeft;
					inlj.setExpression(e);
					inlj.setColumnNames(a.idx, a.idx2);
					inlj.setTableNames(a.tableName1, a.tableName2);
					inlj.setKid(tish);
					if(currentNode == null){
						currentNode = inlj;
					}
					else{
						inlj.setParent(currentNode);
					}
					currentNode = inlj;
					if(!isReturn){
						if(goLeft){
							cross.getLeft().accept(this);
						}
						else{
							cross.getRight().accept(this);
						}
					}
					//					IndexFilterNestedLoopJoin ifnlj = new IndexFilterNestedLoopJoin();
					//					if(GlobalConfiguration.isPushSelect){
					//						if(tish.getOpName().compareTo("OpTable") != 0){ // there's preselect
					//							a.filterInRight = true;
					//						}
					//					}
					//					if(a.filterInLeft){
					//						ifnlj.filterInLeft = true;
					//					}
					//					if(a.filterInRight){
					//						ifnlj.filterInRight = true;
					//					}
					//					ifnlj.isLeft = a.isLeft;
					//					ifnlj.setExpression(e);
					//					ifnlj.setColumnNames(a.idx, a.idx2);
					//					ifnlj.setTableNames(a.tableName1, a.tableName2);
					//					ifnlj.setKid(tish);
					//					if(currentNode == null){
					//						currentNode = ifnlj;
					//					}
					//					else{
					//						ifnlj.setParent(currentNode);
					//					}
					//					currentNode = ifnlj;
					//					if(!isReturn){
					//						if(goLeft){
					//							cross.getLeft().accept(this);
					//						}
					//						else{
					//							cross.getRight().accept(this);
					//						}
					//					}
				}
			}
			else{
				oj.setKid(tish);

				if(currentNode == null){
					currentNode = oj;
				}
				else{
					oj.setParent(currentNode);
				}

				currentNode = oj;

				if(!isReturn){
					if(goLeft){
						cross.getLeft().accept(this);
					}
					else{
						cross.getRight().accept(this);
					}
				}
			}
		}
		else{ // just a cross
			OpCross oc = new OpCross();
			Operation tish = insertPreSelection(t);
			oc.setKid(tish);
			if( isLast){
				//oc.setKid(lastNode);
			}
			if(currentNode == null){
				currentNode = oc;
			}
			else{
				oc.setParent(currentNode);
			}
			if(isLast){
				//currentNode = lastNode;
			}
			else
				currentNode = oc;

			if(!isReturn){
				if(goLeft){
					cross.getLeft().accept(this);
				}
				else{
					cross.getRight().accept(this);
				}
			}
		}
	}

	//	public void crossHelp2(OpTable t1, OpTable t2){
	//		Expression e = isJoinable(t1);
	//		if( e != null){ // is joinable
	//			OpJoin oj = new OpJoin();
	//			oj.setExpression(e);
	//			Operation tish = insertPreSelection(t1);
	//			oj.setKid(tish);
	//			if(currentNode == null){
	//				currentNode = oj;
	//			}
	//			else{
	//				oj.setParent(currentNode);
	//			}
	//			currentNode = oj;
	//		}
	//		else{ // just a cross
	//			OpCross oc = new OpCross();
	//			Operation tish = insertPreSelection(t1);
	//			oc.setKid(tish);
	//			if(currentNode == null){
	//				currentNode = oc;
	//			}
	//			else{
	//				oc.setParent(currentNode);
	//			}
	//			currentNode = oc;
	//		}
	//		
	//		e = isJoinable(t2);
	//		if( e != null){ // is joinable
	//			OpJoin oj = new OpJoin();
	//			oj.setExpression(e);
	//			Operation tish = insertPreSelection(t2);
	//			oj.setKid(tish);
	//			if(currentNode == null){
	//				currentNode = oj;
	//			}
	//			else{
	//				oj.setParent(currentNode);
	//			}
	//			//currentNode = oj;
	//			//oj.setKid(lastNode);
	//			//currentNode = lastNode;
	//		}
	//		else{ // just a cross
	//			OpCross oc = new OpCross();
	//			Operation tish = insertPreSelection(t2);
	//			oc.setKid(tish);
	//			if(currentNode == null){
	//				currentNode = oc;
	//			}
	//			else{
	//				oc.setParent(currentNode);
	//			}
	//			currentNode = oc;
	//			//oc.setKid(lastNode);
	//			//currentNode = lastNode;
	//		}
	//	}

	//	@Override
	//	public void visit(Cross cross) {
	//		// TODO Auto-generated method stub
	//		// NEED TO CHANGE TO JOIN!
	//		OpCross op = (OpCross) cross.op;
	//		Operation r = op.getRight();
	//		Operation l = op.getLeft();
	//		
	//		//if r and l are all table
	//		if( r instanceof OpTable &&
	//				l instanceof OpTable){
	//			// check wether exists in lastNode
	//			OpTable t1 = (OpTable)r;
	//			OpTable t2 = (OpTable)l;
	//			String tn1 = t1.getAlias();
	//			String tn2 = t2.getAlias();
	//			boolean t1islast1 = (tn1.compareTo(last1) == 0);
	//			boolean t1islast2 = (tn1.compareTo(last2) == 0);
	//			boolean t1islast = t1islast1 || t1islast2;
	//			boolean t2islast1 = (tn2.compareTo(last1) == 0);
	//			boolean t2islast2 = (tn2.compareTo(last2) == 0);
	//			boolean t2islast = t2islast1 || t2islast2;
	//			
	//			if( t1islast && t2islast){
	//				lastNode.setParent(currentNode);
	//				currentNode = lastNode;
	//				return;
	//			}
	//			else if( t1islast){
	//				crossHelp(true, true, false, t2, cross);
	//			}
	//			else if( t2islast){
	//				crossHelp(true, true, false, t1, cross);
	//			}
	//			else{
	//				crossHelp2(t1, t2);
	//			}
	//		}
	//		//else if only r is table
	//		else if (r instanceof OpTable &&
	//			l instanceof OpCross){
	//			// check wether exists in lastNode
	//			OpTable t1 = (OpTable)r;
	//			String tn1 = t1.getAlias();
	//			boolean t1islast1 = (tn1.compareTo(last1) == 0);
	//			boolean t1islast2 = (tn1.compareTo(last2) == 0);
	//			boolean t1islast = t1islast1 || t1islast2;
	//			if(t1islast){
	//				cross.getLeft().accept(this);
	//			}
	//			else{
	//				crossHelp(false, false, true, t1, cross);
	//			}
	//		}
	//		//else if only l is table
	//		else if (r instanceof OpCross &&
	//				l instanceof OpTable){
	//				// check wether exists in lastNode
	//			OpTable t1 = (OpTable)l;
	//			String tn1 = t1.getAlias();
	//			boolean t1islast1 = (tn1.compareTo(last1) == 0);
	//			boolean t1islast2 = (tn1.compareTo(last2) == 0);
	//			boolean t1islast = t1islast1 || t1islast2;
	//			if(t1islast){
	//				cross.getRight().accept(this);
	//			}
	//			else{
	//				crossHelp(false, false, false, t1, cross);
	//			}
	//		}
	//		// child are all crosses?
	//		else{
	//			if(currentNode == null){
	//				currentNode = new OpCross();
	//			}
	//			else{
	//				OpCross op1 = new OpCross();
	//				op1.setParent(currentNode);
	//				currentNode = op1;
	//			}
	//			copyAndContinue(cross);
	//		}
	//	}

	public void crossHelp2(OpTable t1, OpTable t2){
		Expression e = isJoinable(t1);
		if( e != null){ // is joinable
			OpJoin oj = new OpJoin();
			oj.setExpression(e);
			setJoinAnnotation(e, oj, true, null);
			Operation tish = insertPreSelection(t1);
			Operation tish2 = insertPreSelection(t2);
			if(oj.getAnnotation() != null){
				Annotation a = oj.getAnnotation();
				if( a.idxType.compareTo("HashJoin") == 0){
					if ( GlobalConfiguration.isPushSelect){
						if (tish.getOpName().compareTo("OpTable") != 0){ // there's preselect
							a.filterInLeft = true;
						}
						if (tish2.getOpName().compareTo("OpTable") != 0 ){
							a.filterInRight = true;
						}
					}
					//					if( a.filterInLeft || a.filterInRight){
					//						IndexFilterJoin ifj = new IndexFilterJoin();
					//						ifj.setExpression(e);
					//						ifj.setIndexNames(a.idx, a.idx2);
					//						ifj.setTableNames(a.tableName1, a.tableName2);
					//						ifj.setKid(tish);
					//						if(a.filterInLeft){
					//							ifj.filterInLeft = true;
					//						}
					//						if (a.filterInRight){
					//							ifj.filterInRight = true;
					//						}
					//						
					//						if(currentNode == null){
					//							currentNode = ifj;
					//						}
					//						else{
					//							ifj.setParent(currentNode);
					//						}
					//						currentNode = ifj;
					//					}
					//					else{ // just index join, no filter whatsoever
					//						IndexJoin ij = new IndexJoin();
					//						ij.setExpression(e);
					//						ij.setIndexNames(a.idx, a.idx2);
					//						ij.setTableNames(a.tableName1, a.tableName2);
					//						ij.setKid(tish);
					//						if(currentNode == null){
					//							currentNode = ij;
					//						}
					//						else{
					//							ij.setParent(currentNode);
					//						}
					//						currentNode = ij;
					//					}
					IndexJoin ij = new IndexJoin();
					ij.setExpression(e);
					ij.setIndexNames(a.idx, a.idx2);
					ij.setTableNames(a.tableName1, a.tableName2);
					ij.setKid(tish);
					if(currentNode == null){
						currentNode = ij;
					}
					else{
						ij.setParent(currentNode);
					}
					currentNode = ij;
					
					
				}else if(a.idxType.compareTo("IndexNestedLoopJoin") == 0){
					if ( GlobalConfiguration.isPushSelect){
						if (tish.getOpName().compareTo("OpTable") != 0){ // there's preselect
							a.filterInLeft = true;
						}
						if (tish2.getOpName().compareTo("OpTable") != 0 ){
							a.filterInRight = true;
						}
					}
					//					if(a.filterInLeft || a.filterInRight){
					//						IndexFilterNestedLoopJoin ifnlj = new IndexFilterNestedLoopJoin();
					//						ifnlj.isLeft = a.isLeft;
					//						ifnlj.setExpression(e);
					//						ifnlj.setColumnNames(a.idx, a.idx2);
					//						ifnlj.setTableNames(a.tableName1, a.tableName2);
					//						ifnlj.setKid(tish);
					//						if(a.filterInLeft){
					//							ifnlj.filterInLeft = true;
					//						}
					//						if( a.filterInRight){
					//							ifnlj.filterInRight = true;
					//						}
					//						if(currentNode == null){
					//							currentNode = ifnlj;
					//						}
					//						else{
					//							ifnlj.setParent(currentNode);
					//						}
					//						currentNode = ifnlj;
					//					}
					//					else{ // just inlj, with no filter in two kids.
					//						IndexNestedLoopJoin inlj = new IndexNestedLoopJoin();
					//						inlj.isLeft = a.isLeft;
					//						inlj.setExpression(e);
					//						inlj.setColumnNames(a.idx, a.idx2);
					//						inlj.setTableNames(a.tableName1, a.tableName2);
					//						inlj.setKid(tish);
					//						if(currentNode == null){
					//							currentNode = inlj;
					//						}
					//						else{
					//							inlj.setParent(currentNode);
					//						}
					//						currentNode = inlj;
					//					}
					IndexNestedLoopJoin inlj = new IndexNestedLoopJoin();
					inlj.isLeft = a.isLeft;
					inlj.setExpression(e);
					inlj.setColumnNames(a.idx, a.idx2);
					inlj.setTableNames(a.tableName1, a.tableName2);
					inlj.setKid(tish);
					if(currentNode == null){
						currentNode = inlj;
					}
					else{
						inlj.setParent(currentNode);
					}
					currentNode = inlj;
				}
			}else{ // if there's no annotation (no index)
				oj.setKid(tish);
				if(currentNode == null){
					currentNode = oj;
				}
				else{
					oj.setParent(currentNode);
				}
				currentNode = oj;
			}
		}
		else{ // just a cross
			OpCross oc = new OpCross();
			Operation tish = insertPreSelection(t1);
			oc.setKid(tish);
			if(currentNode == null){
				currentNode = oc;
			}
			else{
				oc.setParent(currentNode);
			}
			currentNode = oc;
		}

		Operation tish = insertPreSelection(t2);
		currentNode.setKid(tish);
	}

	@Override
	public void visit(Cross cross) {

		Cross newCross = cross;
		// reOrganize the table order based on 
		// extracted selection conditions, i.e.
		// preSelections.
		if(!reOrganized){
			reOrganize(cross);
			Porter p = new Porter();
			newCross = (Cross) p.opToVPC(cross.op);
			reOrganized = true;
			OpPlusInterface parent = cross.getParent();
			parent.setLeft(null);
			parent.setRight(newCross);
		}

		// TODO Auto-generated method stub
		OpCross op = (OpCross) newCross.op;
		Operation r = op.getRight();
		Operation l = op.getLeft();

		//if r and l are all table
		if( r instanceof OpTable &&
				l instanceof OpTable){
			// check wether exists in lastNode
			OpTable t1 = (OpTable)r;
			OpTable t2 = (OpTable)l;
			crossHelp2(t1, t2);
		}
		//else if only r is table
		else if (r instanceof OpTable &&
				l instanceof OpCross){
			// check wether exists in lastNode
			OpTable t1 = (OpTable)r;
			crossHelp(false, false, true, t1, newCross);
		}
		//else if only l is table
		else if (r instanceof OpCross &&
				l instanceof OpTable){
			// check wether exists in lastNode
			OpTable t1 = (OpTable)l;
			crossHelp(false, false, false, t1, newCross);
		}
		// child are all crosses?
		else{
			if(currentNode == null){
				currentNode = new OpCross();
			}
			else{
				OpCross op1 = new OpCross();
				op1.setParent(currentNode);
				currentNode = op1;
			}
			copyAndContinue(newCross);
		}
	}

	@Override
	public void visit(Distinct distinct) {
		// TODO Auto-generated method stub
		if(currentNode == null){
			currentNode = new OpDistinct();
		}
		else{
			OpDistinct op = new OpDistinct();
			op.setParent(currentNode);
			currentNode = op;
		}
		net.sf.jsqlparser.statement.select.Distinct d = ((OpDistinct)distinct.op).getDistinct();
		((OpDistinct)currentNode).setDistinct(d);

		copyAndContinue(distinct);

	}

	@Override
	public void visit(GroupBy groupby) {

		if(currentNode == null){
			currentNode = new OpGroupBy();
		}
		else{
			OpGroupBy op = new OpGroupBy();
			op.setParent(currentNode);
			currentNode = op;
		}
		java.util.List l = ((OpGroupBy)groupby.op).getGroupByColumnList();
		((OpGroupBy)currentNode).setGroupByColumnList(l);

		copyAndContinue(groupby);
	}

	@Override
	public void visit(Having having) {
		if(currentNode == null){
			currentNode = new OpHaving();
		}
		else{
			OpHaving op = new OpHaving();
			op.setParent(currentNode);
			currentNode = op;
		}

		copyAndContinue(having);

	}

	@Override
	public void visit(Join join) {
	}

	@Override
	public void visit(Limit limit) {
		if(currentNode == null){
			currentNode = new OpLimit();
		}
		else{
			OpLimit op = new OpLimit();
			op.setParent(currentNode);
			currentNode = op;
		}
		net.sf.jsqlparser.statement.select.Limit l = ((OpLimit)limit.op).getLimit();
		((OpLimit)currentNode).setLimit(l);	
		copyAndContinue(limit);		

	}

	@Override
	public void visit(OrderBy orderby) {
		if(currentNode == null){
			currentNode = new OpOrderBy();
		}
		else{
			OpOrderBy op = new OpOrderBy();
			op.setParent(currentNode);
			currentNode = op;
		}
		java.util.List l = ((OpOrderBy)orderby.op).getOrderByList();
		((OpOrderBy)currentNode).setOrderByList(l);

		copyAndContinue(orderby);

	}

	@Override
	public void visit(SelectCondition selectcondition) {

		////////////////////////////////////
		Expression e = selectcondition.getOperation().getExpression();

		if( e instanceof AndExpression){
			BinaryExpression l = (BinaryExpression)e;
			Expression r = l.getRightExpression();
			while( true ){
				if(r instanceof EqualsTo ){
					equalsToProc( (EqualsTo)r);
				}
				else if(r instanceof LikeExpression){
					likeProc((LikeExpression)r);
				}
				else if(r instanceof OrExpression){
					orProc((OrExpression)r);
				}
				//other comparison expression, to see if there's preSelect
				else if( r instanceof NotEqualsTo ||
						r instanceof GreaterThan ||
						r instanceof GreaterThanEquals ||
						r instanceof MinorThan ||
						r instanceof MinorThanEquals){
					compareProc((BinaryExpression)r);
				}
				else{
					if(leftovers == null){
						leftovers = r;
					}
					else{
						leftovers = new AndExpression(leftovers, r);
					}
				}

				if( l.getLeftExpression() instanceof AndExpression ){
					l = (AndExpression) l.getLeftExpression();
					r = l.getRightExpression();
				}
				else{
					l = (BinaryExpression) l.getLeftExpression();
					if( l instanceof EqualsTo){
						equalsToProc((EqualsTo)l);
					}
					else if (l instanceof OrExpression){
						orProc((OrExpression)l);
					}
					else if( l instanceof NotEqualsTo ||
							l instanceof GreaterThan ||
							l instanceof GreaterThanEquals ||
							l instanceof MinorThan ||
							l instanceof MinorThanEquals){
						compareProc((BinaryExpression)l);
					}
					else{
						if(leftovers == null){
							leftovers = l;
						}
						else{
							leftovers = new AndExpression(leftovers, l);
						}
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
		else if( e instanceof NotEqualsTo ||
				e instanceof GreaterThan ||
				e instanceof GreaterThanEquals||
				e instanceof MinorThan ||
				e instanceof MinorThanEquals){
			compareProc((BinaryExpression)e);
		}
		else{
			leftovers = e;
		}


		if(currentNode == null){
			currentNode = new OpSelectCondition();
		}
		else{
			if(leftovers != null){
				OpSelectCondition op = new OpSelectCondition();
				op.setParent(currentNode);
				currentNode = op;
				currentNode.setExpression(leftovers);
				leftovers = null;//clear
			}
		}


		//set the last node if any equi joins exists
		//setLastNode();

		if(selectcondition.getRight()!= null)
			selectcondition.getRight().accept(this);
		if(selectcondition.getLeft() != null)
			selectcondition.getLeft().accept(this);
	}

	@Override
	public void visit(Table table) {
		//TODO should add join
		OpTable op= (OpTable) table.op;
		String tn = op.getAlias();
		if(preSelect.containsKey(tn)){ // pushable!
			Operation tish = insertPreSelection(op);
			if(currentNode == null){
				currentNode = tish;
			}
			else{
				tish.setParent(currentNode);
			}
			currentNode = tish;
			//			
			//			BinaryExpression c = null; // preSelect conditions
			//			ArrayList<BinaryExpression> es = preSelect.get(tn);
			//			for(BinaryExpression e: es){
			//				if(c == null){
			//					c = e;
			//				}
			//				else{
			//					c = new AndExpression(c, e);
			//				}
			//			}
			//			// inserting a select operation above pushable table
			//			if(currentNode == null){
			//				currentNode = new OpSelectCondition();
			//			}else{
			//				OpSelectCondition n = new OpSelectCondition();
			//				n.setParent(currentNode);
			//				currentNode = n;
			//			}
			//			currentNode.setExpression(c);
			//			// adding table under this select node
			//			OpTable t = new OpTable();
			//			op.setParent(currentNode);
			//			currentNode = t;
			//			copyAndContinue(table);
		}else{ // no preSelect
			if(currentNode == null){
				currentNode = new OpTable();
				((OpTable)currentNode).setName(tn);
				currentNode.setAlias(tn);
			}
			else{
				OpTable t = new OpTable();
				t.setName(tn);
				t.setAlias(tn);
				t.setParent(currentNode);
				currentNode = t;
			}
		}
	}

	@Override
	public void visit(Target target) {

		if(currentNode == null){
			currentNode = new OpTarget();
		}
		else{
			OpTarget op = new OpTarget();
			op.setParent(currentNode);
			currentNode = op;
		}
		((OpTarget)currentNode).setTargetList(((OpTarget)target.op).getTargetList());
		copyAndContinue(target);
	}

	@Override
	public void visit(Union union) {
		if(currentNode == null){
			currentNode = new OpUnion();
		}
		else{
			OpUnion op = new OpUnion();
			op.setParent(currentNode);
			currentNode = op;
		}

		copyAndContinue(union);

	}

	public ArrayList<String> getJoinTableName(EqualsTo e){
		Column l = (Column) e.getLeftExpression();
		Column r =  (Column) e.getRightExpression();

		String tn1 = l.getTable().getName();
		String tn2 = r.getTable().getName();

		ArrayList<String> tns = new ArrayList<String>();
		tns.add(tn1);
		tns.add(tn2);
		return tns;
	}
	public void removeFromHashHash(String tn1, String tn2){
		HashMap<String, ArrayList<EqualsTo>> sub1 = equiJoinConditions.get(tn1);
		HashMap<String, ArrayList<EqualsTo>> sub2 = equiJoinConditions.get(tn2);
		if(sub1 != null){
			sub1.remove(tn2);
			if(sub1.isEmpty()){
				equiJoinConditions.remove(tn1);
			}
		}	
		if(sub2 != null){
			sub2.remove(tn1);
			if(sub2.isEmpty()){
				equiJoinConditions.remove(tn2);
			}
		}
	}
	public void putToHashHash(EqualsTo e){
		ArrayList<String> tns = getJoinTableName(e);
		HashMap<String, ArrayList<EqualsTo>> sub = null;
		ArrayList<EqualsTo> l = null;
		String tn1 = tns.get(0);
		String tn2 = tns.get(1);

		if( equiJoinConditions.containsKey(tn1)){ // this table already exists equijoin
			sub = equiJoinConditions.get(tn1);
			if(sub.containsKey(tn2)){
				l = sub.get(tn2);
			}
			else{
				l = new ArrayList<EqualsTo>();
				sub.put(tn2, l);
			}
			l.add(e);
		}
		else{
			sub = new HashMap<String, ArrayList<EqualsTo>>();
			l = new ArrayList<EqualsTo>();
			l.add(e);
			sub.put(tn2, l);
			equiJoinConditions.put(tn1, sub);
		}

		if(equiJoinConditions.containsKey(tn2)){
			sub = equiJoinConditions.get(tn2);
			if(sub.containsKey(tn1)){
				l = sub.get(tn1);
			}
			else{
				l = new ArrayList<EqualsTo>();
				sub.put(tn1, l);
			}
			l.add(e);
		}
		else{
			sub = new HashMap<String, ArrayList<EqualsTo>>();
			l = new ArrayList<EqualsTo>();
			l.add(e);
			sub.put(tn1, l);
			equiJoinConditions.put(tn2, sub);
		}
	}

	public void likeProc(LikeExpression r){
		//TODO THIS IS PROBABLY BUGGY
		String tn = ((Column)((LikeExpression)r).getLeftExpression()).getTable().getName();
		
		if (GlobalConfiguration.isIndexWisePushSelect){
			String cn = ((Column)((BinaryExpression) r).getLeftExpression()).getColumnName();
			OpTable t = new OpTable();
			t.setName(tn);
			if(isIndex(cn, t) != null){
				ArrayList<BinaryExpression> al = preSelect.get(tn);
				if (al == null){
					al = new ArrayList<BinaryExpression>();
					al.add(r);
					preSelect.put(tn, al);
					return;
				}
				else{
					al.add(r);
					return;
				}
			}else{//preselect with no index
				if(leftovers == null){
					leftovers = r;
				}
				else{
					leftovers = new AndExpression(leftovers, r);// assume all are in CNF form
				}
			}
		}else{ // preselect without index concern
			ArrayList<BinaryExpression> al = preSelect.get(tn);
			if (al == null){
				al = new ArrayList<BinaryExpression>();
				al.add(r);
				preSelect.put(tn, al);
				return;
			}
			else{
				al.add(r);
				return;
			}
		}
	}

	public void equalsToProc(BinaryExpression r){

		String tn = getExclusiveEquiTableName(r);
		if( tn == null){ // EquiJoin
			Expression rhs = r.getRightExpression();
			if ( !( rhs instanceof StringValue ) && !(rhs instanceof Function)) //EquiJjoin
				putToHashHash((EqualsTo)r);
			else{
				if(leftovers == null){
					leftovers = r;
				}
				else{
					leftovers = new AndExpression(leftovers, r);
				}
			}
		}
		else if( tn != null && !tn.equals("AGG")){

			if (GlobalConfiguration.isIndexWisePushSelect){
				String cn = ((Column)((BinaryExpression) r).getLeftExpression()).getColumnName();
				OpTable t = new OpTable();
				t.setName(tn);
				if(isIndex(cn, t) != null){
					ArrayList<BinaryExpression> al = preSelect.get(tn);
					if (al == null){
						al = new ArrayList<BinaryExpression>();
						al.add(r);
						preSelect.put(tn, al);
						return;
					}
					else{
						al.add(r);
						return;
					}
				}else{//preselect with no index
					if(leftovers == null){
						leftovers = r;
					}
					else{
						leftovers = new AndExpression(leftovers, r);// assume all are in CNF form
					}
				}
			}else{ // preselect without index concern
				ArrayList<BinaryExpression> al = preSelect.get(tn);
				if (al == null){
					al = new ArrayList<BinaryExpression>();
					al.add(r);
					preSelect.put(tn, al);
					return;
				}
				else{
					al.add(r);
					return;
				}
			}
//			ArrayList<BinaryExpression> al = preSelect.get(tn);
//			if (al == null){
//				al = new ArrayList<BinaryExpression>();
//				al.add(r);
//				preSelect.put(tn, al);
//			}
//			else{
//				al.add(r);
//			}
		}
		else{
			if(leftovers == null){
				leftovers = r;
			}
			else{
				leftovers = new AndExpression(leftovers, r);
			}
		}
	}
	public void compareProc(BinaryExpression e){

		String tn = getExclusiveEquiTableName(e);
		if(tn == null){ // some other join, left into leftovers (temporary solution). TODO

			if(leftovers == null){
				leftovers = e;
			}
			else{
				leftovers = new AndExpression(leftovers, e);
			}
		}
		else if( tn != null && !tn.equals("AGG")){
			if (GlobalConfiguration.isIndexWisePushSelect){
				String cn = ((Column)((BinaryExpression) e).getLeftExpression()).getColumnName();
				OpTable t = new OpTable();
				t.setName(tn);
				if(isIndex(cn, t) != null){
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
				}else{//preselect with no index
					if(leftovers == null){
						leftovers = e;
					}
					else{
						leftovers = new AndExpression(leftovers, e);// assume all are in CNF form
					}
				}
			}else{
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
//			ArrayList<BinaryExpression> al = preSelect.get(tn);
//			if (al == null){
//				al = new ArrayList<BinaryExpression>();
//				al.add(e);
//				preSelect.put(tn, al);
//			}
//			else{
//				al.add(e);
//			}
		}
		else{
			if(leftovers == null){
				leftovers = e;
			}
			else{
				leftovers = new AndExpression(leftovers, e);
			}
		}
	}

	public String getExclusiveEquiTableName(BinaryExpression e){
		if (GlobalConfiguration.isPushSelect){
			Expression l = e.getLeftExpression();
			Expression r =  e.getRightExpression();

			Boolean lIsTable = l instanceof Column;//? Table? or Column
			Boolean rIsTable = r instanceof Column;

			
			if ( lIsTable && rIsTable ){
				String tn1 = ((Column)l).getTable().getName();
				String tn2 = ((Column)r).getTable().getName();
				if(tn1.contains("AGG") || tn2.contains("AGG")){
					return "AGG";
				}
				// this should be considered as two table operation, right? I'm not sure...
				//else if(tn1.compareTo(tn2) == 0){
				//	return tn1;
				//}
				else
					return null;
			}
			else{
				String tn;
				if(lIsTable){
					tn = ((Column)l).getTable().getName();
					if(tn == null){ // only one table is in query
						HashMap<String, OpTable> at = Sql2RA.getAllTables();
						if (at.size() == 1){
							Iterator itr = at.keySet().iterator();
							tn = (String) itr.next();
						}
					}
					return tn;
				}
				else{
					tn = ((Column)r).getTable().getName();
					if(tn == null){ // only one table is in query
						HashMap<String, OpTable> at = Sql2RA.getAllTables();
						if (at.size() == 1){
							Iterator itr = at.keySet().iterator();
							tn = (String) itr.next();
						}
					}
					return tn;
				}
			}
		}
		else{
			return null;
		}
	}
	public void orProc(OrExpression e){
		Expression r = e.getRightExpression();
		String tn = null;
		if( r instanceof EqualsTo ){ // basic a = b or a = c or a = ...
			//assuming left and right expression of OrExpression should constraint the same attribute
			tn = getExclusiveEquiTableName((BinaryExpression) r);
			if( tn != null && !tn.equals("AGG")){
				if (GlobalConfiguration.isIndexWisePushSelect){
					String cn = ((Column)((BinaryExpression) r).getLeftExpression()).getColumnName();
					OpTable t = new OpTable();
					t.setName(tn);
					if(isIndex(cn, t) != null){
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
					}else{//preselect with no index
						if(leftovers == null){
							leftovers = e;
						}
						else{
							leftovers = new AndExpression(leftovers, e);// assume all are in CNF form
						}
					}
				}
				else{//push without index concern
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
			else{ // A.a = B.b or A.a = B.c ?
				if(leftovers == null){
					leftovers = e;
				}
				else{
					leftovers = new AndExpression(leftovers, e);// assume all are in CNF form
				}
			}
		}
		else if ( r instanceof LikeExpression){
			tn = ((Column)((LikeExpression)r).getLeftExpression()).getTable().getName();
			if (GlobalConfiguration.isIndexWisePushSelect){
				String cn = ((Column)((BinaryExpression) r).getLeftExpression()).getColumnName();
				OpTable t = new OpTable();
				t.setName(tn);
				if(isIndex(cn, t) != null){
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
				}else{//preselect with no index
					if(leftovers == null){
						leftovers = e;
					}
					else{
						leftovers = new AndExpression(leftovers, e);// assume all are in CNF form
					}
				}
			}
			else{//preselect without index concern
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
		else{
			if(leftovers == null){
				leftovers = e;
			}
			else{
				leftovers = new AndExpression(leftovers, e);// assume all are in CNF form
			}
		}
	}

	// return null if attr is not t's index
	// otherwise, return index ID
	public String isIndex(String attr, OpTable t){
		String idx = null;
		idx = t.getIndex().get(attr.toLowerCase());
		return idx;
	}

	// name is confusing. there's nothing to do with GlobalConfiguration.isPushSelect.
	// if PreSelection exist, this will return the Select condition node
	// if non-exist, this will just return the table node
	public Operation insertPreSelection(OpTable t){
		String tn = t.getAlias();
		if(preSelect.containsKey(tn)){ // pushable!
			//assuming that same attribute constraint will appear at the same place 
			//like a <= 1 and a > 0, not a<=1 and blah blah and a >0
			ArrayList<Annotation> cnfs = new ArrayList<Annotation>(); // preSelect conditions contain Index
			BinaryExpression ic = null;
			BinaryExpression c = null; // preSelect conditions doesn't contain Index
			String prevIndex = null;// get ahold of previous index, if different from current Index, new Node is created.
			Annotation a = new Annotation();
			a.tableName1 = tn;
			ArrayList<BinaryExpression> es = preSelect.get(tn);
			for(BinaryExpression e: es){
				// assume preSelection condition, only LHS will contain attribute info.
				String idx = null;
				if( e.getLeftExpression() instanceof Column){
					String lhs = ((Column) e.getLeftExpression()).getColumnName().toLowerCase();
					idx = isIndex(lhs, t);
				}
				if(idx != null){
					if(e instanceof GreaterThan ||
							e instanceof GreaterThanEquals ||
							e instanceof MinorThan ||
							e instanceof MinorThanEquals){
						if(prevIndex == null || prevIndex.compareTo(idx)==0){ //TODO whatif only one idx appeared
							if(ic == null){
								ic = e;
								a.idxType = "B+";
								if ( e instanceof MinorThanEquals){
									a.highInclusive = true;
									a.rangeHigh = e.getRightExpression().toString();
								}else if (e instanceof GreaterThanEquals){
									a.lowInclusive = true;
									a.rangeLow = e.getRightExpression().toString();
								}else if (e instanceof MinorThan){
									a.rangeHigh = e.getRightExpression().toString();
								}else if ( e instanceof GreaterThan){
									a.rangeLow = e.getRightExpression().toString();
								}
							}else{
								ic = new AndExpression(ic, e);
								if ( e instanceof MinorThanEquals){
									a.highInclusive = true;
									a.rangeHigh = e.getRightExpression().toString();
								}else if (e instanceof GreaterThanEquals){
									a.lowInclusive = true;
									a.rangeLow = e.getRightExpression().toString();
								}else if (e instanceof MinorThan){
									a.rangeHigh = e.getRightExpression().toString();
								}else if ( e instanceof GreaterThan){
									a.rangeLow = e.getRightExpression().toString();
								}
							}
							prevIndex = idx;
						}else if( prevIndex.compareTo(idx) != 0){ // when two idx both appeared in preselection 
							a.exp = ic;
							a.idx = prevIndex;
							cnfs.add(a);
							a = new Annotation();
							a.idxType = "B+";
							ic = e;
							if ( e instanceof MinorThanEquals){
								a.highInclusive = true;
								a.rangeLow = e.getRightExpression().toString();
							}else if (e instanceof GreaterThanEquals){
								a.lowInclusive = true;
								a.rangeHigh = e.getRightExpression().toString();
							}
							prevIndex = idx;
						}
					}else if ( e instanceof EqualsTo || 
							e instanceof NotEqualsTo){
						if(prevIndex == null || prevIndex.compareTo(idx)==0){ 
							prevIndex = idx;
							if(ic == null){
								ic = e;
								a.idxType = "hash";
							}else{
								ic = new AndExpression(ic, e);
							}
						}else if( prevIndex.compareTo(idx) != 0){ // when two idx both appeared in preselection 
							a.exp = ic;
							a.idx = prevIndex;
							cnfs.add(a);
							a = new Annotation();
							a.idxType = "hash";
							ic = e;
							prevIndex = idx;
						}
					}
				}else{ // if idx == null, condition is not based on index
					if(c == null){
						c = e;
					}
					else{
						c = new AndExpression(c, e);
					}
				}
			}
			if( ic != null){ // add the last indexd condition to ArrayList
				a.exp = ic;
				a.idx = prevIndex;
				cnfs.add(a);
			}

			//adding all the indexed condition on top of table
			OpSelectCondition n = null;
			Operation bottom = t; // BANANA..BANANANA...BANANA...BANANANA
			for(Annotation cnf: cnfs){
				if (cnf.idxType.compareTo("B+") == 0 && cnf.rangeHigh != null && cnf.rangeLow != null){
					n = new IndexRangeScan();
					((IndexRangeScan)n).setTabelName(cnf.tableName1);
					((IndexRangeScan)n).setIndexName(cnf.idx);
					((IndexRangeScan)n).highInclusive = cnf.highInclusive;
					((IndexRangeScan)n).lowInclusive = cnf.lowInclusive;
					((IndexRangeScan)n).rangeHigh = cnf.rangeHigh;
					((IndexRangeScan)n).rangeLow = cnf.rangeLow;
					n.setExpression(cnf.exp);
					bottom.setParent(n);
					bottom = n;
				}else if ( cnf.idxType.compareTo("hash") == 0){
					n = new IndexScan();
					((IndexScan)n).setTabelName(cnf.tableName1);
					((IndexScan)n).setIndexName(cnf.idx);
					n.setExpression(cnf.exp);
					bottom.setParent(n);
					bottom = n;
				}else{
//					if(leftovers == null){
//						leftovers = cnf.exp;
//					}
//					else{
//						leftovers = new AndExpression(leftovers, cnf.exp);
//					}
					n = new OpSelectCondition();
					n.setExpression(cnf.exp);
					n.setAnnotation(cnf);
					bottom.setParent(n);
					bottom = n;
				}
			}
			// adding all non-indexed condition on top of current node
			if(c != null){
				//TODO
//				if(leftovers == null){
//					leftovers = c;
//				}
//				else{
//					leftovers = new AndExpression(leftovers, c);
//				}
				
				n = new OpSelectCondition();
				n.setExpression(c);
				bottom.setParent(n);
				bottom = n;
			}
			return bottom;
		}else{ // if  there's no preSelect condition
			return t;
		}
	}

	// this is used for reOrganizing the order of the table
	// before the tree is traversed to all crossings.
	// reOrganizing: first try to adjust the table size, if the table 
	// has preSelection, then the size is considered to be the size of intermediate size.
	// otherwise, the initial size is used ( which is the default size)
	public void reOrganize(Cross cross){
		HashMap<String, OpTable> allTables = Sql2RA.getAllTables();

		// First, change the size of all preselected table
		Iterable<String> tableAlias = allTables.keySet();
		Iterator itr = tableAlias.iterator();
		while(itr.hasNext()){
			String ta = (String) itr.next();
			if(preSelect.containsKey(ta)){
				setSize(allTables.get(ta), preSelect.get(ta));
			}
		}

		//Second, adjust the order of the table according to their estimated size
		ArrayList<OpTable> sortedTables = new ArrayList<OpTable>();
		sortedTables.addAll(allTables.values());
		for(int i = 0; i < sortedTables.size(); i++){
			for (int j = i+1; j < sortedTables.size(); j++){
				OpTable t1 = sortedTables.get(i);
				int s1 = t1.getSize();
				OpTable t2 = sortedTables.get(j);
				int s2 = t2.getSize();
				if( s1 > s2){
					sortedTables.set(i, t2);
					sortedTables.set(j, t1);
				}
			}
		}

		//Third, rebuilt the tree ( rest of the tree starting from crosses)
		// top down manner
		OpCross oc =  (OpCross) cross.op;
		oc.setKidNull();//rebuilding tree. Hopefully java trash collector can notice this

		for ( int i = 0; i < sortedTables.size() - 2; i++){
			OpTable t =  sortedTables.get(i);
			oc.setKid(t);
			OpCross leftCross= new OpCross();
			oc.setKid(leftCross);
			oc = leftCross;
		}
		oc.setKid(sortedTables.get(sortedTables.size() - 2));
		oc.setKid(sortedTables.get(sortedTables.size() - 1));

		return;
	}

	// this is used to estimate the size of the table after preselection.
	// size is hard-coded!!!! :( THIS IS NOT GOOD!
	// all the data are based on checkpoint3_8mb data set
	public void setSize(OpTable table, ArrayList<BinaryExpression> ae){
		// how about by knowing the filename of the sql. and assign the size
		// TODO

		//		int flag = -1;
		//		String ftn = table.getName().toUpperCase(); // full table name
		//		for(BinaryExpression be : ae){
		//			int curSize = table.getSize();
		//			String l = be.getLeftExpression().toString();
		//			String r = be.getRightExpression().toString();
		//			if(be instanceof EqualsTo){
		//				flag = 0;
		//			}else if(be instanceof NotEqualsTo){
		//				flag = 1;
		//			}else if(be instanceof GreaterThan){
		//				flag = 2;
		//			}else if(be instanceof MinorThan){
		//				flag = 3;
		//			}else if(be instanceof GreaterThanEquals){
		//				flag = 4;
		//			}else if(be instanceof MinorThanEquals){
		//				flag = 5;
		//			}
		//			int num = 7;
		//			if(ftn.compareTo("LINEITEM") == 0){
		//				if(l.matches(".*1992-09-01.*") && flag == 5){ // shipdate <= DATE('1992-09-01') _tpch1
		//					table.setSize(curSize * );
		//				}else if(l.matches(".*1995-03-15.*") && flag == 2){ // shipdate > DATE('1995-03-15')_tpch3
		//					table.setSize();
		//				}else if(l.matches(".*1994-01-01.*") && flag == 4){ // shipdate >= DATE('1994-01-01')_tpch6
		//					
		//				}else if(l.matches(".*1995-01-01.*") && flag == 3){//shipdate < date('1995-01-01')_tpch6
		//					
		//				}else if(l.matches(".*discount.*") && flag == 2){ // discount > 0.05 _tpch6
		//					
		//				}else if(l.matches(".*discount.*") && flag == 3){// discount <0.07 _tpch6
		//					
		//				}else if(l.matches(".*quantity.*") && flag == 3){ // quantity < 24 _tpch6
		//					
		//				}else if(l.matches(".*1995-01-01.*") && flag == 4){//shipdate >= date('1995-01-01')_tpch7a
		//					
		//				}else if(l.matches(".*1996-12-31.*") && flag == 5){//shipdate <= date('1996-12-31')_tpch7a
		//					
		//				}else if(l.matches(".*1996-01-01.*") && flag == 4){//shipdate >= date('1996-01-01')_tpch7b
		//					
		//				}else if(l.matches(".*1997-12-31.*") && flag == 5){//shipdate <= date('1997-12-31')_tpch7b
		//					
		//				}else if(l.matches(".*1995-02-01.*") && flag == 4){//shipdate >= date('1995-02-01')_tpch7c
		//					
		//				}else if(l.matches(".*1996-11-28.*") && flag == 5){//shipdate <= date('1996-11-28')_tpch7c
		//					
		//				}else if(l.matches(".*1996-02-01.*") && flag == 4){//shipdate >= date('1996-02-01')_tpch7d
		//					
		//				}else if(l.matches(".*1997-11-28.*") && flag == 5){//shipdate <= date('1997-11-28')_tpch7d
		//					
		//				}else if(l.matches(".*1996-03-01.*") && flag == 4){//shipdate >= date('1996-03-01')_tpch7e
		//					
		//				}else if(l.matches(".*1997-12-28.*") && flag == 5){//shipdate <= date('1997-12-28')_tpch7e
		//					
		//				}else if(l.matches(".*1995-03-01.*") && flag == 4){//shipdate >= date('1995-03-01')_tpch7f
		//					num = 7;
		//				}else if(l.matches(".*1996-12-28.*") && flag == 5 && num == 7){//shipdate <= date('1996-12-28')_tpch7f
		//					
		//				}else if(l.matches(".*1995-04-01.*") && flag == 4){//shipdate >= date('1995-04-01')_tpch7f
		//					
		//				}else if(l.matches(".*1996-12-28.*") && flag == 5 && num != 7){//shipdate <= date('1996-12-28')_tpch7f
		//					
		//				}
		//			}else if(ftn.compareTo("ORDERS") == 0){
		//				if(l.matches(".*1995-03-15.*") && flag == 3){ // orders.orderdate < DATE('1995-03-15')..._tpch3
		//					table.setSize();
		//				}else if(l.matches(".*1994-01-01.*") && flag == 4){//orderdate >= DATE('1994-01-01')_tpch5
		//					
		//				}else if(l.matches(".*1994-02-01.*") && flag == 3){ // orderdate < DATE('1994-02-01') _tpch5
		//					
		//				}else if(l.matches(".*1995-03-05.*") && flag == 4){//orderdate >= DATE('1995-03-05')_tpch10a
		//					
		//				}else if(l.matches(".*1995-04-05.*") && flag == 3){ // orderdate < DATE('1995-04-05') _tpch10a
		//					
		//				}
		//			}else if(ftn.compareTo("PARTSUPP") == 0){
		//				
		//			}else if(ftn.compareTo("PART") == 0){
		//				
		//			}else if(ftn.compareTo("CUSTOMER") == 0){
		//				if(l.matches("\'BUILDING\'") && flag == 0){ // customer.mktsegment = 'BUILDING'
		//					table.setSize();
		//				}
		//				
		//			}else if(ftn.compareTo("SUPPLIER") == 0){
		//				
		//			}else if(ftn.compareTo("NATION") == 0){
		//				
		//			}else if(ftn.compareTo("REGION") == 0){
		//				
		//			}
		//		}

	}
}
