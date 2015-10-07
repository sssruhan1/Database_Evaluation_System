package edu.buffalo.cse562.RAOptimizer;

import edu.buffalo.cse562.sql2RA.*;


/**
 * @author ruhansa
 * This class is for transferring between a normal Operation class 
 * ( those in edu.buffalo.cse562.sql2RA package) to Visitor-pattern 
 * compatible operation class ( those in edu.buffalo.cse562.RAOptimizer package).
 */
public class Porter {
	
	
	/**
	 * @param root, normal Operation class tree
	 * @return Visitor-pattern compatible operation class tree
	 * using post-order traverse
	 */
	public OpPlusInterface opToVPC(Operation root){

		
		OpPlusInterface right = null;
		OpPlusInterface left = null;
		
		if( root.getLeft() != null){
			left = opToVPC(root.getLeft());
		}
		if( root.getRight() != null){
			right = opToVPC(root.getRight());
		}
		
		OpPlusInterface oi = null;
		if(root instanceof OpAggregate){
			oi = new Aggregate(root);
		}
		if(root instanceof OpCross){
			oi = new Cross(root);
		}
		if(root instanceof OpDistinct){
			oi = new Distinct(root);
		}
		if(root instanceof OpGroupBy){
			oi = new GroupBy(root);
		}
		if(root instanceof OpHaving){
			oi = new Having(root);
		}
		if(root instanceof OpJoin){
			oi = new Join(root);
		}
		if(root instanceof OpLimit){
			oi = new Limit(root);
		}
		if(root instanceof OpOrderBy){
			oi = new OrderBy(root);
		}
		if(root instanceof OpSelectCondition){
			oi = new SelectCondition(root);
		}
		if(root instanceof OpTable){
			oi = new Table(root);
		}
		if(root instanceof OpTarget){
			oi = new Target(root);
		}
		if(root instanceof OpUnion){
			oi = new Union(root);
		}
		
		if(oi != null){
			oi.setLeft(left);
			oi.setRight(right);
			if(left != null){
				left.setParent(oi);
			}
			if(right != null){
				right.setParent(oi);
			}
		}
		
		return oi;
	}
	
	public Operation VPCToOp(OpPlusInterface root){
		
		if(root != null)
			return root.op;
		
		return null;
		
	}

}
