package edu.buffalo.cse562.sql.expression;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;

/**
 *
 * @author Niccolo' Meneghetti
 */
public class SimpleCalculator extends AbstractExpressionVisitor
{
    private long accumulator;

    public long getResult()
    { return accumulator; }

    @Override
    public void visit(LongValue lv)
    { accumulator=lv.getValue(); }


    @Override
    public void visit(Addition adtn)
    {
        adtn.getLeftExpression().accept(this);
        long leftValue = accumulator;
        adtn.getRightExpression().accept(this);
        long rightValue = accumulator;
        accumulator=(leftValue+rightValue);
    }

    @Override
    public void visit(Subtraction s)
    {
        s.getLeftExpression().accept(this);
        long leftValue = accumulator;
        s.getRightExpression().accept(this);
        long rightValue = accumulator;
        accumulator=(leftValue-rightValue);
    }

    @Override
    public void visit(Parenthesis prnths)
    { prnths.getExpression().accept(this); }


}
