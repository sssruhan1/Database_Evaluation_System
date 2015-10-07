package edu.buffalo.cse562.sql.expression;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

import java.util.List;

import edu.buffalo.cse562.algebra.RelationalExpr;


public class RelationalStatementVisitor implements StatementVisitor {
    private RelationalExpr re;

    @Override
    public void visit(CreateTable createTable) {
        List<ColumnDefinition> cd = createTable.getColumnDefinitions();
        String name = createTable.getTable().getName();
        System.out.println("The table is " + name);
    }

    @Override
    public void visit(Select select) {
        select.accept(this);
    }

    public RelationalExpr get_relational_expr() {
        return re;
    }

    @Override
    public void visit(Delete delete) {
        throw new UnsupportedOperationException("Not supported yet.");
    };

    public void visit(Drop drop) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(Insert insert) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(Replace replace) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(Truncate truncate) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(Update update) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public static void main(String[] args) {

    }

}
