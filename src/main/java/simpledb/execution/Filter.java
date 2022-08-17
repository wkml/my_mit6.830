package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    // 断言，实现条件过滤的重要属性
    private Predicate predicate;
    // 数据源，我们从这里获取一条一条的Tuple用predicate去过滤
    private OpIterator child;
    // 我们返回结果元组（行）的描述信息
    // 在Filter中与传入的数据源是相同的
    // 而在其它运算符中是根据返回结果的情况去创建TupleDesc的
    private TupleDesc tupleDesc;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.child = child;
        this.predicate = p;
        this.tupleDesc = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException, TransactionAbortedException, DbException {
        // some code goes here
        Tuple tuple;
        while (this.child.hasNext()) {
            tuple = this.child.next();
            if (tuple != null) {
                // 每次调用fetchNext，我们是从Filter的child数据源中不断取出tuple，
                // 只要有一条Tuple满足predicate的filter的过滤条件，我们就可以返回一条Tuple，
                // 即这条Tuple是经过过滤条件筛选之后的有效Tuple。
                if (this.predicate.filter(tuple)) {
                    return tuple;
                }
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
