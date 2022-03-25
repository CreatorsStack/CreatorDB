package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

import static simpledb.execution.Aggregator.NO_GROUPING;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator        child;
    private int               agField;
    private int               gbField;
    private Type              gbFieldType;

    private Aggregator.Op     op;
    private Aggregator        aggregator;

    private TupleIterator     iterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.agField = afield;
        this.gbField = gfield;
        this.op = aop;
        this.child = child;

        TupleDesc originTd = this.child.getTupleDesc();
        this.gbFieldType = (this.gbField == -1 ? null : originTd.getFieldType(this.gbField));
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return this.gbField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return this.child.getTupleDesc().getFieldName(this.gbField);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return this.agField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return this.child.getTupleDesc().getFieldName(this.agField);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();

        TupleDesc originTd = this.child.getTupleDesc();
        // Build aggregator
        if (originTd.getFieldType(agField) == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(this.gbField, this.gbFieldType, this.agField, this.op);
        } else {
            this.aggregator = new StringAggregator(this.gbField, this.gbFieldType, this.agField, this.op);
        }

        // Merge tuples into group
        while (this.child.hasNext()) {
            final Tuple tuple = this.child.next();
            this.aggregator.mergeTupleIntoGroup(tuple);
        }
        this.iterator = (TupleIterator) this.aggregator.iterator();
        this.iterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.iterator.hasNext()) {
            return this.iterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    public void close() {
        // some code goes here
        this.child.close();
        this.iterator.close();
        super.close();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        if (this.td != null) {
            return this.td;
        }
        // some code goes here
        if (this.gbField == NO_GROUPING) {
            Type[] types = new Type[] { Type.INT_TYPE };
            String[] names = new String[] { this.aggregateFieldName() };
            this.td = new TupleDesc(types, names);
        } else {
            Type[] types = new Type[] { this.gbFieldType, Type.INT_TYPE };
            String[] names = new String[] { this.groupFieldName(), this.aggregateFieldName() };
            this.td = new TupleDesc(types, names);
        }
        return this.td;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children.length > 0) {
            this.child = children[0];
        }
    }

}
