package simpledb.algorithm.Join;

import simpledb.execution.JoinPredicate;
import simpledb.execution.OpIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

import java.util.Arrays;

public abstract class JoinStrategy {

    protected final OpIterator    child1;
    protected final OpIterator    child2;
    protected final TupleDesc     td;
    protected final JoinPredicate joinPredicate;

    public JoinStrategy(final OpIterator child1, final OpIterator child2, final TupleDesc td,
                        final JoinPredicate joinPredicate) {
        this.child1 = child1;
        this.child2 = child2;
        this.td = td;
        this.joinPredicate = joinPredicate;
    }

    protected Tuple mergeTuple(final Tuple tuple1, final Tuple tuple2, final TupleDesc td) {
        final Tuple tuple = new Tuple(td);
        int len1 = tuple1.getTupleDesc().numFields();
        for (int i = 0; i < len1; i++) {
            tuple.setField(i, tuple1.getField(i));
        }
        for (int i = 0; i < tuple2.getTupleDesc().numFields(); i++) {
            tuple.setField(i + len1, tuple2.getField(i));
        }
        return tuple;
    }

    protected int fetchTuples(final OpIterator child, final Tuple[] tuples) throws Exception {
        int i = 0;
        Arrays.fill(tuples, null);
        while (child.hasNext() && i < tuples.length) {
            final Tuple next = child.next();
            if (next != null) {
                tuples[i++] = next;
            }
        }
        return i;
    }

    // Join child1 and child2, return a tuple iterator result
    public abstract TupleIterator doJoin();

    public abstract void close();
}
