package simpledb.algorithm.Join;

import simpledb.execution.JoinPredicate;
import simpledb.execution.OpIterator;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

// An impl of simple hashJoin
public class HashJoin extends JoinStrategy {

    public HashJoin(final OpIterator child1, final OpIterator child2, final TupleDesc td,
                    final JoinPredicate joinPredicate) {
        super(child1, child2, td, joinPredicate);
    }

    @Override
    public TupleIterator doJoin() {
        return null;
    }

}
