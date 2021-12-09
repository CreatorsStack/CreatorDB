package simpledb.algorithm.Join;

import simpledb.execution.JoinPredicate;
import simpledb.execution.OpIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

import java.util.ArrayList;
import java.util.List;

// An impl for nestedLoopJoin
public class NestedLoopJoin extends JoinStrategy {

    public NestedLoopJoin(final OpIterator child1, final OpIterator child2, final TupleDesc td,
                          final JoinPredicate joinPredicate) {
        super(child1, child2, td, joinPredicate);
    }

    @Override
    public TupleIterator doJoin() {
        final List<Tuple> tuples = new ArrayList<>();
        try {
            child1.rewind();
            while (child1.hasNext()) {
                final Tuple lTuple = child1.next();
                child2.rewind();
                while (child2.hasNext()) {
                    final Tuple rTuple = child2.next();
                    if (this.joinPredicate.filter(lTuple, rTuple)) {
                        tuples.add(mergeTuple(lTuple, rTuple, this.td));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error happen when nested loop join");
        }
        return new TupleIterator(this.td, tuples);
    }

    @Override
    public void close() {

    }
}
