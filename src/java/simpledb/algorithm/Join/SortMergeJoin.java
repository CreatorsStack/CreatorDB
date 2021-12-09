package simpledb.algorithm.Join;

import simpledb.execution.JoinPredicate;
import simpledb.execution.OpIterator;
import simpledb.execution.Predicate;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// An impl of sort merge join
public class SortMergeJoin extends JoinStrategy {
    private final int  blockCacheSize = 131072 * 5;
    private  Tuple[]   block1 ;
    private  Tuple[]   block2 ;

    private JoinPredicate lt ;
    private JoinPredicate eq ;


    public SortMergeJoin(final OpIterator child1, final OpIterator child2, final TupleDesc td, final JoinPredicate joinPredicate) {
        super(child1, child2, td, joinPredicate);
        final int tuple1Num = this.blockCacheSize / child1.getTupleDesc().getSize();
        final int tuple2Num = this.blockCacheSize / child2.getTupleDesc().getSize();

        // build cache block
        this.block1 = new Tuple[tuple1Num];
        this.block2 = new Tuple[tuple2Num];

        final int field1 = joinPredicate.getField1();
        final int field2 = joinPredicate.getField2();
        this.lt = new JoinPredicate(field1, Predicate.Op.LESS_THAN, field2);
        this.eq = new JoinPredicate(field1, Predicate.Op.EQUALS, field2);
    }


    @Override
    public TupleIterator doJoin()  {
        final List<Tuple> tupleList = new ArrayList<>();

        // fetch child1
        try {
            child1.rewind();
            while (child1.hasNext()) {
                int end1 = fetchTuples(child1, block1);
                // Fetch each block of child2, and do merge join
                child2.rewind();
                while (child2.hasNext()) {
                    int end2 = fetchTuples(child2, block2);
                    mergeJoin(tupleList, end1, end2);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error happen when sort merge join:" + e.getMessage());
        }
        Arrays.fill(this.block1, null);
        Arrays.fill(this.block2, null);
        return new TupleIterator(this.td, tupleList);
    }

    private void mergeJoin(final List<Tuple> tupleList, int end1, int end2) {
        // 1.Sort each block
        final int field1 = this.joinPredicate.getField1();
        final int field2 = this.joinPredicate.getField2();
        sortTuples(block1, end1, field1);
        sortTuples(block2, end2, field2);

        // 2.Join
        int index1 = 0, index2 = 0;
        final Predicate.Op op = this.joinPredicate.getOperator();
        switch (op) {
            case EQUALS : {
                while (index1 < end1 && index2 < end2) {
                    final Tuple lTuple = this.block1[index1];
                    final Tuple rTuple = this.block2[index2];
                    if (eq.filter(lTuple, rTuple)) {
                        // If equal , we should find the right boundary that equal to lTuple in block1 and rTuple in block2
                        final JoinPredicate eq1 = new JoinPredicate(field1, Predicate.Op.EQUALS, field1);
                        final JoinPredicate eq2 = new JoinPredicate(field2, Predicate.Op.EQUALS, field2);
                        int begin1 = index1 + 1, begin2 = index2 + 1;
                        while (begin1 < end1 && eq1.filter(lTuple, this.block1[begin1])) begin1 ++;
                        while (begin2 < end2 && eq2.filter(rTuple, this.block2[begin2])) begin2 ++;
                        for (int i = index1; i < begin1; i++) {
                            for (int j = index2; j < begin2; j++) {
                                tupleList.add(mergeTuple(this.block1[i], this.block2[j], this.td));
                            }
                        }
                        index1 = begin1;
                        index2 = begin2;
                    } else if (lt.filter(lTuple, rTuple)) {
                        index1 ++;
                    } else {
                        index2 ++;
                    }
                }
                return;
            }
            case LESS_THAN: case LESS_THAN_OR_EQ: {
                while (index1 < end1) {
                    final Tuple lTuple = this.block1[index1 ++];
                    index2 = end2 - 1;
                    while (index2 >= 0) {
                        final Tuple rTuple = this.block2[index2 --];
                        if (this.joinPredicate.filter(lTuple, rTuple)) {
                            tupleList.add(mergeTuple(lTuple, rTuple, this.td));
                        } else {
                            break;
                        }
                    }
                }
                return;
            }
            case GREATER_THAN: case GREATER_THAN_OR_EQ: {
                while (index1 < end1) {
                    final Tuple lTuple = this.block1[index1++];
                    index2 = 0;
                    while (index2 < end2) {
                        final Tuple rTuple = this.block2[index2++];
                        if (this.joinPredicate.filter(lTuple, rTuple)) {
                            tupleList.add(mergeTuple(lTuple, rTuple, this.td));
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    }

    private void sortTuples(final Tuple[] tuples, int field, int len) {
        final JoinPredicate lt = new JoinPredicate(field, Predicate.Op.LESS_THAN, field);
        final JoinPredicate gt = new JoinPredicate(field, Predicate.Op.GREATER_THAN, field);
        Arrays.sort(tuples, 0, len, (o1, o2) -> {
            if (lt.filter(o1, o2)) {
                return -1;
            }
            if (gt.filter(o1, o2)) {
                return 1;
            }
            return 0;
        });
    }


    @Override
    public void close() {
        this.block1 = null;
        this.block2 = null;
    }
}
