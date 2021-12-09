package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long   serialVersionUID = 1L;

    private Map<Field, Integer> groupMap;

    // Group by field
    private int                 gbField;
    private Type                gbFieldType;
    // Aggregation field
    private int                 agField;
    // Aggregation operation
    private Op                  op;
    private Field               DEFAULT_FIELD    = new StringField("Default", 10);

    private TupleDesc           td;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Err in StringAggregator: What != Count");
        }
        this.groupMap = new HashMap<>();
        this.gbField = gbfield;
        this.agField = afield;
        this.op = what;
        this.gbFieldType = gbfieldtype;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (this.td == null) {
            buildTupleDesc(tup.getTupleDesc());
        }
        final Field gbField = tup.getField(this.gbField);
        final Field target = (this.gbField == NO_GROUPING ? DEFAULT_FIELD : gbField);
        this.groupMap.put(target, this.groupMap.getOrDefault(target, 0) + 1);
    }

    public void buildTupleDesc(final TupleDesc originTd) {
        // some code goes here
        if (this.gbField == NO_GROUPING) {
            Type[] types = new Type[] { Type.INT_TYPE };
            String[] names = new String[] { originTd.getFieldName(this.gbField) };
            this.td = new TupleDesc(types, names);
        } else {
            Type[] types = new Type[] { this.gbFieldType, Type.INT_TYPE };
            String[] names = new String[] { originTd.getFieldName(this.gbField), originTd.getFieldName(this.agField) };
            this.td = new TupleDesc(types, names);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        final List<Tuple> tuples = new ArrayList<>();
        if (this.gbField != NO_GROUPING) {
            this.groupMap.forEach((key, cnt) -> {
                final Tuple tuple = new Tuple(this.td);
                tuple.setField(0, key);
                tuple.setField(1, new IntField(cnt));
                tuples.add(tuple);
            });
        } else {
            final Tuple tuple = new Tuple(this.td);
            tuple.setField(0, new IntField(this.groupMap.get(DEFAULT_FIELD)));
            tuples.add(tuple);
        }
        return new TupleIterator(this.td, tuples);
    }
}
