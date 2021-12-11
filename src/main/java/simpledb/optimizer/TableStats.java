package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap      = new ConcurrentHashMap<>();

    static final int                                       IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    private static final int              NUM_HIST_BINS = 100;

    // FieldId -> histogram (String or Integer)
    private final Map<Integer, Histogram> histogramMap;
    private int                           totalTuples;
    private int                           totalPages;
    private int                           tableId;
    private int                           ioCostPerPage;
    private TupleDesc                     td;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        this.ioCostPerPage = ioCostPerPage;
        this.tableId = tableid;
        this.histogramMap = new HashMap<>();
        this.totalTuples = 0;
        final HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.totalPages = table.numPages();
        this.td = table.getTupleDesc();

        // Build histogram for every field
        final Map<Integer, ArrayList> fieldValues = fetchFieldValues(tableId);
        for (final int fieldId : fieldValues.keySet()) {
            if (td.getFieldType(fieldId) == Type.INT_TYPE) {
                final List<Integer> values = (ArrayList<Integer>) fieldValues.get(fieldId);
                final int minVal = Collections.min(values);
                final int maxVal = Collections.max(values);
                final IntHistogram histogram = new IntHistogram(NUM_HIST_BINS, minVal, maxVal);
                for (final Integer v : values) {
                    histogram.addValue(v);
                }
                this.histogramMap.put(fieldId, histogram);
            } else {
                final List<String> values = (ArrayList<String>) fieldValues.get(fieldId);
                final StringHistogram histogram = new StringHistogram(NUM_HIST_BINS);
                for (final String v : values) {
                    histogram.addValue(v);
                }
                this.histogramMap.put(fieldId, histogram);
            }
        }
    }

    // Fetch table field's values by seqScan
    private Map<Integer, ArrayList> fetchFieldValues(final int tableId) {
        final Map<Integer, ArrayList> fieldValueMap = new HashMap<>();
        for (int i = 0; i < td.numFields(); i++) {
            if (td.getFieldType(i) == Type.INT_TYPE) {
                fieldValueMap.put(i, new ArrayList<Integer>());
            } else {
                fieldValueMap.put(i, new ArrayList<String>());
            }
        }

        final SeqScan seqScan = new SeqScan(new TransactionId(), tableId);
        try {
            seqScan.open();
            while (seqScan.hasNext()) {
                this.totalTuples++;
                final Tuple next = seqScan.next();
                for (int i = 0; i < td.numFields(); i++) {
                    final Field field = next.getField(i);
                    switch (field.getType()) {
                        case INT_TYPE: {
                            final int value = ((IntField) field).getValue();
                            fieldValueMap.get(i).add(value);
                            break;
                        }
                        case STRING_TYPE: {
                            final String value = ((StringField) field).getValue();
                            if (!Objects.equals(value, "")) {
                                fieldValueMap.get(i).add(value);
                            }
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fieldValueMap;
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return this.ioCostPerPage * this.totalPages;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (this.totalTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (this.histogramMap.containsKey(field)) {
            switch (this.td.getFieldType(field)) {
                case INT_TYPE: {
                    final IntHistogram histogram = (IntHistogram) this.histogramMap.get(field);
                    return histogram.estimateSelectivity(op, ((IntField) constant).getValue());
                }
                case STRING_TYPE: {
                    final StringHistogram histogram = (StringHistogram) this.histogramMap.get(field);
                    return histogram.estimateSelectivity(op, ((StringField) constant).getValue());
                }
            }
        }
        return 0.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.totalTuples;
    }

}
