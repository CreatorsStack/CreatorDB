package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private List<TDItem> descList;

    private int          fieldNum;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type         fieldType;

        /**
         * The name of the field
         * */
        public String             fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public boolean equals(Object o)
        {
            if(o == null)
                return false;
            if(o == this)
                return true;
            if(!(o instanceof TDItem))
                return false;

            TDItem tdio = (TDItem) o;
            return tdio.fieldType == this.fieldType;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return this.descList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("The typeAr length must be equal than fieldAr length");
        }
        this.descList = new ArrayList<>(typeAr.length);
        this.fieldNum = typeAr.length;
        for (int i = 0; i < typeAr.length; i++) {
            final TDItem item = new TDItem(typeAr[i], fieldAr[i]);
            this.descList.add(item);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, new String[typeAr.length]);
    }

    public TupleDesc(final List<TDItem> itemList) {
        // some code goes here
        this.descList = new ArrayList<>(itemList);
        this.fieldNum = this.descList.size();
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.fieldNum;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= this.fieldNum) {
            throw new NoSuchElementException();
        }
        return this.descList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= this.fieldNum) {
            throw new NoSuchElementException();
        }
        return this.descList.get(i).fieldType;
    }

    public List<TDItem> getDescList() {
        return descList;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) {
            throw new NoSuchElementException();
        }
        for (int i = 0; i < this.fieldNum; i++) {
            if (name.equals(this.descList.get(i).fieldName)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (int i = 0; i < this.fieldNum; i++) {
            size += this.descList.get(0).fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        final List<TDItem> newDesc = new ArrayList<>(td1.descList);
        newDesc.addAll(td2.descList);
        return new TupleDesc(newDesc);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if(o == null)
            return false;
        if(o == this)
            return true;
        if(!(o instanceof TupleDesc))
            return false;

        TupleDesc tdo = (TupleDesc) o;
        if(this.numFields() != tdo.numFields())
            return false;
        Iterator<TDItem> it1 = tdo.iterator();
        Iterator<TDItem> it2 = this.iterator();
        while(it2.hasNext())
        {
            // override equals in TDItem
            if(!(it1.next().equals(it2.next())))
                return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        return "TupleDesc{" + "descList=" + descList + ", fieldNum=" + fieldNum + '}';
    }
}
