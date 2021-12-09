package simpledb.util;

import java.util.Iterator;

public class IteratorWrapper<T> implements Iterator<T> {
    private int currentPos;
    private T[] objects;

    public IteratorWrapper(final T[] objects) {
        this.objects = objects;
        this.currentPos = 0;
    }

    @Override
    public boolean hasNext() {
        return this.currentPos < this.objects.length && this.objects[this.currentPos] != null;
    }

    @Override
    public T next() {
        T value = this.objects[this.currentPos];
        this.currentPos++;
        return value;
    }
}
