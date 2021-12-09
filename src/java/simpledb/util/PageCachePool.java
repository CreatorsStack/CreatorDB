package simpledb.util;



import simpledb.storage.Tuple;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// Cache pool for file pages(tuples), cache for fileIterator
public class PageCachePool {
    private final List<Iterator<Tuple>> pageCache;
    private Iterator<Tuple>             currentIterator;
    private int                         currentIndex;
    private int                         maxCacheNum;

    public PageCachePool(final double cacheRate, final int totalPage) {
        this.maxCacheNum = (int) cacheRate * totalPage;
        if (this.maxCacheNum < 1) {
            this.maxCacheNum = 1;
        }
        this.pageCache = new ArrayList<>();
    }

    public void addPage(final Iterator<Tuple> tupleIterator) {
        if (this.pageCache.size() < this.maxCacheNum) {
            this.pageCache.add(tupleIterator);
        }
    }

    public void init() {
        this.currentIndex = 0;
        if (this.pageCache.size() > 0) {
            this.currentIterator = this.pageCache.get(0);
        }
    }

    public void clear() {
        this.pageCache.clear();
        this.currentIterator = null;
    }

    public boolean hasNext() {
        if (this.currentIterator == null || this.pageCache.size() == 0) {
            return false;
        }
        if (this.currentIterator.hasNext()) {
            return true;
        }
        if (this.currentIndex + 1 < this.pageCache.size()) {
            this.currentIndex ++;
            this.currentIterator = this.pageCache.get(this.currentIndex);
            return this.currentIterator.hasNext();
        }
        return false;
    }

    public Tuple next() {
        if (hasNext()) {
            return this.currentIterator.next();
        }
        return null;
    }

    public int getMaxCacheNum() {
        return maxCacheNum;
    }
}
