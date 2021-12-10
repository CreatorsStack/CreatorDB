package simpledb.util;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.storage.DbFileIterator;
import simpledb.storage.HeapPage;
import simpledb.storage.HeapPageId;
import simpledb.storage.Tuple;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
    private final int           totalPage;
    private final TransactionId transactionId;
    private final int           tableId;
    private int                 currentPageId;
    private PageCachePool       pageCachePool;

    public HeapFileIterator(final int totalPages, final TransactionId transactionId, final int tableId) {
        this.totalPage = totalPages;
        this.transactionId = transactionId;
        this.tableId = tableId;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.pageCachePool = new PageCachePool(0.2, this.totalPage);
        this.currentPageId = 0;
        cacheFilePages();
    }

    // Cache pages as many as have
    public void cacheFilePages() {
        this.pageCachePool.clear();
        int i = this.currentPageId;
        for (; i < this.currentPageId + this.pageCachePool.getMaxCacheNum() && i < this.totalPage; i++) {
            try {
                final HeapPageId pageId = new HeapPageId(this.tableId, i);
                final HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.transactionId, pageId,
                    Permissions.READ_ONLY);
                this.pageCachePool.addPage(page.iterator());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.currentPageId = i;
        this.pageCachePool.init();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (this.pageCachePool == null) {
            return false;
        }
        if (this.pageCachePool.hasNext()) {
            return true;
        }
        // Cache another batch of pages
        while (!this.pageCachePool.hasNext()) {
            cacheFilePages();
            if (this.totalPage == this.currentPageId) {
                break;
            }
        }
        return this.pageCachePool.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (hasNext()) {
            return this.pageCachePool.next();
        }
        throw new NoSuchElementException("The Iterator don't have more elements");
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        open();
    }

    @Override
    public void close() {
        if (this.pageCachePool != null) {
            this.pageCachePool.clear();
            this.pageCachePool = null;
        }
    }
}
