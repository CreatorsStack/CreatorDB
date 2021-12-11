package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private final Map<PageId, List<Lock>> lockMap;

    public LockManager() {
        this.lockMap = new ConcurrentHashMap<>();
    }

    public boolean tryAcquireLock(final PageId pageId, final TransactionId tid, final int lockType, final int timeout) {
        final long now = System.currentTimeMillis();
        while (true) {
            if (System.currentTimeMillis() - now >= timeout) {
                return false;
            }
            if (acquireLock(pageId, tid, lockType)) {
                return true;
            }
        }

    }

    public synchronized boolean acquireLock(final PageId pageId, final TransactionId tid, final int lockType) {
        // 1.If page is unlock, just return true
        if (!this.lockMap.containsKey(pageId)) {
            final Lock lock = new Lock(tid, lockType);
            final List<Lock> locks = new ArrayList<>();
            locks.add(lock);
            this.lockMap.put(pageId, locks);
            return true;
        }
        final List<Lock> locks = this.lockMap.get(pageId);

        // 2.Check whether this txn holds a lock
        for (final Lock lock : locks) {
            if (lock.getTid().equals(tid)) {
                if (lock.getLockType() == lockType) {
                    return true;
                }
                if (lock.getLockType() == 1) {
                    return true;
                }
                if (lock.getLockType() == 0 && locks.size() == 1) {
                    lock.setLockType(1);
                    return true;
                }
                return false;
            }
        }
        // 3.Check whether exists a writeLock
        if (locks.size() > 0 && locks.get(0).getLockType() == 1) {
            return false;
        }

        // 4.There already exists another locks, so we just can get a readLock
        if (lockType == 0) {
            final Lock lock = new Lock(tid, lockType);
            locks.add(lock);
            return true;
        }
        return false;
    }

    public synchronized boolean releaseLock(final PageId pageId, final TransactionId tid) {
        if (!this.lockMap.containsKey(pageId)) {
            return false;
        }
        final List<Lock> locks = this.lockMap.get(pageId);
        for (int i = 0; i < locks.size(); i++) {
            final Lock lock = locks.get(i);
            if (lock.getTid().equals(tid)) {
                locks.remove(lock);
                this.lockMap.put(pageId, locks);
                if (locks.size() == 0) {
                    this.lockMap.remove(pageId);
                }
                return true;
            }
        }
        return false;
    }

    public synchronized void releaseLockByTxn(final TransactionId tid) {
        this.lockMap.forEach((pid, locks) -> {
            if (holdsLock(pid, tid)) {
                releaseLock(pid, tid);
            }
        });
    }

    public synchronized boolean holdsLock(final PageId pageId, final TransactionId tid) {
        if (!this.lockMap.containsKey(pageId)) {
            return false;
        }
        final List<Lock> locks = this.lockMap.get(pageId);
        for (int i = 0; i < locks.size(); i++) {
            final Lock lock = locks.get(i);
            if (lock.getTid().equals(tid)) {
                return true;
            }
        }
        return false;
    }

}
