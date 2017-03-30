package com.database.uokdb.db;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantLock;

@Deprecated
class LongConcurrentHashMap< V>

        implements Serializable  {
    private static final long serialVersionUID = 7249069246763182397L;

    static final int DEFAULT_INITIAL_CAPACITY = 16;


    public final long hashSalt = Double.doubleToLongBits(Math.random());

    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    static final int DEFAULT_CONCURRENCY_LEVEL = 16;
        transient int threshold;

        transient volatile HashEntry<V>[] table;

        final float loadFactor;

        Segment(int initialCapacity, float lf) {
            super(CC.FAIR_LOCKS);
            loadFactor = lf;
            setTable(HashEntry.<V>newArray(initialCapacity));
        }

        @SuppressWarnings("unchecked")
        static <V> Segment<V>[] newArray(int i) {
            return new Segment[i];
        }

        void setTable(HashEntry<V>[] newTable) {
            threshold = (int)(newTable.length * loadFactor);
            table = newTable;
        }

        HashEntry<V> getFirst(int hash) {
            HashEntry<V>[] tab = table;
            return tab[hash & (tab.length - 1)];
        }

        V readValueUnderLock(HashEntry<V> e) {
            lock();
            try {
                return e.value;
            } finally {
                unlock();
            }
        }

        V get(final long key, int hash) {
            if (count != 0) {
                HashEntry<V> e = getFirst(hash);
                while (e != null) {
                    if (e.hash == hash && key == e.key) {
                        V v = e.value;
                        if (v != null)
                            return v;
                        return readValueUnderLock(e);
                    }
                    e = e.next;
                }
            }
            return null;
        }

        boolean containsKey(final long key, int hash) {
            if (count != 0) { // read-volatile
                HashEntry<V> e = getFirst(hash);
                while (e != null) {
                    if (e.hash == hash && key == e.key)
                        return true;
                    e = e.next;
                }
            }
            return false;
        }

        boolean containsValue(Object value) {
            if (count != 0) { // read-volatile
                HashEntry<V>[] tab = table;
                //int len = tab.length;
                for (HashEntry<V> aTab : tab) {
                    for (HashEntry<V> e = aTab; e != null; e = e.next) {
                        V v = e.value;
                        if (v == null) // recheck
                            v = readValueUnderLock(e);
                        if (value.equals(v))
                            return true;
                    }
                }
            }
            return false;
        }

        boolean replace(long key, int hash, V oldValue, V newValue) {
            lock();
            try {
                HashEntry<V> e = getFirst(hash);
                while (e != null && (e.hash != hash || key!=e.key))
                    e = e.next;

                boolean replaced = false;
                if (e != null && oldValue.equals(e.value)) {
                    replaced = true;
                    e.value = newValue;
                }
                return replaced;
            } finally {
                unlock();
            }
        }

        V replace(long key, int hash, V newValue) {
            lock();
            try {
                HashEntry<V> e = getFirst(hash);
                while (e != null && (e.hash != hash || key != e.key))
                    e = e.next;

                V oldValue = null;
                if (e != null) {
                    oldValue = e.value;
                    e.value = newValue;
                }
                return oldValue;
            } finally {
                unlock();
            }
        }


        V put(long key, int hash, V value, boolean onlyIfAbsent) {
            lock();
            try {
                int c = count;
                if (c++ > threshold) // ensure capacity
                    rehash();
                HashEntry<V>[] tab = table;
                int index = hash & (tab.length - 1);
                HashEntry<V> first = tab[index];
                HashEntry<V> e = first;
                while (e != null && (e.hash != hash || key!=e.key))
                    e = e.next;

                V oldValue;
                if (e != null) {
                    oldValue = e.value;
                    if (!onlyIfAbsent)
                        e.value = value;
                }
                else {
                    oldValue = null;
                    ++modCount;
                    tab[index] = new HashEntry<V>(key, hash, first, value);
                    count = c; // write-volatile
                }
                return oldValue;
            } finally {
                unlock();
            }
        }

        void rehash() {
            HashEntry<V>[] oldTable = table;
            int oldCapacity = oldTable.length;
            if (oldCapacity >= MAXIMUM_CAPACITY)
                return;

            HashEntry<V>[] newTable = HashEntry.newArray(oldCapacity<<1);
            threshold = (int)(newTable.length * loadFactor);
            int sizeMask = newTable.length - 1;
            for (HashEntry<V> e : oldTable) {
                if (e != null) {
                    HashEntry<V> next = e.next;
                    int idx = e.hash & sizeMask;

                    if (next == null)
                        newTable[idx] = e;

                    else {
                        HashEntry<V> lastRun = e;
                        int lastIdx = idx;
                        for (HashEntry<V> last = next;
                             last != null;
                             last = last.next) {
                            int k = last.hash & sizeMask;
                            if (k != lastIdx) {
                                lastIdx = k;
                                lastRun = last;
                            }
                        }
                        newTable[lastIdx] = lastRun;

                        for (HashEntry<V> p = e; p != lastRun; p = p.next) {
                            int k = p.hash & sizeMask;
                            HashEntry<V> n = newTable[k];
                            newTable[k] = new HashEntry<V>(p.key, p.hash,
                                    n, p.value);
                        }
                    }
                }
            }
            table = newTable;
        }

        V remove(final long key, int hash, Object value) {
            lock();
            try {
                int c = count - 1;
                HashEntry<V>[] tab = table;
                int index = hash & (tab.length - 1);
                HashEntry<V> first = tab[index];
                HashEntry<V> e = first;
                while (e != null && (e.hash != hash || key!=e.key))
                    e = e.next;

                V oldValue = null;
                if (e != null) {
                    V v = e.value;
                    if (value == null || value.equals(v)) {
                        oldValue = v;
                       
                        ++modCount;
                        HashEntry<V> newFirst = e.next;
                        for (HashEntry<V> p = first; p != e; p = p.next)
                            newFirst = new HashEntry<V>(p.key, p.hash,
                                    newFirst, p.value);
                        tab[index] = newFirst;
                        count = c;
                    }
                }
                return oldValue;
            } finally {
                unlock();
            }
        }

        void clear() {
            if (count != 0) {
                lock();
                try {
                    HashEntry<V>[] tab = table;
                    for (int i = 0; i < tab.length ; i++)
                        tab[i] = null;
                    ++modCount;
                    count = 0; // write-volatile
                } finally {
                    unlock();
                }
            }
        }
    }

    public LongConcurrentHashMap(int initialCapacity,
                                 float loadFactor, int concurrencyLevel) {
        if (!(loadFactor > 0) || initialCapacity < 0 || concurrencyLevel <= 0)
            throw new IllegalArgumentException();

        if (concurrencyLevel > MAX_SEGMENTS)
            concurrencyLevel = MAX_SEGMENTS;

        int sshift = 0;
        int ssize = 1;
        while (ssize < concurrencyLevel) {
            ++sshift;
            ssize <<= 1;
        }
        segmentShift = 32 - sshift;
        segmentMask = ssize - 1;
        this.segments = Segment.newArray(ssize);

        if (initialCapacity > MAXIMUM_CAPACITY)
            initialCapacity = MAXIMUM_CAPACITY;
        int c = initialCapacity / ssize;
        if (c * ssize < initialCapacity)
            ++c;
        int cap = 1;
        while (cap < c)
            cap <<= 1;

        for (int i = 0; i < this.segments.length; ++i)
            this.segments[i] = new Segment<V>(cap, loadFactor);
    }

    public LongConcurrentHashMap(int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL);
    }

    public LongConcurrentHashMap() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL);
    }

    public boolean isEmpty() {
        final Segment<V>[] segments = this.segments;

        int[] mc = new int[segments.length];
        int mcsum = 0;
        for (int i = 0; i < segments.length; ++i) {
            if (segments[i].count != 0)
                return false;
            else
                mcsum += mc[i] = segments[i].modCount;
        }

        if (mcsum != 0) {
            for (int i = 0; i < segments.length; ++i) {
                if (segments[i].count != 0 ||
                        mc[i] != segments[i].modCount)
                    return false;
            }
        }
        return true;
    }

    public int size() {
        final Segment<V>[] segments = this.segments;
        long sum = 0;
        long check = 0;
        int[] mc = new int[segments.length];

        for (int k = 0; k < RETRIES_BEFORE_LOCK; ++k) {
            check = 0;
            sum = 0;
            int mcsum = 0;
            for (int i = 0; i < segments.length; ++i) {
                sum += segments[i].count;
                mcsum += mc[i] = segments[i].modCount;
            }
            if (mcsum != 0) {
                for (int i = 0; i < segments.length; ++i) {
                    check += segments[i].count;
                    if (mc[i] != segments[i].modCount) {
                        check = -1; // force retry
                        break;
                    }
                }
            }
            if (check == sum)
                break;
        }
        if (check != sum) {
            sum = 0;
            for (Segment<V> segment : segments) segment.lock();
            for (Segment<V> segment : segments) sum += segment.count;
            for (Segment<V> segment : segments) segment.unlock();
        }
        if (sum > Integer.MAX_VALUE)
            return Integer.MAX_VALUE;
        else
            return (int)sum;
    }

    
    public Iterator<V> valuesIterator() {
        return new ValueIterator();
    }

    
    public LongMapIterator<V> longMapIterator() {
        return new MapIterator();
    }

    public V get(long key) {
        final int hash = DataIO.longHash(key ^ hashSalt);
        return segmentFor(hash).get(key, hash);
    }

    public boolean containsKey(long key) {
        final int hash = DataIO.longHash(key ^ hashSalt);
        return segmentFor(hash).containsKey(key, hash);
    }

    public boolean containsValue(Object value) {
        if (value == null)
            throw new NullPointerException();

        // See explanation of modCount use above

        final Segment<V>[] segments = this.segments;
        int[] mc = new int[segments.length];

        // Try a few times without locking
        for (int k = 0; k < RETRIES_BEFORE_LOCK; ++k) {
            //int sum = 0;
            int mcsum = 0;
            for (int i = 0; i < segments.length; ++i) {
                //int c = segments[i].count;
                mcsum += mc[i] = segments[i].modCount;
                if (segments[i].containsValue(value))
                    return true;
            }
            boolean cleanSweep = true;
            if (mcsum != 0) {
                for (int i = 0; i < segments.length; ++i) {
                    //int c = segments[i].count;
                    if (mc[i] != segments[i].modCount) {
                        cleanSweep = false;
                        break;
                    }
                }
            }
            if (cleanSweep)
                return false;
        }
        // Resort to locking all segments
        for (Segment<V> segment : segments) segment.lock();
        boolean found = false;
        try {
            for (Segment<V> segment : segments) {
                if (segment.containsValue(value)) {
                    found = true;
                    break;
                }
            }
        } finally {
            for (Segment<V> segment : segments) segment.unlock();
        }
        return found;
    }

    public V put(long key, V value) {
        if (value == null)
            throw new NullPointerException();
        final int hash = DataIO.longHash(key ^ hashSalt);
        return segmentFor(hash).put(key, hash, value, false);
    }

    public V putIfAbsent(long key, V value) {
        if (value == null)
            throw new NullPointerException();
        final int hash = DataIO.longHash(key ^ hashSalt);
        return segmentFor(hash).put(key, hash, value, true);
    }

    public V remove(long key) {
        final int hash = DataIO.longHash(key ^ hashSalt);
        return segmentFor(hash).remove(key, hash, null);
    }

    public boolean remove(long key, Object value) {
        final int hash = DataIO.longHash(key ^ hashSalt);
        return value != null && segmentFor(hash).remove(key, hash, value) != null;
    }

    public boolean replace(long key, V oldValue, V newValue) {
        if (oldValue == null || newValue == null)
            throw new NullPointerException();
        final int hash = DataIO.longHash(key ^ hashSalt);
        return segmentFor(hash).replace(key, hash, oldValue, newValue);
    }

    public V replace(long key, V value) {
        if (value == null)
            throw new NullPointerException();
        final int hash = DataIO.longHash(key ^ hashSalt);
        return segmentFor(hash).replace(key, hash, value);
    }

    
    public void clear() {
        for (Segment<V> segment : segments) segment.clear();
    }

    abstract class HashIterator {
        int nextSegmentIndex;
        int nextTableIndex;
        HashEntry<V>[] currentTable;
        HashEntry< V> nextEntry;
        HashEntry< V> lastReturned;

        HashIterator() {
            nextSegmentIndex = segments.length - 1;
            nextTableIndex = -1;
            advance();
        }


        final void advance() {
            if (nextEntry != null && (nextEntry = nextEntry.next) != null)
                return;

            while (nextTableIndex >= 0) {
                if ( (nextEntry = currentTable[nextTableIndex--]) != null)
                    return;
            }

            while (nextSegmentIndex >= 0) {
                Segment<V> seg = segments[nextSegmentIndex--];
                if (seg.count != 0) {
                    currentTable = seg.table;
                    for (int j = currentTable.length - 1; j >= 0; --j) {
                        if ( (nextEntry = currentTable[j]) != null) {
                            nextTableIndex = j - 1;
                            return;
                        }
                    }
                }
            }
        }

        public boolean hasNext() { return nextEntry != null; }

        HashEntry<V> nextEntry() {
            if (nextEntry == null)
                throw new NoSuchElementException();
            lastReturned = nextEntry;
            advance();
            return lastReturned;
        }

        public void remove() {
            if (lastReturned == null)
                throw new IllegalStateException();
            LongConcurrentHashMap.this.remove(lastReturned.key);
            lastReturned = null;
        }
    }

    final class KeyIterator
            extends HashIterator
            implements Iterator<Long>
    {
        
        public Long next()        { return super.nextEntry().key; }
    }

    final class ValueIterator
            extends HashIterator
            implements Iterator<V>
    {
        
        public V next()        { return super.nextEntry().value; }
    }


    final class MapIterator extends HashIterator implements LongMapIterator<V>{

        private long key;
        private V value;

        
        public boolean moveToNext() {
            if(!hasNext()) return false;
            HashEntry<V> next = nextEntry();
            key = next.key;
            value = next.value;
            return true;
        }

        
        public long key() {
            return key;
        }

        
        public V value() {
            return value;
        }
    }



    /** Iterates over LongMap key and values without boxing long keys */
    public interface LongMapIterator<V>{
        boolean moveToNext();
        long key();
        V value();

        void remove();
    }

    
    public String toString(){
        final StringBuilder b = new StringBuilder();
        b.append(getClass().getSimpleName());
        b.append('[');
        boolean first = true;
        LongMapIterator<V> iter = longMapIterator();
        while(iter.moveToNext()){
            if(first){
                first = false;
            }else{
                b.append(", ");
            }
            b.append(iter.key());
            b.append(" => ");
            b.append(iter.value());
        }
        b.append(']');
        return b.toString();
    }

}