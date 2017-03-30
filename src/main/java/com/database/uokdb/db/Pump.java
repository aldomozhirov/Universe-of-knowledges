package com.database.uokdb.db;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class Pump {


    private static final Logger LOG = Logger.getLogger(Pump.class.getName());


    public static <E> Iterator<E> sort(Iterator<E> source, boolean mergeDuplicates, final int batchSize,
                                       Comparator comparator, final Serializer<E> serializer) {
        return sort(source,mergeDuplicates,batchSize, comparator, serializer, null);
    }

    public static <E> Iterator<E> sort(Iterator<E> source, boolean mergeDuplicates, final int batchSize,
            Comparator comparator, final Serializer<E> serializer, Executor executor){
        if(batchSize<=0) throw new IllegalArgumentException();
        if(comparator==null)
            comparator=Fun.comparator();
        if(source==null)
            source = Fun.emptyIterator();

        int counter = 0;
        final Object[] presort = new Object[batchSize];
        final List<File> presortFiles = new ArrayList<File>();
        final List<Integer> presortCount2 = new ArrayList<Integer>();

        try{
            while(source.hasNext()){
                presort[counter]=source.next();
                counter++;

                if(counter>=batchSize){
                    //sort all items
                    arraySort(presort, presort.length, comparator ,executor);

                    //flush presort into temporary file
                    File f = File.createTempFile("mapdb","sort");
                    f.deleteOnExit();
                    presortFiles.add(f);
                    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f)));
                    for(Object e:presort){
                        serializer.serialize(out,(E)e);
                    }
                    out.close();
                    presortCount2.add(counter);
                    Arrays.fill(presort,0);
                    counter = 0;
                }
            }
            //now all records from source are fetch
            if(presortFiles.isEmpty()){
                //no presort files were created, so on-heap sorting is enough
                arraySort(presort, counter, comparator, executor);
                return arrayIterator(presort,0, counter);
            }

            final int[] presortCount = new int[presortFiles.size()];
            for(int i=0;i<presortCount.length;i++) presortCount[i] = presortCount2.get(i);
            //compose iterators which will iterate over data saved in files
            Iterator[] iterators = new Iterator[presortFiles.size()+1];
            final DataInputStream[] ins = new DataInputStream[presortFiles.size()];
            for(int i=0;i<presortFiles.size();i++){
                ins[i] = new DataInputStream(new BufferedInputStream(new FileInputStream(presortFiles.get(i))));
                final int pos = i;
                iterators[i] = new Iterator(){

                    @Override public boolean hasNext() {
                        return presortCount[pos]>0;
                    }

                    @Override public Object next() {
                        try {
                            Object ret =  serializer.deserialize(ins[pos],-1);
                            if(--presortCount[pos]==0){
                                ins[pos].close();
                                presortFiles.get(pos).delete();
                            }
                            return ret;
                        } catch (IOException e) {
                            throw new IOError(e);
                        }
                    }

                    @Override public void remove() {
                        //ignored
                    }

                };
            }

            //and add iterator over data on-heap
            arraySort(presort, counter, comparator, executor);
            iterators[iterators.length-1] = arrayIterator(presort,0,counter);

            //and finally sort presorted iterators and return iterators over them
            return sort(comparator, mergeDuplicates, iterators);

        }catch(IOException e){
            throw new IOError(e);
        }finally{
            for(File f:presortFiles) f.delete();
        }
    }

     static private Method parallelSortMethod;
    static{
        try {
            parallelSortMethod = Arrays.class.getMethod("parallelSort", Object[].class, int.class, int.class, Comparator.class);
        } catch (NoSuchMethodException e) {
            //java 6 & 7
            parallelSortMethod = null;
        }
    }

    public static void arraySort(Object[] array, int arrayLen, Comparator comparator,  Executor executor) {
        //if executor is specified, try to use parallel method in java 8
        if(executor!=null && parallelSortMethod!=null){
            //TODO this uses common pool, but perhaps we should use Executor instead
            try {
                parallelSortMethod.invoke(null, array, 0, arrayLen, comparator);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e); //TODO exception hierarchy here?
            }
        }
        Arrays.sort(array, 0, arrayLen, comparator);
    }

    public static <E> Iterator<E> sort(Comparator<E> comparator, final boolean mergeDuplicates, final Iterator<E>... iterators) {
        final Comparator comparator2 = comparator==null?Fun.COMPARATOR:comparator;
        return new Iterator<E>(){

            final NavigableSet<Object[]> items = new TreeSet<Object[]>(
                    new Fun.ArrayComparator(new Comparator[]{comparator2,Fun.COMPARATOR}));

            Object next = this; //is initialized with this so first `next()` will not throw NoSuchElementException

            {
                for(int i=0;i<iterators.length;i++){
                    if(iterators[i].hasNext()){
                        items.add(new  Object[]{iterators[i].next(), i});
                    }
                }
                next();
            }


            @Override public boolean hasNext() {
                return next!=null;
            }

            @Override public E next() {
                if(next == null)
                    throw new NoSuchElementException();

                Object oldNext = next;

                Object[] lo = items.pollFirst();
                if(lo == null){
                    next = null;
                    return (E) oldNext;
                }

                next = lo[0];

                if(oldNext!=this && comparator2.compare(oldNext,next)>0){
                    throw new IllegalArgumentException("One of the iterators is not sorted");
                }

                Iterator iter = iterators[(Integer)lo[1]];
                if(iter.hasNext()){
                    items.add(new Object[]{iter.next(),lo[1]});
                }

                if(mergeDuplicates){
                    while(true){
                        Iterator<Object[]> subset = Fun.filter(items,next).iterator();
                        if(!subset.hasNext())
                            break;
                        List<Object[]> subset2 = new LinkedList<Object[]>();
                        while(subset.hasNext())
                            subset2.add(subset.next());
                        List<Object[]> toadd = new ArrayList<Object[]>();
                        for(Object[] t:subset2){
                            items.remove(t);
                            iter = iterators[(Integer)t[1]];
                            if(iter.hasNext())
                                toadd.add(new Object[]{iter.next(),t[1]});
                        }
                        items.addAll(toadd);
                    }
                }


                return (E) oldNext;
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static <E> Iterator<E> merge(Executor executor, final Iterator... iters){
        if(iters.length==0)
            return Fun.emptyIterator();

        final Iterator<E> ret = new Iterator<E>() {
                int i = 0;
                Object next = this;

                {
                    next();
                }

                @Override
                public boolean hasNext() {
                    return next != null;
                }

                @Override
                public E next() {
                    if (next == null)
                        throw new NoSuchElementException();

                    //move to next iterator if necessary
                    while (!iters[i].hasNext()) {
                        i++;
                        if (i == iters.length) {
                            //reached end of iterators
                            Object ret = next;
                            next = null;
                            return (E) ret;
                        }
                    }

                    //take next item from iterator
                    Object ret = next;
                    next = iters[i].next();
                    return (E) ret;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };


        if(executor == null){
            //single threaded
            return ret;
        }

        final Object poisonPill = new Object();

        //else perform merge in separate thread and use blocking queue
        final BlockingQueue q = new ArrayBlockingQueue(128);
        //feed blocking queue in separate thread
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    try {
                        while (ret.hasNext())
                            q.put(ret.next());
                    } finally {
                        q.put(poisonPill); //TODO poison pill should be send in non blocking way, perhaps remove elements?
                    }
                } catch (InterruptedException e) {
                    LOG.log(Level.SEVERE, "feeder failed", e);
                }
            }
        });

        return poisonPillIterator(q,poisonPill);
    }

    public static <E> Iterator<E> poisonPillIterator(final BlockingQueue<E> q, final Object poisonPill) {

        return new Iterator<E>() {

            E next = getNext();

            private E getNext() {
                try {
                    E ret = q.take();
                    if(ret==poisonPill)
                        return null;
                    return ret;
                } catch (InterruptedException e) {
                    throw new DBException.Interrupted(e);
                }

            }

            @Override
            public boolean hasNext() {
                return next!=null;
            }

            @Override
            public E next() {
                E ret = next;
                if(ret == null)
                    throw new NoSuchElementException();
                next = getNext();
                return ret;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static  <E,K,V> long buildTreeMap(Iterator<E> source,
                                             Engine engine,
                                             Fun.Function1<K, E> keyExtractor,
                                             Fun.Function1<V, E> valueExtractor,
                                             boolean ignoreDuplicates,
                                             int nodeSize,
                                             boolean valuesStoredOutsideNodes,
                                             long counterRecid,
                                             BTreeKeySerializer keySerializer,
                                             Serializer<V> valueSerializer,
                                             Executor executor){

        //TODO upper levels of tree  could be created in separate thread

        if(keyExtractor==null)
            keyExtractor= (Fun.Function1<K, E>) Fun.extractNoTransform();
        if(valueSerializer==null){
            //this is set
            valueSerializer = (Serializer<V>) BTreeMap.BOOLEAN_PACKED;
            if(valueExtractor!=null)
                throw new IllegalArgumentException();
            valueExtractor = new Fun.Function1() {
                @Override
                public Object run(Object e) {
                    return Boolean.TRUE;
                }
            };
        }
        Serializer valueNodeSerializer = valuesStoredOutsideNodes ? BTreeMap.VALREF_SERIALIZER : valueSerializer;

        // update source iterator with new one, which just ignores duplicates
        if(ignoreDuplicates){
            source = ignoreDuplicatesIterator(source,keySerializer.comparator(), keyExtractor);
        }

        source = checkSortedIterator(source,keySerializer.comparator(), keyExtractor);

        final double NODE_LOAD = 0.75;
        // split if node is bigger than this
        final int maxNodeSize = (int) (nodeSize * NODE_LOAD);

        // temporary serializer for nodes
        Serializer<BTreeMap.BNode> nodeSerializer = new BTreeMap.NodeSerializer(valuesStoredOutsideNodes,keySerializer,valueNodeSerializer,0);

        //hold tree structure
        ArrayList<ArrayList<K>> dirKeys = new ArrayList();
        dirKeys.add(new ArrayList());
        ArrayList<ArrayList<Long>> dirRecids = new ArrayList();
        dirRecids.add(arrayList(0L));

        ArrayList<K> leafKeys = new ArrayList<K>();
        ArrayList<Object> leafValues = new ArrayList<Object>();

        long counter = 0;
        long rootRecid = 0;
        long lastLeafRecid = 0;

        SOURCE_LOOP:
        while(source.hasNext()){
            E iterNext = source.next();
            final boolean isLeftMost = !source.hasNext();
            counter++;

            final K key = keyExtractor.run(iterNext);

            Object value = valueExtractor.run(iterNext);
            if(valuesStoredOutsideNodes) {
                long recid = engine.put((V) value, valueSerializer);
                value = new BTreeMap.ValRef(recid);
            }

            leafKeys.add(key);


            // if is not last and is small enough, do not split
            if(!isLeftMost && leafKeys.size()<=maxNodeSize) {
                leafValues.add(value);
                continue SOURCE_LOOP;
            }

            if(isLeftMost) {
                leafValues.add(value);
            }

            Collections.reverse(leafKeys);
            Collections.reverse(leafValues);

            BTreeMap.LeafNode leaf = new BTreeMap.LeafNode(
                    keySerializer.arrayToKeys(leafKeys.toArray()),
                    isLeftMost,             //left most
                    lastLeafRecid==0,   //right most
                    false,
                    valueNodeSerializer.valueArrayFromArray(leafValues.toArray()),
                    lastLeafRecid
            );

            lastLeafRecid = engine.put(leaf,nodeSerializer);

            //handle case when there is only single leaf and no dirs, in that case it will become root
            if(isLeftMost && dirKeys.get(0).size()==0){
                rootRecid = lastLeafRecid;
                break SOURCE_LOOP;
            }

            //update parent directory
            K leafLink = leafKeys.get(0);

            dirKeys.get(0).add(leafLink);
            dirRecids.get(0).add(lastLeafRecid);

            leafKeys.clear();
            leafValues.clear();

            if(!isLeftMost){
                leafKeys.add(key);
                leafKeys.add(key);
                leafValues.add(value);
            }


            // iterate over keys and save them if too large or is last
            for(int level=0;
                level<dirKeys.size();
                level++){

                ArrayList<K> keys = dirKeys.get(level);

                //break loop if current level does not need saving
                //that means this is not last entry and size is small enough
                if(!isLeftMost && keys.size()<=maxNodeSize){
                    continue SOURCE_LOOP;
                }
                if(isLeftMost){
                    //remove redundant first key
                    keys.remove(keys.size()-1);
                }


                //node needs saving

                Collections.reverse(keys);
                List<Long> recids = dirRecids.get(level);
                Collections.reverse(recids);

                boolean isRightMost = (level+1 == dirKeys.size());

                //construct node
                BTreeMap.DirNode dir = new BTreeMap.DirNode(
                    keySerializer.arrayToKeys(keys.toArray()),
                    isLeftMost,
                    isRightMost,
                    false,
                    toLongArray(recids)
                );

                //finally save
                long dirRecid = engine.put(dir,nodeSerializer);

                //if its both most left and most right, save it as new root
                if(isLeftMost && isRightMost) {
                    rootRecid = dirRecid;
                    break SOURCE_LOOP;
                }

                //prepare next directory at the same level, clear and add link to just saved node
                K linkKey = keys.get(0);
                keys.clear();
                recids.clear();
                keys.add(linkKey);
                recids.add(dirRecid);

                //now update directory at parent level
                if(dirKeys.size()==level+1){
                    //dir is empty, so it needs updating
                    dirKeys.add(new ArrayList<K>());
                    dirRecids.add(arrayList(0L));
                }
                dirKeys.get(level+1).add(linkKey);
                dirRecids.get(level+1).add(dirRecid);
            }
        }

        //handle empty iterator, insert empty node
        if(rootRecid == 0) {
            BTreeMap.LeafNode emptyRoot = new BTreeMap.LeafNode(
                    keySerializer.emptyKeys(),
                    true,
                    true,
                    false,
                    valueNodeSerializer.valueArrayEmpty(),
                    0L);

            rootRecid = engine.put(emptyRoot, nodeSerializer);
        }

        if(counterRecid!=0)
            engine.update(counterRecid,counter,Serializer.LONG);


        return engine.put(rootRecid,Serializer.RECID);
    }

    private static <E,K> Iterator<E> checkSortedIterator(final Iterator<E> source, final Comparator comparator, final Fun.Function1<K, E> keyExtractor) {
        return new Iterator<E>() {

            E next = source.hasNext()?
                    source.next():null;


            E advance(){
                if(!source.hasNext())
                    return null;
                E ret = source.next();
                //check order

                int compare = comparator.compare(
                        keyExtractor.run(ret),
                        keyExtractor.run(next));
                if(compare==0){
                    throw new DBException.PumpSourceDuplicate(next);
                }
                if(compare>0) {
                    throw new DBException.PumpSourceNotSorted();
                }

                return ret;
            }

            @Override
            public boolean hasNext() {
                return next!=null;
            }

            @Override
            public E next() {
                if(next==null)
                    throw new NoSuchElementException();

                E ret = next;
                next = advance();
                return ret;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

    }

    private static <E,K> Iterator<E> ignoreDuplicatesIterator(final Iterator<E> source, final Comparator<K> comparator, final Fun.Function1<K, E> keyExtractor) {
        return new Iterator<E>() {

            E next = source.hasNext()?
                    source.next():null;


            E advance(){
                while(source.hasNext()){
                    E n = source.next();
                    if(comparator.compare(
                            keyExtractor.run(n),
                            keyExtractor.run(next))
                            ==0){
                        continue; //ignore duplicate
                    }
                    return n; // new element
                }
                return null; //no more entries in iterator
            }

            @Override
            public boolean hasNext() {
                return next!=null;
            }

            @Override
            public E next() {
                if(next==null)
                    throw new NoSuchElementException();

                E ret = next;
                next = advance();
                return ret;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static Object toLongArray(List<Long> child) {
        boolean allInts = true;
        for(Long l:child){
            if(l>Integer.MAX_VALUE) {
                allInts = false;
                break;
            }

        }
        if(allInts){
            int[] ret = new int[child.size()];
            for(int i=0;i<ret.length;i++){
                ret[i] = child.get(i).intValue();
            }
            return ret;
        }else{
            long[] ret = new long[child.size()];
            for(int i=0;i<ret.length;i++){
                ret[i] = child.get(i);
            }
            return ret;
        }
    }

    private static <E> ArrayList<E> arrayList(E item){
        ArrayList<E> ret = new ArrayList<E>();
        ret.add(item);
        return ret;
    }

    private static <E> Iterator<E> arrayIterator(final Object[] array, final int fromIndex, final int toIndex) {
        return new Iterator<E>(){

            int index = fromIndex;

            @Override
            public boolean hasNext() {
                return index<toIndex;
            }

            @Override
            public E next() {
                if(index>=toIndex) throw new NoSuchElementException();
                return (E) array[index++];
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static void copy(DB src, DB target) {
        //TODO implement
    }

    public static void backupFull(DB db, OutputStream out) {
        Store store = Store.forDB(db);
        store.backup(out, false);
    }

    public static DB backupFullRestore(DBMaker.Maker maker, InputStream in) {
        DB db = maker.make();
        Store store = Store.forDB(db);
        store.backupRestore(new InputStream[]{in});
        return db;
    }

    public static void backupIncremental(DB db, File backupDir) {
        try {
            File[] files = backupDir.listFiles();
            boolean isEmpty = (files.length==0);

            //find maximal timestamp, increase current if necessary
            long timestamp = System.currentTimeMillis();
            long lastTimestamp = 0;
            for(File f:files){
                String num = nameWithoutExt(f);
                long fTimestamp = Long.valueOf(num);
                timestamp = Math.max(fTimestamp+1, timestamp);
                lastTimestamp = Math.max(lastTimestamp, fTimestamp);
            }

            File file = new File(backupDir, "" + timestamp + (isEmpty?".full":".inc"));

            FileOutputStream out = new FileOutputStream(file);
            Store store = Store.forDB(db);

            //write header
            DataOutputStream out2 = new DataOutputStream(out);
            out2.writeInt(StoreDirect.HEADER);
            out2.writeInt(0); //checksum
            out2.writeLong(store.makeFeaturesBitmap());
            out2.writeLong(0); //file size
            out2.writeLong(timestamp);
            out2.writeLong(lastTimestamp);

            store.backup(out, true);
            out.flush();
            out.close();
        }catch(IOException e){
            throw new DBException.VolumeIOError(e);
        }
    }

    public static DB backupIncrementalRestore(DBMaker.Maker maker, File backupDir) {
        try{
            File[] files = backupDir.listFiles();

            //sort by timestamp
            Arrays.sort(files, new Comparator<File>(){
                @Override
                public int compare(File o1, File o2) {
                    long n1 = Long.valueOf(nameWithoutExt(o1));
                    long n2 = Long.valueOf(nameWithoutExt(o2));
                    return Fun.compareLong(n1,n2);
                }
            });

            InputStream[] ins = new InputStream[files.length];
            for(int i=0;i<ins.length;i++){
                ins[i] = new FileInputStream(files[i]);
                DataIO.skipFully(ins[i], 40);
            }

            DB db = maker.make();
            Store store = Store.forDB(db);
            store.backupRestore(ins);
            return db;
        }catch(IOException e){
            throw new DBException.VolumeIOError(e);
        }
    }


    public static String nameWithoutExt(File f) {
        String num = f.getName();
        num = num.substring(0, num.indexOf('.'));
        return num;
    }

}