
package com.database.uokdb.db;

import java.io.Closeable;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DB implements Closeable {

    public static final Logger LOG = Logger.getLogger(DB.class.getName());
    public static final String METRICS_DATA_WRITE = "data.write";
    public static final String METRICS_RECORD_WRITE = "record.write";
    public static final String METRICS_DATA_READ = "data.read";
    public static final String METRICS_RECORD_READ = "record.read";
    public static final String METRICS_CACHE_HIT = "cache.hit";
    public static final String METRICS_CACHE_MISS = "cache.miss";


    public interface Keys{
        String type = ".type";

        String keySerializer = ".keySerializer";
        String valueSerializer = ".valueSerializer";
        String serializer = ".serializer";

        String counterRecids = ".counterRecids";

        String hashSalt = ".hashSalt";
        String segmentRecids = ".segmentRecids";

        String expire = ".expire";
        String expireMaxSize = ".expireMaxSize";
        String expireAccess = ".expireAccess";
        String expireStoreSize = ".expireStoreSize";
        String expireHeads = ".expireHeads";
        String expireTails = ".expireTails";
        String expireTick = ".expireTick";
        String expireTimeStart = ".expireTimeStart";

        String rootRecidRef = ".rootRecidRef";
        String maxNodeSize = ".maxNodeSize";
        String valuesOutsideNodes = ".valuesOutsideNodes";
        String numberOfNodeMetas = ".numberOfNodeMetas";

        String headRecid = ".headRecid";
        String tailRecid = ".tailRecid";
        String useLocks = ".useLocks";
        String size = ".size";
        String recid = ".recid";
        String headInsertRecid = ".headInsertRecid";

    }

    public final boolean strictDBGet;
    public final boolean deleteFilesAfterClose;

    public Engine engine;
    public Map<String, WeakReference<?>> namesInstanciated = new HashMap<String, WeakReference<?>>();

    public Map<IdentityWrapper, String> namesLookup =
            new ConcurrentHashMap<IdentityWrapper, String>();

    public SortedMap<String, Object> catalog;

    public ScheduledExecutorService executor = null;

    public SerializerPojo serializerPojo;

    public ScheduledExecutorService metricsExecutor;
    public ScheduledExecutorService storeExecutor;
    public ScheduledExecutorService cacheExecutor;

    public final Set<String> unknownClasses = new ConcurrentSkipListSet<String>();

    public final ReadWriteLock consistencyLock;

    public static class IdentityWrapper{

        final Object o;

        public IdentityWrapper(Object o) {
            this.o = o;
        }

        public int hashCode() {
            return System.identityHashCode(o);
        }

        public boolean equals(Object v) {
            return ((IdentityWrapper)v).o==o;
        }
    }

    public DB(final Engine engine){
        this(engine,false,false, null, false, null, 0, null, null, null);
    }

    public DB(
            final Engine engine,
            boolean strictDBGet,
            boolean deleteFilesAfterClose,
            ScheduledExecutorService executor,
            boolean lockDisable,
            ScheduledExecutorService metricsExecutor,
            long metricsLogInterval,
            ScheduledExecutorService storeExecutor,
            ScheduledExecutorService cacheExecutor,
            Fun.Function1<Class, String> classLoader
            ) {
        this.engine = engine;
        this.strictDBGet = strictDBGet;
        this.deleteFilesAfterClose = deleteFilesAfterClose;
        this.executor = executor;
        this.consistencyLock = lockDisable ?
                new Store.ReadWriteSingleLock(Store.NOLOCK) :
                new ReentrantReadWriteLock();

        this.metricsExecutor = metricsExecutor==null ? executor : metricsExecutor;
        this.storeExecutor = storeExecutor;
        this.cacheExecutor = cacheExecutor;

        serializerPojo = new SerializerPojo(
                new Fun.Function1<String, Object>() {
                    @Override
                    public String run(Object o) {
                        if(o==DB.this)
                            return "$$DB_OBJECT_Q!#!@#!#@9009a09sd";
                        return getNameForObject(o);
                    }
                },
                new Fun.Function1<Object, String>() {
                    @Override
                    public Object run(String name) {
                        Object ret = get(name);
                        if(ret == null && "$$DB_OBJECT_Q!#!@#!#@9009a09sd".equals(name))
                            return DB.this;
                        return ret;
                    }
                },
                new Fun.Function1Int<SerializerPojo.ClassInfo>() {
                    public SerializerPojo.ClassInfo run(int index) {
                        long[] classInfoRecids = DB.this.engine.get(Engine.RECID_CLASS_CATALOG, Serializer.RECID_ARRAY);
                        if(classInfoRecids==null || index<0 || index>=classInfoRecids.length)
                            return null;
                        return getEngine().get(classInfoRecids[index], serializerPojo.classInfoSerializer);
                    }
                },
                new Fun.Function0<SerializerPojo.ClassInfo[]>() {
                    public SerializerPojo.ClassInfo[] run() {
                        long[] classInfoRecids = engine.get(Engine.RECID_CLASS_CATALOG, Serializer.RECID_ARRAY);
                        SerializerPojo.ClassInfo[] ret = new SerializerPojo.ClassInfo[classInfoRecids==null?0:classInfoRecids.length];
                        for(int i=0;i<ret.length;i++){
                            ret[i] = engine.get(classInfoRecids[i],serializerPojo.classInfoSerializer);
                        }
                        return ret;
                    }
                },
        new Fun.Function1<Void, String>() {
                     public Void run(String className) {
                        unknownClasses.add(className);
                        return null;
                    }
                },
                classLoader,
                engine);
        reinit();

        if(metricsExecutor!=null && metricsLogInterval!=0){

            if(!CC.METRICS_CACHE){
                LOG.warning("MapDB was compiled without cache metrics. No cache hit/miss will be reported");
            }

            metricsExecutor.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    metricsLog();
                }
            }, metricsLogInterval, metricsLogInterval, TimeUnit.MILLISECONDS);
        }
    }

    public void metricsLog() {
        Map metrics = DB.this.metricsGet();
        String s = metrics.toString();
        LOG.info("Metrics: "+s);
    }

    public Map<String,Long> metricsGet() {
        Map ret = new TreeMap();
        Store s = Store.forEngine(engine);
        s.metricsCollect(ret);
        return Collections.unmodifiableMap(ret);
    }

    public void reinit() {
        catalog = BTreeMap.preinitCatalog(this);
    }

    public <A> A catGet(String name, A init){
        if(CC.ASSERT && ! (Thread.holdsLock(DB.this)))
            throw new AssertionError();
        A ret = (A) catalog.get(name);
        return ret!=null? ret : init;
    }


    public <A> A catGet(String name){
        if(CC.ASSERT && ! (Thread.holdsLock(DB.this)))
            throw new AssertionError();
        return (A) catalog.get(name);
    }

    public <A> A catPut(String name, A value){
        if(CC.ASSERT && ! (Thread.holdsLock(DB.this)))
            throw new AssertionError();
        
        catalog.put(name, value);
        return value;
    }

    public <A> A catPut(String name, A value, A retValueIfNull){
        if(CC.ASSERT && ! (Thread.holdsLock(DB.this)))
            throw new AssertionError();
        if(value==null) return retValueIfNull;
        catalog.put(name, value);
        return value;
    }

    public String getNameForObject(Object obj) {
        return namesLookup.get(new IdentityWrapper(obj));
    }

    
    public Object serializableOrPlaceHolder(Object o) {
        SerializerBase b = (SerializerBase)getDefaultSerializer();
        if(o == null || b.isSerializable(o)){
            try {
                DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
                b.serialize(out,o);
                return o;
            } catch (Exception e) {
                return Fun.PLACEHOLDER;
            }
        }

        return Fun.PLACEHOLDER;
    }
    
    public static class BTreeMapMaker{
        public final String name;
        public final DB db;

        public BTreeMapMaker(String name) {
            this(name,null);
        }

        public BTreeMapMaker(String name, DB db) {
            this.name = name;
            this.db = db;
            executor = db==null ? null : db.executor;
        }


        public int nodeSize = 32;
        public boolean valuesOutsideNodes = false;
        public boolean counter = false;
        private BTreeKeySerializer _keySerializer;
        private Serializer _keySerializer2;
        private Comparator _comparator;

        public Serializer<?> valueSerializer;

        public Iterator pumpSource;
        public Fun.Function1 pumpKeyExtractor;
        public Fun.Function1 pumpValueExtractor;
        public int pumpPresortBatchSize = -1;
        public boolean pumpIgnoreDuplicates = false;
        public boolean closeEngine = false;

        public Executor executor = null;

        public BTreeMapMaker nodeSize(int nodeSize){
            if(nodeSize>=BTreeMap.NodeSerializer.SIZE_MASK)
                throw new IllegalArgumentException("Too large max node size");
            this.nodeSize = nodeSize;
            return this;
        }

        public BTreeMapMaker valuesOutsideNodesEnable(){
            this.valuesOutsideNodes = true;
            return this;
        }

        public BTreeMapMaker counterEnable(){
            this.counter = true;
            return this;
        }

        public BTreeMapMaker keySerializer(BTreeKeySerializer<?,?> keySerializer){
            this._keySerializer = keySerializer;
            return this;
        }

        public BTreeMapMaker keySerializer(Serializer<?> serializer){
            this._keySerializer2 = serializer;
            return this;
        }

        public BTreeMapMaker keySerializer(Serializer<?> serializer, Comparator<?> comparator){
            this._keySerializer2 = serializer;
            this._comparator = comparator;
            return this;
        }

        public BTreeMapMaker keySerializerWrap(Serializer<?> serializer){
            return keySerializer(serializer);
        }

        public BTreeMapMaker valueSerializer(Serializer<?> valueSerializer){
            this.valueSerializer = valueSerializer;
            return this;
        }

        public BTreeMapMaker comparator(Comparator<?> comparator){
            this._comparator = comparator;
            return this;
        }

        public <K,V> BTreeMapMaker pumpSource(Iterator<K> keysSource,  Fun.Function1<V,K> valueExtractor){
            this.pumpSource = keysSource;
            this.pumpKeyExtractor = Fun.extractNoTransform();
            this.pumpValueExtractor = valueExtractor;
            return this;
        }


        public <K,V> BTreeMapMaker pumpSource(Iterator<Fun.Pair<K,V>> entriesSource){
            this.pumpSource = entriesSource;
            this.pumpKeyExtractor = Fun.extractKey();
            this.pumpValueExtractor = Fun.extractValue();
            return this;
        }

        public BTreeMapMaker pumpSource(NavigableMap m) {
            this.pumpSource =  m.descendingMap().entrySet().iterator();
            this.pumpKeyExtractor = Fun.extractMapEntryKey();
            this.pumpValueExtractor = Fun.extractMapEntryValue();
            return this;
        }

        public BTreeMapMaker pumpPresort(int batchSize){
            this.pumpPresortBatchSize = batchSize;
            return this;
        }

        public <K> BTreeMapMaker pumpIgnoreDuplicates(){
            this.pumpIgnoreDuplicates = true;
            return this;
        }

        public <K,V> BTreeMap<K,V> make(){
            if(db==null)
                throw new IllegalAccessError("This maker is not attached to any DB, it only hold configuration");
            return db.treeMapCreate(BTreeMapMaker.this);
        }

        public <K,V> BTreeMap<K,V> makeOrGet(){
            if(db==null)
                throw new IllegalAccessError("This maker is not attached to any DB, it only hold configuration");

            synchronized(db){
                //TODO add parameter check
                return (BTreeMap<K, V>) (db.catGet(name + Keys.type)==null?
                        make() :
                        db.treeMap(name, getKeySerializer(), valueSerializer));
            }
        }

        public BTreeKeySerializer getKeySerializer() {
            if(_keySerializer==null) {
                if (_keySerializer2 == null && _comparator!=null)
                    _keySerializer2 = db.getDefaultSerializer();
                if(_keySerializer2!=null)
                    _keySerializer = _keySerializer2.getBTreeKeySerializer(_comparator);
            }
            return _keySerializer;
        }

        public <V> BTreeMap<String, V> makeStringMap() {
            keySerializer(Serializer.STRING);
            return make();
        }

        public <V> BTreeMap<Long, V> makeLongMap() {
            keySerializer(Serializer.LONG);
            return make();
        }

        public BTreeMapMaker closeEngine() {
            closeEngine = true;
            return this;
        }


    }

    public class BTreeSetMaker{
        public final String name;


        public BTreeSetMaker(String name) {
            this.name = name;
        }

        public int nodeSize = 32;
        public boolean counter = false;

        private BTreeKeySerializer _serializer;
        private Serializer _serializer2;
        private Comparator _comparator;

        public Iterator<?> pumpSource;
        public int pumpPresortBatchSize = -1;
        public boolean pumpIgnoreDuplicates = false;
        public boolean standalone = false;

        public Executor executor = DB.this.executor;

        public BTreeSetMaker nodeSize(int nodeSize){
            this.nodeSize = nodeSize;
            return this;
        }

        public BTreeSetMaker counterEnable(){
            this.counter = true;
            return this;
        }

        public BTreeSetMaker serializer(BTreeKeySerializer serializer){
            this._serializer = serializer;
            return this;
        }

        public BTreeSetMaker serializer(Serializer serializer){
            this._serializer2 = serializer;
            return this;
        }
        
        public BTreeSetMaker serializer(Serializer serializer, Comparator comparator){
            this._serializer2 = serializer;
            this._comparator = comparator;
            return this;
        }
        public BTreeSetMaker comparator(Comparator<?> comparator){
            this._comparator = comparator;
            return this;
        }

        public BTreeKeySerializer getSerializer() {
            if(_serializer==null) {
                if (_serializer2 == null && _comparator!=null)
                    _serializer2 = getDefaultSerializer();
                if(_serializer2!=null)
                    _serializer = _serializer2.getBTreeKeySerializer(_comparator);
            }
            return _serializer;
        }

        public BTreeSetMaker pumpSource(Iterator<?> source){
            this.pumpSource = source;
            return this;
        }


        public BTreeSetMaker pumpSource(NavigableSet m) {
            this.pumpSource = m.descendingIterator();
            return this;
        }

        public <K> BTreeSetMaker pumpIgnoreDuplicates(){
            this.pumpIgnoreDuplicates = true;
            return this;
        }

        public BTreeSetMaker pumpPresort(int batchSize){
            this.pumpPresortBatchSize = batchSize;
            return this;
        }

        public BTreeSetMaker standalone() {
            this.standalone = true;
            return this;
        }


        public <K> NavigableSet<K> make(){
            return DB.this.treeSetCreate(BTreeSetMaker.this);
        }

        public <K> NavigableSet<K> makeOrGet(){
            synchronized (DB.this){
                //TODO add parameter check
                return (NavigableSet<K>) (catGet(name+Keys.type)==null?
                        make():
                        treeSet(name,getSerializer()));
            }
        }

        public NavigableSet<String> makeStringSet() {
            serializer(BTreeKeySerializer.STRING);
            return make();
        }

        public NavigableSet<Long> makeLongSet() {
            serializer(BTreeKeySerializer.LONG);
            return make();
        }

    }

    public <K> K checkPlaceholder(String nameCatParam, K fromConstructor) {
        K fromCatalog = catGet(nameCatParam);
        if(fromConstructor!=null){
            if(fromCatalog!= Fun.PLACEHOLDER && fromCatalog!=fromConstructor &&
                    !((SerializerBase)getDefaultSerializer()).equalsBinary(fromCatalog, fromConstructor)){
                LOG.warning(nameCatParam+" is defined in Name Catalog, but other serializer was passed as constructor argument. Using one from constructor argument");
            }
            fromCatalog = fromConstructor;
        }
        if(fromCatalog==Fun.PLACEHOLDER || fromCatalog==null){
            throw new DBException.UnknownSerializer(nameCatParam+" is not defined in Name Catalog nor constructor argument");
        }
        return fromCatalog;
    }

    synchronized public <K,V> BTreeMap<K,V> getTreeMap(String name){
        return treeMap(name);
    }

    synchronized public <K,V> BTreeMap<K,V> treeMap(String name) {
        return treeMap(name,(BTreeKeySerializer)null,null);
    }

    synchronized public <K,V> BTreeMap<K,V> treeMap(String name, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        if(keySerializer==null)
            keySerializer = getDefaultSerializer();
        return treeMap(name,keySerializer.getBTreeKeySerializer(null),valueSerializer);
    }

    public  <V> V namedPut(String name, Object ret) {
    namesInstanciated.put(name, new WeakReference<Object>(ret));
    namesLookup.put(new IdentityWrapper(ret), name);
    return (V) ret;
}
    
    synchronized public <K,V> BTreeMap<K,V> treeMap(String name, BTreeKeySerializer keySerializer, Serializer<V> valueSerializer){
        checkNotClosed();
        BTreeMap<K,V> ret = (BTreeMap<K,V>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + Keys.type, null);
        
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0,false);
                new DB(e).treeMap("a");
                
                return namedPut(name,
                        new DB(new Engine.ReadOnlyWrapper(e)).treeMap("a"));
            }

            BTreeMapMaker m = treeMapCreate(name);
            if(keySerializer!=null)
                m = m.keySerializer(keySerializer);
            if(valueSerializer!=null)
                m = m.valueSerializer(valueSerializer);
            return m.make();

        }
        checkType(type, "TreeMap");

        Object keySer2 = checkPlaceholder(name+Keys.keySerializer, keySerializer);
        Object valSer2 = checkPlaceholder(name+Keys.valueSerializer, valueSerializer);

        ret = new BTreeMap<K, V>(engine,
                false,
                (Long) catGet(name + Keys.rootRecidRef),
                catGet(name+Keys.maxNodeSize,32),
                catGet(name+Keys.valuesOutsideNodes,false),
                catGet(name+Keys.counterRecids,0L),
                (BTreeKeySerializer)keySer2,
                (Serializer<V>)valSer2,
                catGet(name+Keys.numberOfNodeMetas,0)
                );
        
        namedPut(name, ret);
        return ret;
    }

    public BTreeMapMaker createTreeMap(String name){
        return treeMapCreate(name);
    }

    public BTreeMapMaker treeMapCreate(String name){
        return new BTreeMapMaker(name,DB.this);
    }

    synchronized public <K,V> BTreeMap<K,V> treeMapCreate(final BTreeMapMaker m){
        String name = m.name;
        checkNameNotExists(name);
        

        BTreeKeySerializer keySerializer = fillNulls(m.getKeySerializer());
        catPut(name+Keys.keySerializer,serializableOrPlaceHolder(keySerializer));
        if(m.valueSerializer==null)
            m.valueSerializer = getDefaultSerializer();
        catPut(name+Keys.valueSerializer,serializableOrPlaceHolder(m.valueSerializer));

        if(m.pumpPresortBatchSize!=-1 && m.pumpSource!=null){
            final Comparator comp = keySerializer.comparator();
            final Fun.Function1 extr = m.pumpKeyExtractor;

            Comparator presortComp =  new Comparator() {
                @Override
                public int compare(Object o1, Object o2) {
                    return - comp.compare(extr.run(o1), extr.run(o2));
                }
            };

            m.pumpSource = Pump.sort(
                    m.pumpSource,
                    m.pumpIgnoreDuplicates,
                    m.pumpPresortBatchSize,
                    presortComp,
                    getDefaultSerializer(),
                    m.executor);
        }
        
        long counterRecid = !m.counter ?0L:engine.put(0L, Serializer.LONG);

        long rootRecidRef;
        if(m.pumpSource==null || !m.pumpSource.hasNext()){
            rootRecidRef = BTreeMap.createRootRef(engine,keySerializer,m.valueSerializer,m.valuesOutsideNodes,0);
        }else{
            rootRecidRef = Pump.buildTreeMap(
                    (Iterator<K>)m.pumpSource,
                    engine,
                    (Fun.Function1<K,K>)m.pumpKeyExtractor,
                    (Fun.Function1<V,K>)m.pumpValueExtractor,
                    m.pumpIgnoreDuplicates,m.nodeSize,
                    m.valuesOutsideNodes,
                    counterRecid,
                    keySerializer,
                    (Serializer<V>)m.valueSerializer,
                    m.executor
            );

        }
        
        BTreeMap<K,V> ret = new BTreeMap<K,V>(
                engine,
                m.closeEngine,
                catPut(name+Keys.rootRecidRef, rootRecidRef),
                catPut(name+Keys.maxNodeSize,m.nodeSize),
                catPut(name+Keys.valuesOutsideNodes,m.valuesOutsideNodes),
                catPut(name+Keys.counterRecids,counterRecid),
                keySerializer,
                (Serializer<V>)m.valueSerializer,
                catPut(m.name+Keys.numberOfNodeMetas,0)
                );
        
        catalog.put(name + Keys.type, "TreeMap");
        namedPut(name, ret);
        return ret;
    }

    public BTreeKeySerializer<?,?> fillNulls(BTreeKeySerializer<?,?> keySerializer) {
        if(keySerializer==null)
            return new BTreeKeySerializer.BasicKeySerializer(getDefaultSerializer(),Fun.COMPARATOR);
        if(keySerializer instanceof BTreeKeySerializer.ArrayKeySerializer) {
            BTreeKeySerializer.ArrayKeySerializer k = (BTreeKeySerializer.ArrayKeySerializer) keySerializer;

            Serializer<?>[] serializers = new Serializer[k.tsize];
            Comparator<?>[] comparators = new Comparator[k.tsize];
            
            for (int i = 0; i < k.tsize; i++) {
                serializers[i] = k.serializers[i] != null && k.serializers[i]!=Serializer.BASIC ? k.serializers[i] : getDefaultSerializer();
                comparators[i] = k.comparators[i] != null ? k.comparators[i] : Fun.COMPARATOR;
            }
            
            return new BTreeKeySerializer.ArrayKeySerializer(comparators, serializers);
        }
        
        return keySerializer;
    }

    public SortedMap<String, Object> getCatalog(){
        return catalog;
    }

    synchronized public <K> NavigableSet<K> getTreeSet(String name){
        return treeSet(name);
    }

    synchronized public <K> NavigableSet<K> treeSet(String name) {
        return treeSet(name, (BTreeKeySerializer)null);
    }

    synchronized public <K> NavigableSet<K> treeSet(String name, Serializer serializer) {
        if(serializer == null)
            serializer = getDefaultSerializer();
        return treeSet(name,serializer.getBTreeKeySerializer(null));
    }

    synchronized public <K> NavigableSet<K> treeSet(String name,BTreeKeySerializer serializer){
        checkNotClosed();
        NavigableSet<K> ret = (NavigableSet<K>) getFromWeakCollection(name);
        if(ret!=null) return ret;
        String type = catGet(name + Keys.type, null);
        if(type==null){
            checkShouldCreate(name);
            if(engine.isReadOnly()){
                Engine e = new StoreHeap(true,1,0,false);
                new DB(e).treeSet("a");
                return namedPut(name,
                        new DB(new Engine.ReadOnlyWrapper(e)).treeSet("a"));
            }
            
            BTreeSetMaker m = treeSetCreate(name);
            if(serializer!=null)
                m = m.serializer(serializer);
            return m.make();

        }
        checkType(type, "TreeSet");

        Object keySer2 = checkPlaceholder(name+Keys.serializer, serializer);

        
        ret = new BTreeMap<K, Object>(
                engine,
                false,
                (Long) catGet(name+Keys.rootRecidRef),
                catGet(name+Keys.maxNodeSize,32),
                false,
                catGet(name+Keys.counterRecids,0L),
                (BTreeKeySerializer)keySer2,
                null,
                catGet(name+Keys.numberOfNodeMetas,0)
        ).keySet();
        
        namedPut(name, ret);
        return ret;

    }

    synchronized public BTreeSetMaker createTreeSet(String name){
        return treeSetCreate(name);
    }

    synchronized public BTreeSetMaker treeSetCreate(String name){
         return new BTreeSetMaker(name);
    }

    synchronized public <K> NavigableSet<K> treeSetCreate(BTreeSetMaker m){
        checkNameNotExists(m.name);
        

        BTreeKeySerializer serializer = fillNulls(m.getSerializer());
        catPut(m.name+Keys.serializer,serializableOrPlaceHolder(serializer));

        if(m.pumpPresortBatchSize!=-1){
            m.pumpSource = Pump.sort(
                    m.pumpSource,
                    m.pumpIgnoreDuplicates,
                    m.pumpPresortBatchSize,
                    Collections.reverseOrder(serializer.comparator()),
                    getDefaultSerializer(),
                    m.executor);
        }

        long counterRecid = !m.counter ?0L:engine.put(0L, Serializer.LONG);
        long rootRecidRef;
        
        if(m.pumpSource==null || !m.pumpSource.hasNext()){
            rootRecidRef = BTreeMap.createRootRef(engine,serializer,null,false, 0);
        }else{
            rootRecidRef = Pump.buildTreeMap(
                    (Iterator<Object>)m.pumpSource,
                    engine,
                    Fun.extractNoTransform(),
                    null,
                    m.pumpIgnoreDuplicates,
                    m.nodeSize,
                    false,
                    counterRecid,
                    serializer,
                    null,
                    m.executor);
        }
        
        NavigableSet<K> ret = new BTreeMap<K,Object>(
                engine,
                m.standalone,
                catPut(m.name+Keys.rootRecidRef, rootRecidRef),
                catPut(m.name+Keys.maxNodeSize,m.nodeSize),
                false,
                catPut(m.name+Keys.counterRecids,counterRecid),
                serializer,
                null,
                catPut(m.name+Keys.numberOfNodeMetas,0)
        ).keySet();
        
        catalog.put(m.name + Keys.type, "TreeSet");
        namedPut(m.name, ret);
        return ret;
    }

    public void checkShouldCreate(String name) {
        if(strictDBGet) throw new NoSuchElementException("No record with this name was found: "+name);
    }

    synchronized public <E> E get(String name){
        
        String type = catGet(name+Keys.type);
        if(type==null) return null;
        if("TreeMap".equals(type)) return (E) treeMap(name);
        if("TreeSet".equals(type)) return (E) treeSet(name);
        
        throw new DBException.DataCorruption("Unknown type: "+name);
    }

    synchronized public boolean exists(String name){
        return catGet(name+Keys.type)!=null;
    }

    synchronized public Map<String,Object> getAll(){
        TreeMap<String,Object> ret= new TreeMap<String, Object>();
        
        for(String name:catalog.keySet()){
            if(!name.endsWith(Keys.type)) continue;
            
            name = name.substring(0,name.length()-5);
            ret.put(name,get(name));
        }

        return Collections.unmodifiableMap(ret);
    }

    synchronized public void rename(String oldName, String newName){
        if(oldName.equals(newName)) return;
        
        Map<String, Object> sub = catalog.tailMap(oldName);
        List<String> toRemove = new ArrayList<String>();
        
        for(String param:sub.keySet()){
            if(!param.startsWith(oldName)) break;

            String suffix = param.substring(oldName.length());
            catalog.put(newName+suffix, catalog.get(param));
            toRemove.add(param);
        }
        if(toRemove.isEmpty()) throw new NoSuchElementException("Could not rename, name does not exist: "+oldName);
        
        WeakReference old = namesInstanciated.remove(oldName);
        if(old!=null){
            Object old2 = old.get();
            if(old2!=null){
                namesLookup.remove(new IdentityWrapper(old2));
                namedPut(newName,old2);
            }
        }
        for(String param:toRemove) catalog.remove(param);
    }

    public void checkNameNotExists(String name) {
        if(catalog.get(name+Keys.type)!=null)
            throw new IllegalArgumentException("Name already used: "+name);
    }

    synchronized public void close(){
        if(engine == null)
            return;

        consistencyLock.writeLock().lock();
        try {

            if(metricsExecutor!=null && metricsExecutor!=executor && !metricsExecutor.isShutdown()){
                metricsExecutor.shutdown();
                metricsExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                metricsExecutor = null;
            }

            if(cacheExecutor!=null && cacheExecutor!=executor && !cacheExecutor.isShutdown()){
                cacheExecutor.shutdown();
                cacheExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                cacheExecutor = null;
            }

            if(storeExecutor!=null && storeExecutor!=executor && !storeExecutor.isShutdown()){
                storeExecutor.shutdown();
                storeExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                storeExecutor = null;
            }


            if (executor != null && !executor.isTerminated()) {
                executor.shutdown();
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                executor = null;
            }

            for (WeakReference r : namesInstanciated.values()) {
                Object rr = r.get();
                if (rr != null && rr instanceof Closeable)
                    ((Closeable) rr).close();
            }

            String fileName = deleteFilesAfterClose ? Store.forEngine(engine).fileName : null;
            engine.close();
            //dereference db to prevent memory leaks
            engine = Engine.CLOSED_ENGINE;
            namesInstanciated = Collections.unmodifiableMap(new HashMap());
            namesLookup = Collections.unmodifiableMap(new HashMap());

            if (deleteFilesAfterClose && fileName != null) {
                File f = new File(fileName);
                if (f.exists() && !f.delete()) {
                    //TODO file was not deleted, log warning
                }
                //TODO delete WAL files and append-only files
            }
        } catch (IOException e) {
            throw new IOError(e);
        } catch (InterruptedException e) {
            throw new DBException.Interrupted(e);
        }finally {
            consistencyLock.writeLock().unlock();
        }
    }

    synchronized public Object getFromWeakCollection(String name){
        WeakReference<?> r =  namesInstanciated.get(name);
        
        if(r==null) return null;
        
        Object o = r.get();
        if(o==null) namesInstanciated.remove(name);
        return o;
    }



    public void checkNotClosed() {
        if(engine == null) throw new IllegalAccessError("DB was already closed");
    }

    public synchronized  boolean isClosed(){
        return engine == null || engine.isClosed();
    }

    synchronized public void commit() {
        checkNotClosed();

        consistencyLock.writeLock().lock();
        try {
            String[] toBeAdded = unknownClasses.isEmpty() ? null : unknownClasses.toArray(new String[0]);

            if (toBeAdded != null) {
                long[] classInfoRecids = engine.get(Engine.RECID_CLASS_CATALOG, Serializer.RECID_ARRAY);
                long[] classInfoRecidsOrig = classInfoRecids;
                if(classInfoRecids==null)
                    classInfoRecids = new long[0];

                int pos = classInfoRecids.length;
                classInfoRecids = Arrays.copyOf(classInfoRecids,classInfoRecids.length+toBeAdded.length);

                for (String className : toBeAdded) {
                    SerializerPojo.ClassInfo classInfo = serializerPojo.makeClassInfo(className);
                    //persist and add new recids
                    classInfoRecids[pos++] = engine.put(classInfo,serializerPojo.classInfoSerializer);
                }
                if(!engine.compareAndSwap(Engine.RECID_CLASS_CATALOG, classInfoRecidsOrig, classInfoRecids, Serializer.RECID_ARRAY)){
                    LOG.log(Level.WARNING, "Could not update class catalog with new classes, CAS failed");
                }
            }


            engine.commit();

            if (toBeAdded != null) {
                for (String className : toBeAdded) {
                    unknownClasses.remove(className);
                }
            }
        }finally {
            consistencyLock.writeLock().unlock();
        }
    }

    synchronized public void rollback() {
        checkNotClosed();
        consistencyLock.writeLock().lock();
        try {
            engine.rollback();
        }finally {
            consistencyLock.writeLock().unlock();
        }
    }

    synchronized public void compact(){
        engine.compact();
    }

    synchronized public DB snapshot(){
        consistencyLock.writeLock().lock();
        try {
            Engine snapshot = TxEngine.createSnapshotFor(engine);
            return new DB(snapshot);
        }finally {
            consistencyLock.writeLock().unlock();
        }
    }

    public  Serializer getDefaultSerializer() {
        return serializerPojo;
    }

    public Engine getEngine() {
        return engine;
    }

    public void checkType(String type, String expected) {
        
        if(!expected.equals(type)) throw new IllegalArgumentException("Wrong type: "+type);
    }


    public ReadWriteLock consistencyLock(){
        return consistencyLock;
    }
}