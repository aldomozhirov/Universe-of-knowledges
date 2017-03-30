package com.database.uokdb.db;


import java.io.File;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

public final class DBMaker{

    public static final Logger LOG = Logger.getLogger(DBMaker.class.getName());

    public static final String TRUE = "true";


    public interface Keys{
        String cache = "cache";

        String cacheSize = "cacheSize";
        String cache_disable = "disable";
        String cache_hashTable = "hashTable";
        String cache_hardRef = "hardRef";
        String cache_softRef = "softRef";
        String cache_weakRef = "weakRef";
        String cache_lru = "lru";
        String cacheExecutorPeriod = "cacheExecutorPeriod";

        String file = "file";

        String metrics = "metrics";
        String metricsLogInterval = "metricsLogInterval";

        String volume = "volume";
        String volume_fileChannel = "fileChannel";
        String volume_raf = "raf";
        String volume_mmapfIfSupported = "mmapfIfSupported";
        String volume_mmapf = "mmapf";
        String volume_byteBuffer = "byteBuffer";
        String volume_directByteBuffer = "directByteBuffer";
        String volume_unsafe = "unsafe";

        String fileMmapCleanerHack = "fileMmapCleanerHack";

        String fileLockDisable = "fileLockDisable";
        String fileLockHeartbeatEnable = "fileLockHeartbeatEnable";

        String lockScale = "lockScale";

        String lock = "lock";
        String lock_readWrite = "readWrite";
        String lock_single = "single";
        String lock_threadUnsafe = "threadUnsafe";

        String store = "store";
        String store_direct = "direct";
        String store_wal = "wal";
        String store_append = "append";
        String store_heap = "heap";
        String store_archive = "archive";
        String storeExecutorPeriod = "storeExecutorPeriod";

        String transactionDisable = "transactionDisable";

        String asyncWrite = "asyncWrite";
        String asyncWriteFlushDelay = "asyncWriteFlushDelay";
        String asyncWriteQueueSize = "asyncWriteQueueSize";

        String deleteFilesAfterClose = "deleteFilesAfterClose";
        String closeOnJvmShutdown = "closeOnJvmShutdown";

        String readOnly = "readOnly";

        String compression = "compression";
        String compression_lzf = "lzf";

        String encryptionKey = "encryptionKey";
        String encryption = "encryption";
        String encryption_xtea = "xtea";

        String checksum = "checksum";

        String freeSpaceReclaimQ = "freeSpaceReclaimQ";
        String commitFileSyncDisable = "commitFileSyncDisable";

        String snapshots = "snapshots";

        String strictDBGet = "strictDBGet";

        String fullTx = "fullTx";

        String allocateStartSize = "allocateStartSize";
        String allocateIncrement = "allocateIncrement";
        String allocateRecidReuseDisable = "allocateRecidReuseDisable";

    }

    public static Maker fileDB(File file){
        return new Maker(file);
    }



    public static final class Maker {
        public Fun.RecordCondition cacheCondition;
        public ScheduledExecutorService executor;
        public ScheduledExecutorService metricsExecutor;
        public ScheduledExecutorService cacheExecutor;

        public ScheduledExecutorService storeExecutor;
        public ClassLoader serializerClassLoader;
        public Map<String,ClassLoader> serializerClassLoaderRegistry;


        public Properties props = new Properties();

        public Maker(){}

        public Maker(File file) {
            props.setProperty(Keys.file, file.getPath());
        }

        public Maker cacheSize(int cacheSize){
            props.setProperty(Keys.cacheSize, "" + cacheSize);
            return this;
        }
        
        public Engine makeEngine(){

            if(storeExecutor==null) {
                storeExecutor = executor;
            }


            final boolean readOnly = propsGetBool(Keys.readOnly);
            final boolean fileLockDisable = propsGetBool(Keys.fileLockDisable) || propsGetBool(Keys.fileLockHeartbeatEnable);
            final String file = props.containsKey(Keys.file)? props.getProperty(Keys.file):"";
            final String volume = props.getProperty(Keys.volume);
            final String store = props.getProperty(Keys.store);

            if(readOnly && file.isEmpty())
                throw new UnsupportedOperationException("Can not open in-memory DB in read-only mode.");

            if(readOnly && !new File(file).exists() && !Keys.store_append.equals(store)){
                throw new UnsupportedOperationException("Can not open non-existing file in read-only mode.");
            }

            DataIO.HeartbeatFileLock heartbeatFileLock = null;
            if(propsGetBool(Keys.fileLockHeartbeatEnable) && file!=null && file.length()>0
                    && !readOnly){ 

                File lockFile = new File(file+".lock");
                heartbeatFileLock = new DataIO.HeartbeatFileLock(lockFile, CC.FILE_LOCK_HEARTBEAT);
                heartbeatFileLock.lock();
            }

            Engine engine;
            int lockingStrategy = 0;
            String lockingStrategyStr = props.getProperty(Keys.lock,Keys.lock_readWrite);
            if(Keys.lock_single.equals(lockingStrategyStr)){
                lockingStrategy = 1;
            }else if(Keys.lock_threadUnsafe.equals(lockingStrategyStr)) {
                lockingStrategy = 2;
            }

            final int lockScale = DataIO.nextPowTwo(propsGetInt(Keys.lockScale,CC.DEFAULT_LOCK_SCALE));

            final long allocateStartSize = propsGetLong(Keys.allocateStartSize,0L);
            final long allocateIncrement = propsGetLong(Keys.allocateIncrement,0L);
            final boolean allocateRecidReuseDisable = propsGetBool(Keys.allocateRecidReuseDisable);

            boolean cacheLockDisable = lockingStrategy!=0;
            byte[] encKey = propsGetXteaEncKey();
            final boolean snapshotEnabled =  propsGetBool(Keys.snapshots);
            if(Keys.store_heap.equals(store)) {
                engine = new StoreHeap(propsGetBool(Keys.transactionDisable), lockScale, lockingStrategy, snapshotEnabled);
            }else if(Keys.store_append.equals(store)){
                if(Keys.volume_byteBuffer.equals(volume)||Keys.volume_directByteBuffer.equals(volume))
                    throw new UnsupportedOperationException("Append Storage format is not supported with in-memory dbs");

                Volume.VolumeFactory volFac = extendStoreVolumeFactory(false);
                engine = new StoreAppend(
                        file,
                        volFac,
                        createCache(cacheLockDisable,lockScale),
                        lockScale,
                        lockingStrategy,
                        propsGetBool(Keys.checksum),
                        Keys.compression_lzf.equals(props.getProperty(Keys.compression)),
                        encKey,
                        propsGetBool(Keys.readOnly),
                        snapshotEnabled,
                        fileLockDisable,
                        heartbeatFileLock,
                        propsGetBool(Keys.transactionDisable),
                        storeExecutor,
                        allocateStartSize,
                        allocateIncrement
                );
            }else{
                Volume.VolumeFactory volFac = extendStoreVolumeFactory(false);
                boolean compressionEnabled = Keys.compression_lzf.equals(props.getProperty(Keys.compression));
                boolean asyncWrite = propsGetBool(Keys.asyncWrite) && !readOnly;

              if(asyncWrite) {
                    engine = new StoreCached(
                            file,
                            volFac,
                            createCache(cacheLockDisable, lockScale),
                            lockScale,
                            lockingStrategy,
                            propsGetBool(Keys.checksum),
                            compressionEnabled,
                            encKey,
                            propsGetBool(Keys.readOnly),
                            snapshotEnabled,
                            fileLockDisable,
                            heartbeatFileLock,
                            storeExecutor,
                            allocateStartSize,
                            allocateIncrement,
                            allocateRecidReuseDisable,
                            CC.DEFAULT_STORE_EXECUTOR_SCHED_RATE,
                            propsGetInt(Keys.asyncWriteQueueSize,CC.DEFAULT_ASYNC_WRITE_QUEUE_SIZE)
                    );
                }else{
                    engine = new StoreDirect(
                            file,
                            volFac,
                            createCache(cacheLockDisable, lockScale),
                            lockScale,
                            lockingStrategy,
                            propsGetBool(Keys.checksum),
                            compressionEnabled,
                            encKey,
                            propsGetBool(Keys.readOnly),
                            snapshotEnabled,
                            fileLockDisable,
                            heartbeatFileLock,
                            storeExecutor,
                            allocateStartSize,
                            allocateIncrement,
                            allocateRecidReuseDisable);
                }
            }

            if(engine instanceof Store){
                ((Store)engine).init();
            }

            if(readOnly)
                engine = new Engine.ReadOnlyWrapper(engine);


            if(propsGetBool(Keys.closeOnJvmShutdown)){
                engine = new Engine.CloseOnJVMShutdown(engine);
            }

            Fun.Pair<Integer,byte[]> check = null;
            try{
                check = (Fun.Pair<Integer, byte[]>) engine.get(Engine.RECID_RECORD_CHECK, Serializer.BASIC);
                if(check!=null){
                    if(check.a != Arrays.hashCode(check.b))
                        throw new RuntimeException("invalid checksum");
                }
            }catch(Throwable e){
                throw new DBException.WrongConfig("Error while opening store. Make sure you have right password, compression or encryption is well configured.",e);
            }
            if(check == null && !engine.isReadOnly()){
                byte[] b = new byte[127];
                if(encKey!=null) {
                    new SecureRandom().nextBytes(b);
                } else {
                    new Random().nextBytes(b);
                }
                check = new Fun.Pair(Arrays.hashCode(b), b);
                engine.update(Engine.RECID_RECORD_CHECK, check, Serializer.BASIC);
                engine.commit();
            }


            return engine;
        }

        public Store.Cache createCache(boolean disableLocks, int lockScale) {
            final String cache = props.getProperty(Keys.cache, CC.DEFAULT_CACHE);
            if(cacheExecutor==null) {
                cacheExecutor = executor;
            }

            long executorPeriod = propsGetLong(Keys.cacheExecutorPeriod, CC.DEFAULT_CACHE_EXECUTOR_PERIOD);

            if(Keys.cache_disable.equals(cache)){
                return null;
            }else if(Keys.cache_hashTable.equals(cache)){
                int cacheSize = propsGetInt(Keys.cacheSize, CC.DEFAULT_CACHE_SIZE) / lockScale;
                return new Store.Cache.HashTable(cacheSize,disableLocks);
            }else if (Keys.cache_hardRef.equals(cache)){
                int cacheSize = propsGetInt(Keys.cacheSize, CC.DEFAULT_CACHE_SIZE) / lockScale;
                return new Store.Cache.HardRef(cacheSize,disableLocks,cacheExecutor, executorPeriod);
            }else if (Keys.cache_weakRef.equals(cache)){
                return new Store.Cache.WeakSoftRef(true, disableLocks, cacheExecutor, executorPeriod);
            }else if (Keys.cache_softRef.equals(cache)){
                return new Store.Cache.WeakSoftRef(false, disableLocks, cacheExecutor,executorPeriod);
            }else if (Keys.cache_lru.equals(cache)){
                int cacheSize = propsGetInt(Keys.cacheSize, CC.DEFAULT_CACHE_SIZE) / lockScale;
                return new Store.Cache.LRU(cacheSize,disableLocks);
            }else{
                throw new IllegalArgumentException("unknown cache type: "+cache);
            }
        }

        public int propsGetInt(String key, int defValue){
            String ret = props.getProperty(key);
            if(ret==null) return defValue;
            return Integer.valueOf(ret);
        }

        public long propsGetLong(String key, long defValue){
            String ret = props.getProperty(key);
            if(ret==null) return defValue;
            return Long.valueOf(ret);
        }


        public boolean propsGetBool(String key){
            String ret = props.getProperty(key);
            return ret!=null && ret.equals(TRUE);
        }

        public byte[] propsGetXteaEncKey(){
            if(!Keys.encryption_xtea.equals(props.getProperty(Keys.encryption)))
                return null;
            return DataIO.fromHexa(props.getProperty(Keys.encryptionKey));
        }
        
        public DB make(){
            boolean strictGet = propsGetBool(Keys.strictDBGet);
            boolean deleteFilesAfterClose = propsGetBool(Keys.deleteFilesAfterClose);
            Engine engine = makeEngine();
            boolean dbCreated = false;
            boolean metricsLog = propsGetBool(Keys.metrics);
            long metricsLogInterval = propsGetLong(Keys.metricsLogInterval, metricsLog ? CC.DEFAULT_METRICS_LOG_PERIOD : 0);
            ScheduledExecutorService metricsExec2 = metricsLog? (metricsExecutor==null? executor:metricsExecutor) : null;

            try{
                DB db =  new  DB(
                        engine,
                        strictGet,
                        deleteFilesAfterClose,
                        executor,
                        false,
                        metricsExec2,
                        metricsLogInterval,
                        storeExecutor,
                        cacheExecutor,
                        makeClassLoader());
                dbCreated = true;
                return db;
            }finally {
                if(!dbCreated)
                    engine.close();
            }
        }
        
        protected Fun.Function1<Class, String> makeClassLoader() {
            if(serializerClassLoader==null &&
                    (serializerClassLoaderRegistry==null || serializerClassLoaderRegistry.isEmpty())){
                return null;
            }

            final ClassLoader serializerClassLoader2 = this.serializerClassLoader;
            final Map<String, ClassLoader> serializerClassLoaderRegistry2 =
                    new HashMap<String, ClassLoader>();
            if(this.serializerClassLoaderRegistry!=null){
                serializerClassLoaderRegistry2.putAll(this.serializerClassLoaderRegistry);
            }

            return new Fun.Function1<Class, String>() {
                @Override
                public Class run(String className) {
                    ClassLoader loader = serializerClassLoaderRegistry2.get(className);
                    if(loader == null)
                        loader = serializerClassLoader2;
                    if(loader == null)
                        loader = Thread.currentThread().getContextClassLoader();
                    return SerializerPojo.classForName(className, loader);
                }
            };
        }

        public static boolean JVMSupportsLargeMappedFiles() {
            String prop = System.getProperty("os.arch");
            if(prop!=null && prop.contains("64")) {
                String os = System.getProperty("os.name");
                if(os==null)
                    return false;
                os = os.toLowerCase();
                return !os.startsWith("windows");
            }

            return false;
        }

        public Maker closeOnJvmShutdown(){
            props.setProperty(Keys.closeOnJvmShutdown,TRUE);
            return this;
        }

        public int propsGetRafMode(){
            String volume = props.getProperty(Keys.volume);
            if(volume==null||Keys.volume_raf.equals(volume)){
                return 2;
            }else if(Keys.volume_mmapfIfSupported.equals(volume)){
                return JVMSupportsLargeMappedFiles()?0:2;
            }else if(Keys.volume_fileChannel.equals(volume)){
                return 3;
            }else if(Keys.volume_mmapf.equals(volume)){
                return 0;
            }
            return 2; 
        }




        public Volume.VolumeFactory  extendStoreVolumeFactory(boolean index) {
            String volume = props.getProperty(Keys.volume);
            boolean cleanerHackEnabled = propsGetBool(Keys.fileMmapCleanerHack);
            if(Keys.volume_byteBuffer.equals(volume))
                return Volume.ByteArrayVol.FACTORY;
            else if(Keys.volume_directByteBuffer.equals(volume))
                return cleanerHackEnabled?
                        Volume.MemoryVol.FACTORY_WITH_CLEANER_HACK:
                        Volume.MemoryVol.FACTORY;
            else if(Keys.volume_unsafe.equals(volume))
                return Volume.UNSAFE_VOL_FACTORY;
            int rafMode = propsGetRafMode();
            if(rafMode == 3)
                return Volume.FileChannelVol.FACTORY;
            boolean raf = rafMode!=0;
            if(raf && index && rafMode==1)
                raf = false;

            return raf?
                    Volume.RandomAccessFileVol.FACTORY:
                    (cleanerHackEnabled?
                            Volume.MappedFileVol.FACTORY_WITH_CLEANER_HACK:
                            Volume.MappedFileVol.FACTORY);
        }

    }

}