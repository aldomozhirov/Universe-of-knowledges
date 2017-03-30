
package com.database.uokdb.db;


import java.io.Closeable;
import java.util.concurrent.ScheduledExecutorService;

public class TxMaker implements Closeable {

    private final boolean strictDBGet;
    public ScheduledExecutorService executor;

    public Engine engine;

    public final Fun.Function1<Class, String> serializerClassLoader;

    public TxMaker(Engine engine) {
        this(engine,false, null, null);
    }

    public TxMaker(
            Engine engine,
            boolean strictDBGet,
            ScheduledExecutorService executor,
            Fun.Function1<Class, String> serializerClassLoader) {
        if(engine==null)
            throw new IllegalArgumentException();
        if(!engine.canSnapshot())
            throw new IllegalArgumentException("Snapshot must be enabled for TxMaker");
        if(engine.isReadOnly())
            throw new IllegalArgumentException("TxMaker can not be used with read-only Engine");
        this.engine = engine;
        this.strictDBGet = strictDBGet;
        this.executor = executor;
        this.serializerClassLoader = serializerClassLoader;
    }

    public Engine getGlobalEngine(){
        return engine;
    }
    
    public DB makeTx(){
        Engine snapshot = engine.snapshot();
        if(snapshot.isReadOnly())
            throw new AssertionError();

        return new DB(snapshot,strictDBGet,false,executor, true, null, 0, null, null, serializerClassLoader);
    }

    public synchronized void close() {
        if (engine != null) {
            engine.close();
            engine = null;
        }
    }

    public <A> A execute(Fun.Function1<A, DB> txBlock) {
        for(;;){
            DB tx = makeTx();
                A a = txBlock.run(tx);
                if(!tx.isClosed())
                    tx.commit();
                return a;
            }
        }
    }

