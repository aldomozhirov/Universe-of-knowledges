package com.database.uokdb.db;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

public interface Engine  extends Closeable {

    long RECID_NAME_CATALOG = 1;

    long RECID_CLASS_CATALOG = 2;
    
    long RECID_RECORD_CHECK = 3;

    long RECID_LAST_RESERVED = 7;

    long RECID_FIRST = RECID_LAST_RESERVED+1;

    long preallocate();

    <A> long put(A value, Serializer<A> serializer);

    <A> A get(long recid, Serializer<A> serializer);

    <A> void update(long recid, A value, Serializer<A> serializer);

    <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer);

    <A> void delete(long recid, Serializer<A>  serializer);

    void close();

    public boolean isClosed();

    void commit();

    void rollback() throws UnsupportedOperationException;

    boolean isReadOnly();

    boolean canRollback();

    boolean canSnapshot();

    Engine snapshot() throws UnsupportedOperationException;

    Engine getWrappedEngine();

    void clearCache();


    void compact();


    abstract class ReadOnly implements Engine{

        @Override
        public long preallocate() {
            throw new UnsupportedOperationException("Read-only");
        }


        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            throw new UnsupportedOperationException("Read-only");
        }

        @Override
        public <A> long put(A value, Serializer<A> serializer) {
            throw new UnsupportedOperationException("Read-only");
        }


        @Override
        public void commit() {
            throw new UnsupportedOperationException("Read-only");
        }

        @Override
        public void rollback() {
            throw new UnsupportedOperationException("Read-only");
        }

        @Override
        public boolean isReadOnly() {
            return true;
        }


        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            throw new UnsupportedOperationException("Read-only");
        }

        @Override
        public <A> void delete(long recid, Serializer<A> serializer){
            throw new UnsupportedOperationException("Read-only");
        }



        @Override
        public void compact() {
            throw new UnsupportedOperationException("Read-only");
        }


    }

    final class ReadOnlyWrapper extends ReadOnly{


        public final Engine engine;


        public ReadOnlyWrapper(Engine engine){
            this.engine = engine;
        }

        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            return engine.get(recid, serializer);
        }

        @Override
        public void close() {
             engine.close();
        }

        @Override
        public boolean isClosed() {
            return engine.isClosed();
        }

        @Override
        public boolean canRollback() {
            return engine.canRollback();
        }


        @Override
        public Engine getWrappedEngine() {
            return engine;
        }

        @Override
        public void clearCache() {
            engine.clearCache();
        }


        @Override
        public boolean canSnapshot() {
            return engine.canSnapshot();
        }

        @Override
        public Engine snapshot() throws UnsupportedOperationException {
            return engine.snapshot();
        }

    }

    class CloseOnJVMShutdown implements Engine{


        final public AtomicBoolean shutdownHappened = new AtomicBoolean(false);

        final Runnable hookRunnable = new Runnable() {
            @Override
            public void run() {
                shutdownHappened.set(true);
                CloseOnJVMShutdown.this.hook = null;
                if(CloseOnJVMShutdown.this.isClosed())
                    return;
                CloseOnJVMShutdown.this.close();
            }
        };

        public final Engine engine;

        public Thread hook;

        public CloseOnJVMShutdown(Engine engine) {
            this.engine = engine;
            hook = new Thread(hookRunnable,"MapDB shutdown hook");
            Runtime.getRuntime().addShutdownHook(hook);
        }


        @Override
        public long preallocate() {
            return engine.preallocate();
        }

        @Override
        public <A> long put(A value, Serializer<A> serializer) {
            return engine.put(value,serializer);
        }

        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            return engine.get(recid,serializer);
        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            engine.update(recid,value,serializer);
        }

        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            return engine.compareAndSwap(recid,expectedOldValue,newValue,serializer);
        }

        @Override
        public <A> void delete(long recid, Serializer<A> serializer) {
            engine.delete(recid,serializer);
        }

        @Override
        public void close() {
            engine.close();
            if(!shutdownHappened.get() && hook!=null){
                Runtime.getRuntime().removeShutdownHook(hook);
            }
            hook = null;
        }

        @Override
        public boolean isClosed() {
            return engine.isClosed();
        }

        @Override
        public void commit() {
            engine.commit();
        }

        @Override
        public void rollback() throws UnsupportedOperationException {
            engine.rollback();
        }

        @Override
        public boolean isReadOnly() {
            return engine.isReadOnly();
        }

        @Override
        public boolean canRollback() {
            return engine.canRollback();
        }

        @Override
        public boolean canSnapshot() {
            return engine.canSnapshot();
        }

        @Override
        public Engine snapshot() throws UnsupportedOperationException {
            return engine.snapshot();
        }

        @Override
        public Engine getWrappedEngine() {
            return engine;
        }

        @Override
        public void clearCache() {
            engine.clearCache();
        }

        @Override
        public void compact() {
            engine.compact();
        }

    }

    Engine CLOSED_ENGINE = new Engine(){


        @Override
        public long preallocate() {
            throw new IllegalAccessError("already closed");
        }


        @Override
        public <A> long put(A value, Serializer<A> serializer) {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public <A> void delete(long recid, Serializer<A> serializer) {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public void close() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public boolean isClosed() {
            return true;
        }

        @Override
        public void commit() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public void rollback() throws UnsupportedOperationException {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public boolean isReadOnly() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public boolean canRollback() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public boolean canSnapshot() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public Engine snapshot() throws UnsupportedOperationException {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public Engine getWrappedEngine() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public void clearCache() {
            throw new IllegalAccessError("already closed");
        }

        @Override
        public void compact() {
            throw new IllegalAccessError("already closed");
        }


    };
}