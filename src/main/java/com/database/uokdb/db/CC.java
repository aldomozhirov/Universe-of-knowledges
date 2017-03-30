package com.database.uokdb.db;

interface CC {
	
    boolean ASSERT = true;

    boolean PARANOID = false;

    boolean LOG_FINE = false;

    boolean LOG_STORE = LOG_FINE;

    boolean LOG_STORE_RECORD = LOG_FINE;

    boolean LOG_STORE_ALLOC = LOG_FINE;

    boolean LOG_WAL_CONTENT = LOG_FINE;

    boolean LOG_EWRAP = LOG_FINE;

    boolean LOG_HTREEMAP = LOG_FINE;

    int DEFAULT_LOCK_SCALE = 16;

    int DEFAULT_CACHE_SIZE = 2048;

    String DEFAULT_CACHE = DBMaker.Keys.cache_disable;

    long DEFAULT_CACHE_EXECUTOR_PERIOD = 1000;

    int DEFAULT_FREE_SPACE_RECLAIM_Q = 5;

    boolean FAIR_LOCKS = false;


    int VOLUME_PAGE_SHIFT = 20; // 1 MB

    long VOLUME_PRINT_STACK_AT_OFFSET = 0;


    long DEFAULT_HTREEMAP_EXECUTOR_PERIOD = 1000;
    long DEFAULT_STORE_EXECUTOR_SCHED_RATE = 1000;

    long DEFAULT_METRICS_LOG_PERIOD = 10000;

    boolean METRICS_CACHE = true;
    boolean METRICS_STORE = true;

    int DEFAULT_ASYNC_WRITE_QUEUE_SIZE = 1024;

    Volume.VolumeFactory DEFAULT_MEMORY_VOLUME_FACTORY = Volume.ByteArrayVol.FACTORY;

    Volume.VolumeFactory DEFAULT_FILE_VOLUME_FACTORY = Volume.RandomAccessFileVol.FACTORY;

    int FILE_RETRY = 16;

    int FILE_LOCK_HEARTBEAT = 1000;

    boolean VOLUME_ZEROUT = true;

}