package com.database.uokdb.db;

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public final class Fun {

    public static final Object PLACEHOLDER = new Object(){
        @Override public String toString() {
            return "Fun.PLACEHOLDER";
        }
    };

	public static <T> Comparator<T> comparator(){
		return Fun.COMPARATOR;
	}
	
	public static <T> Comparator<T> reverseComparator(){
		return Fun.REVERSE_COMPARATOR;
	}
	
    @SuppressWarnings("rawtypes")
	public static final Comparator COMPARATOR = new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
            return o1.compareTo(o2);
        }
    };

    @SuppressWarnings("rawtypes")
	public static final Comparator REVERSE_COMPARATOR = new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
            return -COMPARATOR.compare(o1,o2);
        }
    };

    public static final Iterator EMPTY_ITERATOR = new ArrayList(0).iterator();

    public static <T> Iterator<T> emptyIterator(){
    	return EMPTY_ITERATOR;
    }

    private Fun(){}

    static public boolean eq(Object a, Object b) {
        return a==b || (a!=null && a.equals(b));
    }

    public static long roundUp(long number, long roundUpToMultipleOf) {
        return ((number+roundUpToMultipleOf-1)/(roundUpToMultipleOf))*roundUpToMultipleOf;
    }

    public static long roundDown(long number, long roundDownToMultipleOf) {
        return number  - number % roundDownToMultipleOf;
    }

    static String toString(Object keys) {
        if(keys instanceof long[])
            return Arrays.toString((long[]) keys);
        else if(keys instanceof int[])
            return Arrays.toString((int[]) keys);
        else if(keys instanceof byte[])
            return Arrays.toString((byte[]) keys);
        else if(keys instanceof char[])
            return Arrays.toString((char[]) keys);
        else if(keys instanceof float[])
            return Arrays.toString((float[]) keys);
        else if(keys instanceof double[])
            return Arrays.toString((double[]) keys);
        else  if(keys instanceof boolean[])
            return Arrays.toString((boolean[]) keys);
        else  if(keys instanceof Object[])
            return Arrays.toString((Object[]) keys);
        else
            return keys.toString();
    }

    public static boolean arrayContains(long[] longs, long val) {
        for(long val2:longs){
            if(val==val2)
                return true;
        }
        return false;
    }

    static public final class Pair<A,B> implements Comparable<Pair<A,B>>, Serializable {

    	private static final long serialVersionUID = -8816277286657643283L;
		
		final public A a;
        final public B b;

        public Pair(A a, B b) {
            this.a = a;
            this.b = b;
        }

        public Pair(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            this.a = (A) serializer.deserialize(in, objectStack);
            this.b =  (B) serializer.deserialize(in, objectStack);
        }


        @Override public int compareTo(Pair<A,B> o) {
            int i = ((Comparable<A>)a).compareTo(o.a);
            if(i!=0)
                return i;
            return ((Comparable<B>)b).compareTo(o.b);
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Pair<?, ?> t = (Pair<?,?>) o;
            return eq(a,t.a) && eq(b,t.b);
        }

        @Override public int hashCode() {
            int result = a != null ? a.hashCode() : 0;
            result = 31 * result + (b != null ? b.hashCode() : 0);
            return result;
        }

        @Override public String toString() {
            return "Pair[" + a +", "+b+"]";
        }

    }

    public interface Function0<R>{
        R run();
    }

    public interface Function1<R,A>{
        R run(A a);
    }

    public interface Function1Int<R>{
        R run(int a);
    }

    public interface Function2<R,A,B>{
        R run(A a, B b);
    }


    public static <K,V> Fun.Function1<K,Pair<K,V>> extractKey(){
        return new Fun.Function1<K, Pair<K, V>>() {
            @Override
            public K run(Pair<K, V> t) {
                return t.a;
            }
        };
    }

    public static <K,V> Fun.Function1<V,Pair<K,V>> extractValue(){
        return new Fun.Function1<V, Pair<K, V>>() {
            @Override
            public V run(Pair<K, V> t) {
                return t.b;
            }
        };
    }


    public static <K,V> Fun.Function1<K,Map.Entry<K,V>> extractMapEntryKey(){
        return new Fun.Function1<K, Map.Entry<K, V>>() {
            @Override
            public K run(Map.Entry<K, V> t) {
                return t.getKey();
            }
        };
    }

    public static <K,V> Fun.Function1<V,Map.Entry<K,V>> extractMapEntryValue(){
        return new Fun.Function1<V, Map.Entry<K, V>>() {
            @Override
            public V run(Map.Entry<K, V> t) {
                return t.getValue();
            }
        };
    }

    public static <K> Function1<K,K> extractNoTransform() {
        return new Function1<K, K>() {
            @Override
            public K run(K k) {
                return k;
            }
        };
    }


    public static final Comparator<byte[]> BYTE_ARRAY_COMPARATOR = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] o1, byte[] o2) {
            if(o1==o2) return 0;
            final int len = Math.min(o1.length,o2.length);
            for(int i=0;i<len;i++){
                int b1 = o1[i]&0xFF;
                int b2 = o2[i]&0xFF;
                if(b1!=b2)
                    return b1-b2;
            }
            return o1.length - o2.length;
        }
    };


    public static final Comparator<char[]> CHAR_ARRAY_COMPARATOR = new Comparator<char[]>() {
        @Override
        public int compare(char[] o1, char[] o2) {
            final int len = Math.min(o1.length,o2.length);
            for(int i=0;i<len;i++){
                int b1 = o1[i];
                int b2 = o2[i];
                if(b1!=b2)
                    return b1-b2;
            }
            return compareInt(o1.length, o2.length);
        }
    };

    public static final Comparator<int[]> INT_ARRAY_COMPARATOR = new Comparator<int[]>() {
        @Override
        public int compare(int[] o1, int[] o2) {
            if(o1==o2) return 0;
            final int len = Math.min(o1.length,o2.length);
            for(int i=0;i<len;i++){
                if(o1[i]==o2[i])
                    continue;
                if(o1[i]>o2[i])
                    return 1;
                return -1;
            }
            return compareInt(o1.length, o2.length);
        }
    };

    public static final Comparator<long[]> LONG_ARRAY_COMPARATOR = new Comparator<long[]>() {
        @Override
        public int compare(long[] o1, long[] o2) {
            if(o1==o2) return 0;
            final int len = Math.min(o1.length,o2.length);
            for(int i=0;i<len;i++){
                if(o1[i]==o2[i])
                    continue;
                if(o1[i]>o2[i])
                    return 1;
                return -1;
            }
            return compareInt(o1.length, o2.length);
        }
    };

    public static final Comparator<double[]> DOUBLE_ARRAY_COMPARATOR = new Comparator<double[]>() {
        @Override
        public int compare(double[] o1, double[] o2) {
            if(o1==o2) return 0;
            final int len = Math.min(o1.length,o2.length);
            for(int i=0;i<len;i++){
                if(o1[i]==o2[i])
                    continue;
                if(o1[i]>o2[i])
                    return 1;
                return -1;
            }
            return compareInt(o1.length, o2.length);
        }
    };


    /** Compares two arrays which contains comparable elements */
    public static final Comparator<Object[]> COMPARABLE_ARRAY_COMPARATOR = new Comparator<Object[]>() {
        @Override
        public int compare(Object[] o1, Object[] o2) {
            if(o1==o2) return 0;
            final int len = Math.min(o1.length,o2.length);
            for(int i=0;i<len;i++){
                int r = Fun.COMPARATOR.compare(o1[i],o2[i]);
                if(r!=0)
                    return r;
            }
            return compareInt(o1.length, o2.length);
        }
    };

    public static final class ArrayComparator implements Comparator<Object[]>{
        public final Comparator[] comparators;

        public ArrayComparator(Comparator... comparators2) {
            this.comparators = comparators2.clone();
            for(int i=0;i<this.comparators.length;i++){
                if(this.comparators[i]==null)
                    this.comparators[i] = Fun.COMPARATOR;
            }
        }

        public ArrayComparator(SerializerBase serializer, DataInput in, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            this.comparators = (Comparator[]) serializer.deserialize(in, objectStack);
        }


        @Override
        public int compare(Object[] o1, Object[] o2) {
            int len = Math.min(o1.length,o2.length);
            int r;
            for(int i=0;i<len;i++){
                Object a1 = o1[i];
                Object a2 = o2[i];

                if(a1==a2) {
                    r = 0;
                }else if(a1==null) {
                    r = 1;
                }else if(a2==null) {
                    r = -1;
                }else{
                    r = comparators[i].compare(a1,a2);;
                }
                if(r!=0)
                    return r;
            }
            return compareInt(o1.length, o2.length);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ArrayComparator that = (ArrayComparator) o;
            return Arrays.equals(comparators, that.comparators);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(comparators);
        }
    }


    public static int compareInt(int x, int y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    public static int compareLong(long x, long y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    public static  Iterable<Object[]> filter(final NavigableSet<Object[]> set,  final Object... keys) {
        return new Iterable<Object[]>() {
            @Override
            public Iterator<Object[]> iterator() {
                final Iterator<Object[]> iter = set.tailSet(keys).iterator();

                if(!iter.hasNext())
                    return Fun.EMPTY_ITERATOR;

                final Comparator comparator = set.comparator();

                return new Iterator<Object[]>() {

                    Object[] next = moveToNext();

                    Object[] moveToNext() {
                        if(!iter.hasNext())
                            return null;
                        Object[] next = iter.next();
                        if(next==null)
                            return null;
                        Object[] next2 = next.length<=keys.length? next :
                                Arrays.copyOf(next,keys.length);
                        //check all elements are equal
                        if(comparator.compare(next2,keys)!=0){
                            return null;
                        }
                        return next;
                    }

                    @Override
                    public boolean hasNext() {
                        return next!=null;
                    }

                    @Override
                    public Object[] next() {
                        Object[] ret = next;
                        if(ret == null)
                            throw new NoSuchElementException();
                        next = moveToNext();
                        return ret;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };

    }

    public interface RecordCondition<A>{
        boolean run(final long recid, final A value, final Serializer<A> serializer);
    }

    public static final RecordCondition RECORD_ALWAYS_TRUE = new RecordCondition() {
        @Override
        public boolean run(long recid, Object value, Serializer serializer) {
            return true;
        }
    };

}