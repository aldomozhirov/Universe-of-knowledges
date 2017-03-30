package com.database.uokdb.db;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public final class Bind {

	private Bind() {
	}

	public interface MapListener<K, V> {

		void update(K key, V oldVal, V newVal);
	}

	public interface MapWithModificationListener<K, V> extends ConcurrentMap<K, V> {

		public void modificationListenerAdd(MapListener<K, V> listener);

		public void modificationListenerRemove(MapListener<K, V> listener);

		public long sizeLong();
	}

	public static <K, V, V2> void secondaryValue(MapWithModificationListener<K, V> map, final Map<K, V2> secondary,
			final Fun.Function2<V2, K, V> fun) {

		if (secondary.isEmpty()) {

			for (Map.Entry<K, V> e : map.entrySet())
				secondary.put(e.getKey(), fun.run(e.getKey(), e.getValue()));
		}

		map.modificationListenerAdd(new MapListener<K, V>() {
			@Override
			public void update(K key, V oldVal, V newVal) {

				if (newVal == null) {
					secondary.remove(key);

				} else {

					secondary.put(key, fun.run(key, newVal));
				}
			}
		});
	}

	public static <K, V, V2> void secondaryValues(MapWithModificationListener<K, V> map, final Set<Object[]> secondary,
			final Fun.Function2<V2[], K, V> fun) {

		if (secondary.isEmpty()) {

			for (Map.Entry<K, V> e : map.entrySet()) {
				V2[] v = fun.run(e.getKey(), e.getValue());

				if (v != null) {
					for (V2 v2 : v) {

						secondary.add(new Object[] { e.getKey(), v2 });

					}
				}
			}
		}

		map.modificationListenerAdd(new MapListener<K, V>() {
			@Override
			public void update(K key, V oldVal, V newVal) {

				if (newVal == null) {

					V2[] v = fun.run(key, oldVal);
					if (v != null) {
						for (V2 v2 : v) {

							secondary.remove(new Object[] { key, v2 });
						}
					}
				} else if (oldVal == null) {

					V2[] v = fun.run(key, newVal);
					if (v != null) {
						for (V2 v2 : v) {

							secondary.add(new Object[] { key, v2 });
						}
					}
				} else {

					V2[] oldv = fun.run(key, oldVal);
					V2[] newv = fun.run(key, newVal);
					if (oldv == null) {

						if (newv != null) {
							for (V2 v : newv) {

								secondary.add(new Object[] { key, v });
							}
						}
						return;
					}
					if (newv == null) {
						for (V2 v : oldv) {

							secondary.remove(new Object[] { key, v });
						}
						return;
					}

					Set<V2> hashes = new HashSet<V2>();
					Collections.addAll(hashes, oldv);

					for (V2 v : newv) {
						if (!hashes.contains(v)) {
							secondary.add(new Object[] { key, v });
						}
					}
					for (V2 v : newv) {

						hashes.remove(v);
					}
					for (V2 v : hashes) {

						secondary.remove(new Object[] { key, v });
					}
				}
			}
		});
	}

	public static <K, V, K2> void secondaryKey(MapWithModificationListener<K, V> map, final Set<Object[]> secondary,
			final Fun.Function2<K2, K, V> fun) {

		if (secondary.isEmpty()) {
			for (Map.Entry<K, V> e : map.entrySet()) {

				secondary.add(new Object[] { fun.run(e.getKey(), e.getValue()), e.getKey() });
			}
		}
		map.modificationListenerAdd(new MapListener<K, V>() {
			@Override
			public void update(K key, V oldVal, V newVal) {

				if (newVal == null) {
					secondary.remove(new Object[] { fun.run(key, oldVal), key });
				} else if (oldVal == null) {

					secondary.add(new Object[] { fun.run(key, newVal), key });
				} else {

					K2 oldKey = fun.run(key, oldVal);
					K2 newKey = fun.run(key, newVal);
					if (oldKey == newKey || oldKey.equals(newKey))
						return;

					secondary.remove(new Object[] { oldKey, key });

					secondary.add(new Object[] { newKey, key });

				}
			}
		});
	}

	public static <K, V, K2> void secondaryKey(MapWithModificationListener<K, V> map, final Map<K2, K> secondary,
			final Fun.Function2<K2, K, V> fun) {

		if (secondary.isEmpty()) {
			for (Map.Entry<K, V> e : map.entrySet()) {

				secondary.put(fun.run(e.getKey(), e.getValue()), e.getKey());
			}
		}

		map.modificationListenerAdd(new MapListener<K, V>() {
			@Override
			public void update(K key, V oldVal, V newVal) {

				if (newVal == null) {
					secondary.remove(fun.run(key, oldVal));
				} else if (oldVal == null) {
					secondary.put(fun.run(key, newVal), key);
				} else {
					K2 oldKey = fun.run(key, oldVal);
					K2 newKey = fun.run(key, newVal);
					if (oldKey == newKey || oldKey.equals(newKey))
						return;

					secondary.remove(oldKey);

					secondary.put(newKey, key);
				}
			}
		});
	}

	public static <K, V, K2> void secondaryKeys(MapWithModificationListener<K, V> map, final Set<Object[]> secondary,
			final Fun.Function2<K2[], K, V> fun) {

		if (secondary.isEmpty()) {
			for (Map.Entry<K, V> e : map.entrySet()) {

				K2[] k2 = fun.run(e.getKey(), e.getValue());
				if (k2 != null) {
					for (K2 k22 : k2) {

						secondary.add(new Object[] { k22, e.getKey() });
					}
				}
			}
		}

		map.modificationListenerAdd(new MapListener<K, V>() {
			@Override
			public void update(K key, V oldVal, V newVal) {

				if (newVal == null) {

					K2[] k2 = fun.run(key, oldVal);
					if (k2 != null) {
						for (K2 k22 : k2) {

							secondary.remove(new Object[] { k22, key });
						}
					}
				} else if (oldVal == null) {

					K2[] k2 = fun.run(key, newVal);

					if (k2 != null) {
						for (K2 k22 : k2) {

							secondary.add(new Object[] { k22, key });
						}
					}
				} else {

					K2[] oldk = fun.run(key, oldVal);
					K2[] newk = fun.run(key, newVal);
					if (oldk == null) {
						if (newk != null) {
							for (K2 k22 : newk) {

								secondary.add(new Object[] { k22, key });
							}
						}
						return;
					}
					if (newk == null) {
						for (K2 k22 : oldk) {

							secondary.remove(new Object[] { k22, key });
						}
						return;
					}

					Set<K2> hashes = new HashSet<K2>();

					Collections.addAll(hashes, oldk);

					for (K2 k2 : newk) {

						if (!hashes.contains(k2)) {

							secondary.add(new Object[] { k2, key });
						}
					}
					for (K2 k2 : newk) {

						hashes.remove(k2);
					}
					for (K2 k2 : hashes) {

						secondary.remove(new Object[] { k2, key });
					}
				}
			}
		});
	}

	public static <K, V> void mapInverse(MapWithModificationListener<K, V> primary, Set<Object[]> inverse) {
		Bind.secondaryKey(primary, inverse, new Fun.Function2<V, K, V>() {
			@Override
			public V run(K key, V value) {
				return value;
			}
		});
	}

	public static <K, V> void mapInverse(MapWithModificationListener<K, V> primary, Map<V, K> inverse) {
		Bind.secondaryKey(primary, inverse, new Fun.Function2<V, K, V>() {
			@Override
			public V run(K key, V value) {
				return value;
			}
		});
	}

	public static <K, V, C> void histogram(MapWithModificationListener<K, V> primary,
			final ConcurrentMap<C, Long> histogram, final Fun.Function2<C, K, V> entryToCategory) {

		MapListener<K, V> listener = new MapListener<K, V>() {
			@Override
			public void update(K key, V oldVal, V newVal) {

				if (newVal == null) {

					C category = entryToCategory.run(key, oldVal);
					incrementHistogram(category, -1);
				} else if (oldVal == null) {

					C category = entryToCategory.run(key, newVal);
					incrementHistogram(category, 1);
				} else {

					C oldCat = entryToCategory.run(key, oldVal);
					C newCat = entryToCategory.run(key, newVal);

					if (oldCat == newCat || oldCat.equals(newCat))
						return;
					incrementHistogram(oldCat, -1);
					incrementHistogram(newCat, 1);
				}

			}

			private void incrementHistogram(C category, long i) {

				atomicUpdateLoop: for (;;) {

					Long oldCount = histogram.get(category);
					if (oldCount == null) {
						if (histogram.putIfAbsent(category, i) == null) {
							return;
						}
					} else {
						Long newCount = oldCount + i;
						if (histogram.replace(category, oldCount, newCount)) {
							return;
						}
					}
				}
			}
		};

		primary.modificationListenerAdd(listener);

		if (histogram.isEmpty()) {
			for (Map.Entry<K, V> e : primary.entrySet()) {
				listener.update(e.getKey(), null, e.getValue());
			}
		}
	}

	public static <K, V> void mapPutAfterDelete(MapWithModificationListener<K, V> primary,
			final MapWithModificationListener<K, V> secondary, final boolean overwriteSecondary) {

		primary.modificationListenerAdd(new MapListener<K, V>() {
			@Override
			public void update(K key, V oldVal, V newVal) {
				if (newVal == null) {
					if (overwriteSecondary) {
						secondary.put(key, oldVal);
					} else {
						secondary.putIfAbsent(key, oldVal);
					}
				}
			}
		});
	}

}