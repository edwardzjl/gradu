package document_clustering.util;

import com.google.common.collect.Maps;

import java.util.*;
import java.util.stream.Collectors;

/**
 * utility methods for collections
 * <p>
 * Created by edwardlol on 16/9/19.
 */
@SuppressWarnings("unused")
public class CollectionUtil {

    //~ Constructors -----------------------------------------------------------

    // Suppress default constructor for noninstantiability
    private CollectionUtil() {
        throw new AssertionError();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * count the object numbers of a list
     *
     * @param objectList the io list
     * @param <T>        the type of objects in the list
     * @return a map contains all object and their counts
     */
    public static <T> Map<T, Integer> countList(List<T> objectList) {
        Map<T, Integer> resultMap = Maps.newHashMap();
        if (objectList != null) {
            objectList.forEach(object -> {
                int count = resultMap.containsKey(object) ? resultMap.get(object) + 1 : 1;
                resultMap.put(object, count);
            });
        }
        return resultMap;
    }

    /**
     * update a count map
     * if the map contains the object, add its count by cnt;
     * if not, put (object, cnt) into the map
     *
     * @param countMap a map of objects' count
     * @param object   object
     * @param <T>      the type of the key of the map
     * @return the updated countMap
     */
    public static <T> Map<T, Integer> updateCountMap(Map<T, Integer> countMap, T object, Integer cnt) {
        if (countMap == null) {
            countMap = Maps.newHashMap();
        }
        int count = countMap.containsKey(object) ? countMap.get(object) + cnt : cnt;
        countMap.put(object, count);
        return countMap;
    }

    public static <T> Map<T, Long> updateCountMap(Map<T, Long> countMap, T object, Long cnt) {
        if (countMap == null) {
            countMap = Maps.newHashMap();
        }
        long count = countMap.containsKey(object) ? countMap.get(object) + cnt : cnt;
        countMap.put(object, count);
        return countMap;
    }

    public static <T> Map<T, Double> updateCountMap(Map<T, Double> countMap, T object, Double cnt) {
        if (countMap == null) {
            countMap = Maps.newHashMap();
        }
        double count = countMap.containsKey(object) ? countMap.get(object) + cnt : cnt;
        countMap.put(object, count);
        return countMap;
    }

    /**
     * @param indexMap
     * @param item
     * @param index
     * @param <T>
     * @param <I>
     */
    public static <T, I> void updateSetMap(Map<T, Set<I>> indexMap, T item, I index) {
        if (indexMap == null) {
            indexMap = Maps.newHashMap();
        }
        Set<I> indexes = indexMap.containsKey(item) ? indexMap.get(item) : new HashSet<>();
        indexes.add(index);
        indexMap.put(item, indexes);
    }

    /**
     * sort a map by its value
     *
     * @param map the map to be sorted
     * @param <K> the type of the key of the map
     * @param <V> the type of the value of the map
     * @return a {@link LinkedHashMap} contains the same (K, V) pairs of the io map,
     * but the order is sorted by the value
     */
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        return map.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
    }

    /**
     * sort a map by its value in desc
     *
     * @param map the map to be sorted
     * @param <K> the type of the key of the map
     * @param <V> the type of the value of the map
     * @return a {@link LinkedHashMap} contains the same (K, V) pairs of the io map,
     * but the order is sorted by the value in desc
     */
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValueDesc(Map<K, V> map) {
        return map.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
    }
}

// End CollectionUtil.java
