package com.raymond.queue.collection;

/**
 * 缓存类
 *
 * @author :  raymond
 * @version :  V1.0
 * @date :  2021-01-21 16:04
 */
public class CollectionEntry<E> {
    private E data;

    public CollectionEntry() {
    }

    public CollectionEntry(E data) {
        this.data = data;
    }

    public E getData() {
        return data;
    }

    public void setData(E data) {
        this.data = data;
    }
}
