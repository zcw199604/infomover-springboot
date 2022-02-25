package com.info.infomover.entity;

import java.util.List;

//@RegisterForReflection
public class Page<T> {
    public List<T> content;
    public long totalElements;
    public long totalPages;

    private Page(List<T> content, long totalElements, long totalPages) {
        this.content = content;
        this.totalElements = totalElements;
        this.totalPages = totalPages;
    }

    public static <T> Page of(List<T> content, long totalElements, long pageSize) {
        //每页显示的记录数
        //页数
        long totalPages = 0;
        if (totalElements != 1 && pageSize != 0) {
            totalPages = (totalElements - 1) / pageSize + 1;
        }
        return new Page<T>(content, totalElements, totalPages);
    }

    public static long getOffest(long pageNum,long pageSize) {
        if (pageNum < 0) {
            return 0;
        }
        if (pageNum == 1) {
            return 0;
        }
        return (pageNum - 1) * pageSize;

    }

    @Override
    public String toString() {
        return "Page{" +
                "content=" + content +
                ", totalElements=" + totalElements +
                ", totalPages=" + totalPages +
                '}';
    }
}
