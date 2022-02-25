package com.info.infomover.resources.response;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PageResult<T> {

    private int totalPage;

    private int totalSize;

    private T pagingResult;

}
