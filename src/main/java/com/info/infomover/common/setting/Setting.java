package com.info.infomover.common.setting;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(value = ElementType.FIELD)
public @interface Setting {

    /**
     * 可选项，有值为下拉选项，没值时为input框
     */
    String[] values() default {};

    /**
     * 字段从指定输入选择
     */
    Select select() default Select.NULL;

    /**
     * 提示UI选择什么类型的数据，例如选择field或者inputId
     */
    SelectType selectType() default SelectType.NULL;

    String defaultValue() default "";

    String description() default "";
    /*
     * 中文名称
     */
    String displayName() default "";

    /*
     * Tips中文描述
     */
    String tips() default "";

    String category() default "";

    /**
     * 是否放到高级选项
     */
    boolean advanced() default false;

    /**
     * 是否必填
     */
    boolean required() default true;

    /**
     * 作用域
     */
    String scope() default "";

    /**
     * 作用域数据类型
     */
    String scopeType() default "string";

    String[] bind() default {};

    /**
     * 提示UI以某种方式渲染，比如Transform的expressions是String类型， 默认渲染是input框，指定format为sql，使用sql编辑器插件渲染<br/>
     * 已知的format:<br/>
     *  1. sql 渲染成sql编辑器<br/>
     *  2. add 渲染成类似lookup_redis的分类表格<br/>
     *  3. single 分类表格，但是不需要添加按钮，仅一行
     *  4. password 按密码处理方式隐藏输入的值
     *
     * @return
     */
    String format() default "";

    /**
     * 是否是复杂类型的step，如果是需要进行类型拼接。
     * 比如： Source Step是一个复杂类型的Step，因为有Source_KAFKA，Source_HDFS多种分类
     * 目前已知的复杂类型Step有：Source,Sink,Lookup
     *
     * @return
     */
    boolean compositeStep() default false;

    int order() default 1;

    /**
     * 属性是否是隐藏域，默认false
     *
     * @return 是否隐藏
     */
    boolean hidden() default false;


    /**
     * 是否可编辑
     * @return
     */
    boolean disabled() default false;

    /**
     * 是否去除左右空格
     * @return
     */
    boolean noTrim() default false;


}
