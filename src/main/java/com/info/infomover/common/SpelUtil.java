package com.info.infomover.common;

import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import javax.interceptor.InvocationContext;
import java.lang.reflect.Parameter;

/**
 *  spring 表达式 AOP 处理工具类
 *
 */
public class SpelUtil {


    private final SpelExpressionParser parser;
    private final StandardEvaluationContext context;

    public SpelUtil(InvocationContext pjp) {
        this.parser = new SpelExpressionParser();
        this.context = new StandardEvaluationContext();
        extractArgments(pjp);
    }

    /**
     *  得到参数名称和值 放到 spel 上下文
     * @param pjp
     */
    private void extractArgments(InvocationContext pjp) {
        Parameter[] names = pjp.getMethod().getParameters();
        Object[] args = pjp.getParameters();
        for (int i = 0; i < names.length; i++) {
            this.context.setVariable(names[i].getName(), args[i]);
        }
    }

    /**
     * 计算表达式
     *
     * @param expr
     * @return
     */
    public Object cacl(String expr) {
        if (expr == null || expr.trim().isEmpty() ) {
            return null;
        }
        return this.parser.parseRaw(expr).getValue(this.context);
    }
}
