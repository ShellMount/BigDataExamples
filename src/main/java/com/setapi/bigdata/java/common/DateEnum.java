package com.setapi.bigdata.java.common;


/**
 * 日期类型枚举类
 *
 */
public enum DateEnum {
    YEAR("year"), SEASON("season"), MONTH("month"), WEEK("week"), DAY("day"), HOUR("hour");

    public final String name;

    DateEnum(String name) {
        this.name = name;
    }

    /**
     * 根据属性name的值获取对应的type对象
     * 
     * @param name
     * @return
     */
    public static DateEnum valueOfName(String name) {
        for (DateEnum type : values()) {
            if (type.name.equals(name)) {
                return type;
            }
        }
        return null;
    }
}
