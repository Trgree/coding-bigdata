package org.ace.spark.task.pojo;

/**
 * 数据格式
 */
public enum DataType {
    TEXT(0, "text"),
    CSV(1,"csv"),
    JSON(2,"json"),
    PARQUET(3,"parquet"),
    DB(4,"db"),
    MULTI(5,"multi");// 多种格式输入

    private int state;
    private String val;

    DataType(int state, String val) {
        this.state = state;
        this.val = val;
    }

    public static DataType valOf(String val) {
        if(val == null || (val = val.trim().toLowerCase()).equals("")){
            return null;
        }
        for(DataType p : values()) {
            if(p.val.toLowerCase().equals(val)){
                return p;
            }
        }
        return null;
    }

    public static DataType valOf(Object val){
        if(val == null) {
            return null;
        }
        if(val instanceof DataType) {
            return (DataType)val;
        } else if (val instanceof String){
            return valOf((String)val);
        }
        return null;
    }

    public static DataType stateOf(int state) {
        for (DataType fileType : values()) {
            if (fileType.state == state) {
                return fileType;
            }
        }
        return DataType.CSV;
    }

    public static DataType stateOf(String state) {
        int val = 1;
        try {
            val = Integer.parseInt(state);
        } catch (NumberFormatException e) {
        }
        return stateOf(val);
    }

    public String getVal() {
        return val;
    }

    public int getState() {
        return state;
    }
}
