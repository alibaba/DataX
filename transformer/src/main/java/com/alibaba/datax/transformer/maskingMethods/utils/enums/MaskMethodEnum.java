package com.alibaba.datax.transformer.maskingMethods.utils.enums;


public enum MaskMethodEnum {
    CAESAR_ENCRYPTION(1, "可逆置换-凯撒"),
    RAIL_FENCE_ENCRYPTION(2, "可逆置换-栅栏"),
    MD5_ENCRYPTION(3, "不可逆干扰-MD5"),
    EPSILON_DIFFERENTIAL_PRIVACY(4, "差分隐私"),
    AES_ENCRYPTION(5, "AES算法加密"),
    RSA_ENCRYPTION(6, "RSA算法加密"),
    FORMAT_PRESERVING_ENCRYPTION(7, "保行加密"),
    ANONYMITY_ENCRYPTION(8, "匿名");

    private  int value;
    private String desc;

    //enum类里面的构造函数默认是private的。
    MaskMethodEnum(int value, String desc) {
        this.value = value;
        this.desc = desc;
    }

    /**
     * 根据int value找到对应的desc
     * @param value
     * @return
     */
    public static String getDesc(int value) {
        for (MaskMethodEnum m : MaskMethodEnum.values()) {
            if (m.getValue() == value) {
                return m.getDesc();
            }
        }
        return null;
    }

    /**
     * 根据String desc找到对应的int value 描述
     * @param desc
     * @return
     */
    public static int getValue(String desc) {
        for (MaskMethodEnum m : MaskMethodEnum.values()) {
            if (m.getDesc().equals(desc)) {
                return m.getValue();
            }
        }
        return 0;
    }

    /**
     * 根据int value找到对应的枚举对象
     * @param value
     * @return
     */
    public static MaskMethodEnum getMaskMethodEnum(int value) {
        for (MaskMethodEnum m : MaskMethodEnum.values()) {
            if (m.getValue() == value) {
                return m;
            }
        }
        return null;
    }

    /**
     * 根据String desc找到对应的枚举对象
     * @param desc
     * @return
     */
    public static MaskMethodEnum getMaskMethodEnum(String desc) {
        for (MaskMethodEnum m : MaskMethodEnum.values()) {
            if (m.getDesc().equals(desc)) {
                return m;
            }
        }
        return null;
    }

    //getter and setter
    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
