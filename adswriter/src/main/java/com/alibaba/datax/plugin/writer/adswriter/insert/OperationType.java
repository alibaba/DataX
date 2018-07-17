package com.alibaba.datax.plugin.writer.adswriter.insert;

public enum OperationType {
    // i: insert uo:before image uu:before image un: after image d: delete
    // u:update
    I("i"), UO("uo"), UU("uu"), UN("un"), D("d"), U("u"), UNKNOWN("unknown"), ;
    private OperationType(String type) {
        this.type = type;
    }

    private String type;

    public String getType() {
        return this.type;
    }

    public static OperationType asOperationType(String type) {
        if ("i".equalsIgnoreCase(type)) {
            return I;
        } else if ("uo".equalsIgnoreCase(type)) {
            return UO;
        } else if ("uu".equalsIgnoreCase(type)) {
            return UU;
        } else if ("un".equalsIgnoreCase(type)) {
            return UN;
        } else if ("d".equalsIgnoreCase(type)) {
            return D;
        } else if ("u".equalsIgnoreCase(type)) {
            return U;
        } else {
            return UNKNOWN;
        }
    }

    public boolean isInsertTemplate() {
        switch (this) {
        // 建议merge 过后应该只有I和U两类
        case I:
        case UO:
        case UU:
        case UN:
        case U:
            return true;
        case D:
            return false;
        default:
            return false;
        }
    }

    public boolean isDeleteTemplate() {
        switch (this) {
        // 建议merge 过后应该只有I和U两类
        case I:
        case UO:
        case UU:
        case UN:
        case U:
            return false;
        case D:
            return true;
        default:
            return false;
        }
    }

    public boolean isLegal() {
        return this.type != UNKNOWN.getType();
    }

    @Override
    public String toString() {
        return this.name();
    }
}
