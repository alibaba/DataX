package com.alibaba.datax.plugin.reader.oceanbasev10reader.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author johnrobbet
 */
public class PartInfo {

    private PartType partType;

    List<String> partList;

    public PartInfo(PartType partType) {
        this.partType = partType;
        this.partList = new ArrayList();
    }

    public String getPartType () {
        return partType.getTypeString();
    }

    public void addPart(List partList) {
        this.partList.addAll(partList);
    }

    public List<String> getPartList() {
        return partList;
    }

    public boolean isPartitionTable() {
        return partType != PartType.NONPARTITION && partList.size() > 0;
    }
}
