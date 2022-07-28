package cn.com.bluemoon.metadata.base.service.impl;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import cn.com.bluemoon.metadata.base.enums.ModeEnums;
import cn.com.bluemoon.metadata.base.service.Neo4jOperatorService;
import cn.com.bluemoon.metadata.common.enums.CollectTypeEnum;
import cn.com.bluemoon.metadata.common.enums.IncrementModeEnum;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.neo4j.driver.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/service/impl/CompareNodeSyncServiceImpl.class */
public class CompareNodeSyncServiceImpl extends AbstractNodeSyncService {
    private static final Logger log = LoggerFactory.getLogger(CompareNodeSyncServiceImpl.class);

    public CompareNodeSyncServiceImpl(Neo4jOperatorService neo4jOperatorService) {
        super(neo4jOperatorService);
    }

    @Override // cn.com.bluemoon.metadata.base.service.NodeSyncService
    public boolean support(String mode) {
        return ModeEnums.COMPARE.getCode().equals(mode);
    }

    @Override // cn.com.bluemoon.metadata.base.service.NodeSyncService
    public void batchSync(Driver driver, CreateTypeConfig context, List<Map<String, Object>> allRecs) {
        addDefaultValue(context, allRecs);
        addMoiParentDataId(driver, context, allRecs);
        List<Map<String, Object>> directoryInsert = Lists.newArrayList();
        List<Map<String, Object>> positiveIncrement = Lists.newArrayList();
        List<Map<String, Object>> negtiveIncrement = Lists.newArrayList();
        List<String> compareColumns = context.getColumns();
        List<Map<String, Object>> existing = this.neo4jOperatorService.queryDataByGuid(driver, (List) allRecs.stream().map(x -> {
            return x.get("guid").toString();
        }).collect(Collectors.toList()));
        Map<String, Map<String, Object>> existsData = Maps.newHashMap();
        if (CollectionUtils.isNotEmpty(existing)) {
            existsData = (Map) existing.stream().collect(Collectors.toMap(x -> {
                return x.get("guid").toString();
            }, y -> {
                return y;
            }, l, m -> {
                return l;
            }));
        }
        for (Map<String, Object> allRec : allRecs) {
            Map<String, Object> esData = existsData.get(allRec.get("guid").toString());
            if (esData == null) {
                directoryInsert.add(allRec);
            } else if (!compareDifferences(allRec, esData, compareColumns)) {
                if (IncrementModeEnum.POSITIVE.getCode().equals(Integer.valueOf(Integer.parseInt(context.getIncrementMode())))) {
                    positiveIncrement.add(allRec);
                } else {
                    negtiveIncrement.add(allRec);
                }
            }
        }
        log.info("正在同步中....本次同步{}数量为{}个,当前已同步数量为{},同步节点结果:同步{}: 修改{}个,新增{}个", new Object[]{context.getNodeName(), Integer.valueOf(allRecs.size()), context.getNodeTotalCount(), context.getNodeName(), Integer.valueOf(positiveIncrement.size() + negtiveIncrement.size()), Integer.valueOf(directoryInsert.size())});
        if (negtiveIncrement.size() > 100) {
            positiveIncrement.addAll(negtiveIncrement);
            negtiveIncrement = Lists.newArrayList();
        }
        if (positiveIncrement.size() > 0) {
            generatorDataId(driver, context, positiveIncrement);
            this.neo4jOperatorService.positiveSync(driver, context, positiveIncrement);
        }
        if (negtiveIncrement.size() > 0) {
            this.neo4jOperatorService.negtiveSync(driver, context, negtiveIncrement);
        }
        generatorDataId(driver, context, directoryInsert);
        for (Map<String, Object> stringObjectMap : directoryInsert) {
            stringObjectMap.put("collectType", CollectTypeEnum.ADD.getCode());
        }
        this.neo4jOperatorService.batchInsert(driver, context, directoryInsert);
    }

    private boolean compareDifferences(Map<String, Object> allRec, Map<String, Object> esData, List<String> compareColumns) {
        Collection<? extends String> newArrayList = Lists.newArrayList(new String[]{"moiParentDataId"});
        List<String> temp = Lists.newArrayList(compareColumns);
        temp.addAll(newArrayList);
        List<String> retainIndexField = (List) temp.stream().filter(x -> {
            return indexField.contains(x);
        }).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(retainIndexField)) {
            temp.removeAll(retainIndexField);
            for (String compareColumn : temp) {
                if (!this.dateFieldSet.contains(compareColumn) && !Objects.equals(esData.get(compareColumn), allRec.get(compareColumn))) {
                    return false;
                }
            }
            for (String ignoreFieldSet : this.dateFieldSet) {
                allRec.put(ignoreFieldSet, esData.get(ignoreFieldSet));
            }
            return false;
        }
        for (String compareColumn2 : temp) {
            if (!this.dateFieldSet.contains(compareColumn2) && !Objects.equals(esData.get(compareColumn2), allRec.get(compareColumn2))) {
                return false;
            }
        }
        return true;
    }
}
