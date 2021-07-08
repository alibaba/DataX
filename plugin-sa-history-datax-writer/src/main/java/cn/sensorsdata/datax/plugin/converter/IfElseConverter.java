package cn.sensorsdata.datax.plugin.converter;

import cn.hutool.core.collection.ConcurrentHashSet;
import cn.hutool.core.util.StrUtil;
import cn.sensorsdata.datax.plugin.Converter;
import cn.sensorsdata.datax.plugin.domain.ScriptEngineWrapper;
import cn.sensorsdata.datax.plugin.util.NullUtil;
import cn.sensorsdata.datax.plugin.util.TypeUtil;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class IfElseConverter implements Converter {

    private static final String DEFAULT_RETURN_VALUE = "return null;";
    private static final String RETURN_FALSE = "return false;";
    private static final String IF_FUNCTION_NAME = "ifFun";
    private static final String VALUE_FUNCTION_NAME = "valFun";
    private static final String ELSE_FUNCTION_NAME = "elseFun";
    private static ScriptEngineManager manager = new ScriptEngineManager();
    private static ScriptEngine engine = manager.getEngineByName("javascript");
    private static String functionTemplate = "function ifFun(targetColumnName,value,param){ {} }\n" +
            "function valFun(targetColumnName,value,param){ {} }\n" +
            "function elseFun(targetColumnName,value,param){ {} }";
    private Map<String, ScriptEngineWrapper> cache = new ConcurrentHashMap<>();
    private Set<String> cacheTargetColumnName = new ConcurrentHashSet<>();

    @Override
    public Object transform(String targetColumnName, Object value, Map<String, Object> param) {
        try {
            if (cacheTargetColumnName.contains(targetColumnName)) {

                ScriptEngineWrapper scriptEngineWrapper = cache.get(targetColumnName);
                Object ifFunResult = scriptEngineWrapper.invokeFunction(IF_FUNCTION_NAME, targetColumnName, value, param);
                if (NullUtil.isNullOrBlank(ifFunResult)) {
                    Object elseFunResult = scriptEngineWrapper.invokeFunction(ELSE_FUNCTION_NAME, targetColumnName, value, param);
                    return elseFunResult;
                } else if (TypeUtil.isPrimitive(ifFunResult, Boolean.class) && (boolean) ifFunResult) {
                    return scriptEngineWrapper.invokeFunction(VALUE_FUNCTION_NAME, targetColumnName, value, param);
                } else {
                    return scriptEngineWrapper.invokeFunction(ELSE_FUNCTION_NAME, targetColumnName, value, param);
                }

            } else {

                String ifStr = (String) param.get("if");
                String valStr = (String) param.get("value");
                String elseStr = (String) param.get("else");
                String sharedPool = (String) param.get("sharedPool");
                if (StrUtil.isBlank(ifStr)) {
                    ifStr = RETURN_FALSE;
                }
                if (StrUtil.isBlank(valStr)) {
                    valStr = DEFAULT_RETURN_VALUE;
                }
                if (StrUtil.isBlank(elseStr)) {
                    elseStr = DEFAULT_RETURN_VALUE;
                    if (RETURN_FALSE == ifStr) {
                        return null;
                    }
                }
                String script = StrUtil.format(functionTemplate, ifStr, valStr, elseStr);
                ScriptEngineWrapper sew = new ScriptEngineWrapper();
                sew.setScript(script);
                sew.setSharedPool(sharedPool);
                sew.eval();
                cache.put(targetColumnName, sew);
                cacheTargetColumnName.add(targetColumnName);
                return this.transform(targetColumnName, value, param);
            }

        } catch (Exception e) {
            throw new RuntimeException("IfElse转换器执行异常：" + e.getMessage());
        }
    }


}
