package cn.sensorsdata.datax.plugin.domain;

import cn.hutool.core.util.StrUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ScriptEngineWrapper implements Serializable {

    private static final long serialVersionUID = 3547932395972735654L;

    private ScriptEngineManager manager = new ScriptEngineManager();
    private ScriptEngine engine = manager.getEngineByName("javascript");

    private String script;

    private String sharedPool;

    public void eval() {
        try {

            engine.eval(StrUtil.isBlank(sharedPool) ? script : sharedPool.concat(" \n").concat(script));
        } catch (ScriptException e) {
            e.printStackTrace();
        }
    }

    public Object invokeFunction(String name, Object... args)
            throws ScriptException, NoSuchMethodException {
        if (engine instanceof Invocable) {
            Invocable in = (Invocable) engine;
            return in.invokeFunction(name, args);
        }
        return null;
    }
}
