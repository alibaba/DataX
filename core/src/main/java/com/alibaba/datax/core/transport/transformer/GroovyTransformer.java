package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;
import groovy.lang.GroovyClassLoader;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.groovy.control.CompilationFailedException;

import java.util.Arrays;
import java.util.List;

/**
 * no comments.
 * Created by liqiang on 16/3/4.
 */
public class GroovyTransformer extends Transformer {
    public GroovyTransformer() {
        setTransformerName("dx_groovy");
    }

    private Transformer groovyTransformer;

    @Override
    public Record evaluate(Record record, Object... paras) {

        if (groovyTransformer == null) {
            //全局唯一
            if (paras.length < 1 || paras.length > 2) {
                throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "dx_groovy paras must be 1 or 2 . now paras is: " + Arrays.asList(paras).toString());
            }
            synchronized (this) {

                if (groovyTransformer == null) {
                    String code = (String) paras[0];
                    @SuppressWarnings("unchecked") List<String> extraPackage = paras.length == 2 ?  (List<String>) paras[1] : null;
                    initGroovyTransformer(code, extraPackage);
                }
            }
        }

        return this.groovyTransformer.evaluate(record);
    }

    private void initGroovyTransformer(String code, List<String> extraPackage) {
        GroovyClassLoader loader = new GroovyClassLoader(GroovyTransformer.class.getClassLoader());
        String groovyRule = getGroovyRule(code, extraPackage);

        Class groovyClass;
        try {
            groovyClass = loader.parseClass(groovyRule);
        } catch (CompilationFailedException cfe) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_GROOVY_INIT_EXCEPTION, cfe);
        }

        try {
            Object t = groovyClass.newInstance();
            if (!(t instanceof Transformer)) {
                throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_GROOVY_INIT_EXCEPTION, "datax bug! contact askdatax");
            }
            this.groovyTransformer = (Transformer) t;
        } catch (Throwable ex) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_GROOVY_INIT_EXCEPTION, ex);
        }
    }


    private String getGroovyRule(String expression, List<String> extraPackagesStrList) {
        StringBuffer sb = new StringBuffer();
        if(extraPackagesStrList!=null) {
            for (String extraPackagesStr : extraPackagesStrList) {
                if (StringUtils.isNotEmpty(extraPackagesStr)) {
                    sb.append(extraPackagesStr);
                }
            }
        }
        sb.append("import static com.alibaba.datax.core.transport.transformer.GroovyTransformerStaticUtil.*;");
        sb.append("import com.alibaba.datax.common.element.*;");
        sb.append("import com.alibaba.datax.common.exception.DataXException;");
        sb.append("import com.alibaba.datax.transformer.Transformer;");
        sb.append("import java.util.*;");
        sb.append("public class RULE extends Transformer").append("{");
        sb.append("public Record evaluate(Record record, Object... paras) {");
        sb.append(expression);
        sb.append("}}");

        return sb.toString();
    }


}
