package com.alibaba.datax.transformer.maskingMethods.differentialPrivacy;

import com.alibaba.datax.transformer.maskingMethods.utils.math.algorithm.Laplace;

import java.util.ArrayList;
import java.util.List;

/**
 * <ul>
 * <li><strong>Differential Privacy</strong>:</li>
 * <li>Dwork, C., McSherry, F., Nissim, K. and Smith, A., 2006, March. Calibrating noise to sensitivity in private data analysis. In TCC (Vol. 3876, pp. 265-284).</li>
 * </ul>
 *
 * @author Wenyan Liu
 */

public class EpsilonDifferentialPrivacyImpl extends DifferentialPrivacyMasking {

    double resVal = 0;

    public double execute(double epsilon) throws Exception {
        resVal = Laplace.pdf(1 / epsilon);

        return resVal;
    }

    /**
     * 处理将int/double转成String传递过来的值。
     * @param originData
     * @return
     * @throws Exception
     */
    public List<String> execute(List<String> originData, double epsilon) throws Exception {
        List<String> resVals = new ArrayList<String>();
        for (String data : originData) {
            double val = Double.parseDouble(data) + Laplace.pdf(1 / epsilon);
            resVals.add(Double.toString(val));
        }
        return resVals;
    }

    public String maskOne(String originData, double epsilon) throws Exception {
        double val = Double.parseDouble(originData) + Laplace.pdf(1 / epsilon);
        return String.valueOf(val);
    }

    public double maskOne(double originData, double epsilon) throws Exception {
        double val = originData + Laplace.pdf(1 / epsilon);
        return val;
    }

    public long maskOne(long originData, double epsilon) throws Exception {
        double val = originData + Laplace.pdf(1 / epsilon);
        return (long)val;
    }

    public void mask(){

    }
}