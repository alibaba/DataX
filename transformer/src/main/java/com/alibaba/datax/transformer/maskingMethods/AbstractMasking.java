package com.alibaba.datax.transformer.maskingMethods;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

/**
 * Abstract masking methods.
 *
 * @author Wenyan Liu
 */

public abstract class AbstractMasking implements Masking{

    /**
     * LOG
     */
    protected final Log LOG = LogFactory.getLog(this.getClass());

    /**
     * Parameter
     */
    protected double d;

    /**
     * Execute model.
     *
     * @throws Exception if error occurs during executing model.
     */
    public abstract double execute(double d) throws Exception;



    /**
     * Mask.
     *
     * @throws Exception if error occurs during masking
     */
    public void mask(Map<String, Object> map) throws Exception {
        // TODO: extract other contexts.
        execute(d);
        LOG.info("Job Execute completed.");
        evaluate();
        LOG.info("Job Evaluate completed.");
        cleanup();
    }

    /**
     * Evaluate.
     *
     * @return
     * @throws Exception
     */
    public double evaluate() throws Exception {
        // TODO: extract other contexts.
        return 1;
    }

    /**
     * Cleanup.
     *
     * @throws Exception if error occurs during cleanup
     */
    protected void cleanup() throws Exception {
        // TODO: cleanup.
    }
}