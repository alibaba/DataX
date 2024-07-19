package com.alibaba.datax.plugin.rdbms.reader.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author johnrobbet
 */
public class ObVersion implements Comparable<ObVersion> {

    private static final Logger LOG = LoggerFactory.getLogger(ObVersion.class);

    private int majorVersion;
    private int minorVersion;
    private int releaseNumber;
    private int patchNumber;

    public static final ObVersion V2276 = valueOf("2.2.76");
    public static final ObVersion V2252 = valueOf("2.2.52");
    public static final ObVersion V3 = valueOf("3.0.0.0");
    public static final ObVersion V4000 = valueOf("4.0.0.0");

    private static final ObVersion DEFAULT_VERSION =
        valueOf(System.getProperty("defaultObVersion","3.2.3.0"));

    private static final int VERSION_PART_COUNT = 4;

    public ObVersion(String version) {
        try {
            String[] versionParts = version.split("\\.");
            majorVersion = Integer.valueOf(versionParts[0]);
            minorVersion = Integer.valueOf(versionParts[1]);
            releaseNumber = Integer.valueOf(versionParts[2]);
            int tempPatchNum = 0;
            if (versionParts.length == VERSION_PART_COUNT) {
                try {
                    tempPatchNum = Integer.valueOf(versionParts[3]);
                } catch (Exception e) {
                    LOG.warn("fail to parse ob version: " + e.getMessage());
                }
            }
            patchNumber = tempPatchNum;
        } catch (Exception ex) {
            LOG.warn("fail to get ob version, using default {} {}",
                DEFAULT_VERSION, ex.getMessage());
            majorVersion  = DEFAULT_VERSION.majorVersion;
            minorVersion  = DEFAULT_VERSION.minorVersion;
            releaseNumber = DEFAULT_VERSION.releaseNumber;
            patchNumber   = DEFAULT_VERSION.patchNumber;
        }
    }

    public static ObVersion valueOf(String version) {
        return new ObVersion(version);
    }

    @Override
    public int compareTo(ObVersion o) {
        if (this.majorVersion > o.majorVersion) {
            return 1;
        } else if (this.majorVersion < o.majorVersion) {
            return -1;
        }

        if (this.minorVersion > o.minorVersion) {
            return 1;
        } else if (this.minorVersion < o.minorVersion) {
            return -1;
        }

        if (this.releaseNumber > o.releaseNumber) {
            return 1;
        } else if (this.releaseNumber < o.releaseNumber) {
            return -1;
        }

        if (this.patchNumber > o.patchNumber) {
            return 1;
        } else if (this.patchNumber < o.patchNumber) {
            return -1;
        }

        return 0;
    }

    @Override
    public String toString() {
        return String.format("%d.%d.%d.%d", majorVersion, minorVersion, releaseNumber, patchNumber);
    }
}
