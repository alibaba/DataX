package com.alibaba.datax.plugin.reader.oceanbasev10reader.util;

/**
 * @author johnrobbet
 */
public class ObVersion implements Comparable<ObVersion> {

    private final int majorVersion;
    private final int minorVersion;
    private final int releaseNumber;
    private final int patchNumber;

    public ObVersion(String version) {
        String[] versionParts = version.split("\\.");
        majorVersion = Integer.valueOf(versionParts[0]);
        minorVersion = Integer.valueOf(versionParts[1]);
        releaseNumber = Integer.valueOf(versionParts[2]);
        if (versionParts.length == 4) {
            patchNumber = Integer.valueOf(versionParts[3]);
        } else {
            patchNumber = 0;
        }
    }

    public static ObVersion valueOf(String version) {
        return new ObVersion(version);
    }

    public static final ObVersion V2276 = valueOf("2.2.76");

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
}
