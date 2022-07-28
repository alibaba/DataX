package cn.com.bluemoon.metadata.base.util;

import cn.com.bluemoon.metadata.base.constant.Neo4jCypherConstants;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.neo4j.ogm.annotation.NodeEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/util/LabelEntityHolder.class */
public final class LabelEntityHolder {
    private static final Logger log = LoggerFactory.getLogger(LabelEntityHolder.class);
    private static Map<String, String> labelEntityRelation;

    static {
        labelEntityRelation = new HashMap();
        try {
            labelEntityRelation = scanPackage("cn.com.bluemoon.metadata.neo4j.dal.neo4j.entity");
        } catch (Exception e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
            throw new RuntimeException("初始化出错");
        }
    }

    public static String getEntityClassNameByLabel(String label) {
        return labelEntityRelation.get(label);
    }

    private LabelEntityHolder() {
    }

    static byte[] read(InputStream in) throws IOException {
        byte[] temp = new byte[4096];
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        BufferedInputStream bin = new BufferedInputStream(in);
        while (true) {
            int len = bin.read(temp);
            if (len <= -1) {
                return buffer.toByteArray();
            }
            buffer.write(temp, 0, len);
        }
    }

    /*  JADX ERROR: JadxRuntimeException in pass: BlockProcessor
        jadx.core.utils.exceptions.JadxRuntimeException: Unreachable block: B:21:0x00c0
        	at jadx.core.dex.visitors.blocks.BlockProcessor.checkForUnreachableBlocks(BlockProcessor.java:86)
        	at jadx.core.dex.visitors.blocks.BlockProcessor.processBlocksTree(BlockProcessor.java:52)
        	at jadx.core.dex.visitors.blocks.BlockProcessor.visit(BlockProcessor.java:44)
        */
    static java.util.Map<java.lang.String, java.lang.String> scanPackage(java.lang.String r5) throws java.lang.Exception {
        /*
        // Method dump skipped, instructions count: 311
        */
        throw new UnsupportedOperationException("Method not decompiled: cn.com.bluemoon.metadata.base.util.LabelEntityHolder.scanPackage(java.lang.String):java.util.Map");
    }

    private static Map<String, String> walkFileTree(Path path, final Path basePath) throws IOException {
        final Map<String, String> result = new HashMap<>();
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() { // from class: cn.com.bluemoon.metadata.base.util.LabelEntityHolder.1
            private String packageName;

            {
                this.packageName = Objects.isNull(basePath) ? "" : basePath.toString();
            }

            public FileVisitResult visitFile(Path arg0, BasicFileAttributes arg1) throws IOException {
                if (arg0.toString().endsWith(".class")) {
                    String absolutClassName = arg0.toString().replace(this.packageName, "").substring(1).replace("\\", Neo4jCypherConstants.PATH_DELIMITER).replace(".class", "").replace(Neo4jCypherConstants.PATH_DELIMITER, ".");
                    try {
                        result.put(Class.forName(absolutClassName).getAnnotation(NodeEntity.class).label(), absolutClassName);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
                return FileVisitResult.CONTINUE;
            }

            public FileVisitResult preVisitDirectory(Path arg0, BasicFileAttributes arg1) throws IOException {
                return FileVisitResult.CONTINUE;
            }
        });
        return result;
    }

    static FileSystemProvider getZipFSProvider() {
        for (FileSystemProvider provider : FileSystemProvider.installedProviders()) {
            if ("jar".equals(provider.getScheme())) {
                return provider;
            }
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
    }
}
