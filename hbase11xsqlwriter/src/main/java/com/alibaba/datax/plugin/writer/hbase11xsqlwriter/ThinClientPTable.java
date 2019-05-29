package com.alibaba.datax.plugin.writer.hbase11xsqlwriter;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;

import java.util.List;
import java.util.Map;

public class ThinClientPTable implements PTable {

  private Map<String, ThinClientPColumn> colMap;

  public void setColTypeMap(Map<String, ThinClientPColumn> colMap) {
    this.colMap = colMap;
  }

  @Override
  public long getTimeStamp() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public long getSequenceNumber() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public long getIndexDisableTimestamp() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PName getName() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PName getSchemaName() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PName getTableName() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PName getTenantId() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PTableType getType() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PName getPKName() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public List<PColumn> getPKColumns() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public List<PColumn> getColumns() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public List<PColumnFamily> getColumnFamilies() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PColumnFamily getColumnFamily(byte[] bytes) throws ColumnFamilyNotFoundException {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PColumnFamily getColumnFamily(String s) throws ColumnFamilyNotFoundException {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PColumn getColumnForColumnName(String colname) throws ColumnNotFoundException, AmbiguousColumnException {
    if (!colMap.containsKey(colname)) {
      throw new ColumnNotFoundException("Col " + colname + " not found");
    }
    return colMap.get(colname);
  }

  @Override
  public PColumn getColumnForColumnQualifier(byte[] bytes, byte[] bytes1)
      throws ColumnNotFoundException, AmbiguousColumnException {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PColumn getPKColumn(String s) throws ColumnNotFoundException {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PRow newRow(KeyValueBuilder keyValueBuilder, long l, ImmutableBytesWritable immutableBytesWritable, boolean b,
      byte[]... bytes) {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PRow newRow(KeyValueBuilder keyValueBuilder, ImmutableBytesWritable immutableBytesWritable, boolean b,
      byte[]... bytes) {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public int newKey(ImmutableBytesWritable immutableBytesWritable, byte[][] bytes) {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public RowKeySchema getRowKeySchema() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public Integer getBucketNum() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public List<PTable> getIndexes() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PIndexState getIndexState() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PName getParentName() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PName getParentTableName() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PName getParentSchemaName() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public List<PName> getPhysicalNames() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PName getPhysicalName() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public boolean isImmutableRows() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public boolean getIndexMaintainers(ImmutableBytesWritable immutableBytesWritable,
      PhoenixConnection phoenixConnection) {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public IndexMaintainer getIndexMaintainer(PTable pTable, PhoenixConnection phoenixConnection) {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PName getDefaultFamilyName() {
    return null;
  }

  @Override
  public boolean isWALDisabled() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public boolean isMultiTenant() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public boolean getStoreNulls() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public boolean isTransactional() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public ViewType getViewType() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public String getViewStatement() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public Short getViewIndexId() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public PTableKey getKey() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public IndexType getIndexType() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public int getBaseColumnCount() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public boolean rowKeyOrderOptimizable() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public int getRowTimestampColPos() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public long getUpdateCacheFrequency() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public boolean isNamespaceMapped() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public String getAutoPartitionSeqName() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public boolean isAppendOnlySchema() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public ImmutableStorageScheme getImmutableStorageScheme() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public QualifierEncodingScheme getEncodingScheme() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public EncodedCQCounter getEncodedCQCounter() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public boolean useStatsForParallelization() {
    throw new UnsupportedOperationException("Not implement");
  }

  @Override
  public int getEstimatedSize() {
    throw new UnsupportedOperationException("Not implement");
  }

  public static class ThinClientPColumn implements PColumn {

    private String colName;

    private PDataType pDataType;

    public ThinClientPColumn(String colName, PDataType pDataType) {
      this.colName = colName;
      this.pDataType = pDataType;
    }

    @Override
    public PName getName() {
      throw new UnsupportedOperationException("Not implement");
    }

    @Override
    public PName getFamilyName() {
      throw new UnsupportedOperationException("Not implement");
    }

    @Override
    public int getPosition() {
      throw new UnsupportedOperationException("Not implement");
    }

    @Override
    public Integer getArraySize() {
      throw new UnsupportedOperationException("Not implement");
    }

    @Override
    public byte[] getViewConstant() {
      throw new UnsupportedOperationException("Not implement");
    }

    @Override
    public boolean isViewReferenced() {
      throw new UnsupportedOperationException("Not implement");
    }

    @Override
    public int getEstimatedSize() {
      throw new UnsupportedOperationException("Not implement");
    }

    @Override
    public String getExpressionStr() {
      throw new UnsupportedOperationException("Not implement");
    }

    @Override
    public boolean isRowTimestamp() {
      throw new UnsupportedOperationException("Not implement");
    }

    @Override
    public boolean isDynamic() {
      throw new UnsupportedOperationException("Not implement");
    }

    @Override
    public byte[] getColumnQualifierBytes() {
      throw new UnsupportedOperationException("Not implement");
    }

    @Override
    public boolean isNullable() {
      throw new UnsupportedOperationException("Not implement");
    }

    @Override
    public PDataType getDataType() {
      return pDataType;
    }

    @Override
    public Integer getMaxLength() {
      throw new UnsupportedOperationException("Not implement");
    }

    @Override
    public Integer getScale() {
      throw new UnsupportedOperationException("Not implement");
    }

    @Override
    public SortOrder getSortOrder() {
      throw new UnsupportedOperationException("Not implement");
    }
  }

}
