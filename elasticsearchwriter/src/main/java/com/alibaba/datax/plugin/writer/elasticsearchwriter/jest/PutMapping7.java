package com.alibaba.datax.plugin.writer.elasticsearchwriter.jest;

import io.searchbox.action.GenericResultAbstractAction;
import io.searchbox.client.config.ElasticsearchVersion;

public class PutMapping7  extends GenericResultAbstractAction {
	protected PutMapping7(PutMapping7.Builder builder) {
		super(builder);

		this.indexName = builder.index;
		this.payload = builder.source;
	}

	@Override
	protected String buildURI(ElasticsearchVersion elasticsearchVersion) {
		return super.buildURI(elasticsearchVersion) + "/_mapping";
	}

	@Override
	public String getRestMethodName() {
		return "PUT";
	}

	public static class Builder extends GenericResultAbstractAction.Builder<PutMapping7, PutMapping7.Builder> {
		private String index;
		private Object source;

		public Builder(String index, Object source) {
			this.index = index;
			this.source = source;
		}

		@Override
		public PutMapping7 build() {
			return new PutMapping7(this);
		}
	}

}
