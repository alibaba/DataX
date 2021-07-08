package cn.sensorsdata.datax.plugin.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SaColumnItem implements Serializable {

    private static final long serialVersionUID = -1306205173606479993L;

    private Integer index;

    private String targetColumnName;

    private List<DataConverter> dataConverters;

}
