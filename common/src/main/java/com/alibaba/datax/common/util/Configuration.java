package com.alibaba.datax.common.util;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.*;
import java.util.*;

/**
 * Configuration 提供多级JSON配置信息无损存储 <br>
 * <br>
 * <p/>
 * 实例代码:<br>
 * <p/>
 * 获取job的配置信息<br>
 * Configuration configuration = Configuration.from(new File("Config.json")); <br>
 * String jobContainerClass =
 * configuration.getString("core.container.job.class"); <br>
 * <p/>
 * <br>
 * 设置多级List <br>
 * configuration.set("job.reader.parameter.jdbcUrl", Arrays.asList(new String[]
 * {"jdbc", "jdbc"}));
 * <p/>
 * <p/>
 * <br>
 * <br>
 * 合并Configuration: <br>
 * configuration.merge(another);
 * <p/>
 * <p/>
 * <br>
 * <br>
 * <br>
 * <p/>
 * Configuration 存在两种较好地实现方式<br>
 * 第一种是将JSON配置信息中所有的Key全部打平，用a.b.c的级联方式作为Map的Key，内部使用一个Map保存信息 <br>
 * 第二种是将JSON的对象直接使用结构化树形结构保存<br>
 * <p/>
 * 目前使用的第二种实现方式，使用第一种的问题在于: <br>
 * 1. 插入新对象，比较难处理，例如a.b.c="bazhen"，此时如果需要插入a="bazhen"，也即是根目录下第一层所有类型全部要废弃
 * ，使用"bazhen"作为value，第一种方式使用字符串表示key，难以处理这类问题。 <br>
 * 2. 返回树形结构，例如 a.b.c.d = "bazhen"，如果返回"a"下的所有元素，实际上是一个Map，需要合并处理 <br>
 * 3. 输出JSON，将上述对象转为JSON，要把上述Map的多级key转为树形结构，并输出为JSON <br>
 */
public class Configuration {

    /**
     * 对于加密的keyPath，需要记录下来
     * 为的是后面分布式情况下将该值加密后抛到DataXServer中
     */
    private Set<String> secretKeyPathSet =
            new HashSet<String>();

	private Object root = null;

	/**
	 * 初始化空白的Configuration
	 */
	public static Configuration newDefault() {
		return Configuration.from("{}");
	}

	/**
	 * 从JSON字符串加载Configuration
	 */
	public static Configuration from(String json) {
        json = StrUtil.replaceVariable(json);
		checkJSON(json);

		try {
			return new Configuration(json);
		} catch (Exception e) {
			throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR,
					e);
		}

	}

    /**
	 * 从包括json的File对象加载Configuration
	 */
	public static Configuration from(File file) {
		try {
			return Configuration.from(IOUtils
					.toString(new FileInputStream(file)));
		} catch (FileNotFoundException e) {
			throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR,
					String.format("配置信息错误，您提供的配置文件[%s]不存在. 请检查您的配置文件.", file.getAbsolutePath()));
		} catch (IOException e) {
			throw DataXException.asDataXException(
					CommonErrorCode.CONFIG_ERROR,
					String.format("配置信息错误. 您提供配置文件[%s]读取失败，错误原因: %s. 请检查您的配置文件的权限设置.",
							file.getAbsolutePath(), e));
		}
	}

	/**
	 * 从包括json的InputStream对象加载Configuration
	 */
	public static Configuration from(InputStream is) {
		try {
			return Configuration.from(IOUtils.toString(is));
		} catch (IOException e) {
			throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR,
					String.format("请检查您的配置文件. 您提供的配置文件读取失败，错误原因: %s. 请检查您的配置文件的权限设置.", e));
		}
	}

	/**
	 * 从Map对象加载Configuration
	 */
	public static Configuration from(final Map<String, Object> object) {
		return Configuration.from(Configuration.toJSONString(object));
	}

	/**
	 * 从List对象加载Configuration
	 */
	public static Configuration from(final List<Object> object) {
		return Configuration.from(Configuration.toJSONString(object));
	}

	public String getNecessaryValue(String key, ErrorCode errorCode) {
		String value = this.getString(key, null);
		if (StringUtils.isBlank(value)) {
			throw DataXException.asDataXException(errorCode,
					String.format("您提供配置文件有误，[%s]是必填参数，不允许为空或者留白 .", key));
		}

		return value;
	}
	
	public String getUnnecessaryValue(String key,String defaultValue,ErrorCode errorCode) {
		String value = this.getString(key, defaultValue);
		if (StringUtils.isBlank(value)) {
			value = defaultValue;
		}
		return value;
	}

    public Boolean getNecessaryBool(String key, ErrorCode errorCode) {
        Boolean value = this.getBool(key);
        if (value == null) {
            throw DataXException.asDataXException(errorCode,
                    String.format("您提供配置文件有误，[%s]是必填参数，不允许为空或者留白 .", key));
        }

        return value;
    }

	/**
	 * 根据用户提供的json path，寻址具体的对象。
	 * <p/>
	 * <br>
	 * <p/>
	 * NOTE: 目前仅支持Map以及List下标寻址, 例如:
	 * <p/>
	 * <br />
	 * <p/>
	 * 对于如下JSON
	 * <p/>
	 * {"a": {"b": {"c": [0,1,2,3]}}}
	 * <p/>
	 * config.get("") 返回整个Map <br>
	 * config.get("a") 返回a下属整个Map <br>
	 * config.get("a.b.c") 返回c对应的数组List <br>
	 * config.get("a.b.c[0]") 返回数字0
	 * 
	 * @return Java表示的JSON对象，如果path不存在或者对象不存在，均返回null。
	 */
	public Object get(final String path) {
		this.checkPath(path);
		try {
			return this.findObject(path);
		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * 用户指定部分path，获取Configuration的子集
	 * <p/>
	 * <br>
	 * 如果path获取的路径或者对象不存在，返回null
	 */
	public Configuration getConfiguration(final String path) {
		Object object = this.get(path);
		if (null == object) {
			return null;
		}

		return Configuration.from(Configuration.toJSONString(object));
	}

	/**
	 * 根据用户提供的json path，寻址String对象
	 * 
	 * @return String对象，如果path不存在或者String不存在，返回null
	 */
	public String getString(final String path) {
		Object string = this.get(path);
		if (null == string) {
			return null;
		}
		return String.valueOf(string);
	}

	/**
	 * 根据用户提供的json path，寻址String对象，如果对象不存在，返回默认字符串
	 * 
	 * @return String对象，如果path不存在或者String不存在，返回默认字符串
	 */
	public String getString(final String path, final String defaultValue) {
		String result = this.getString(path);

		if (null == result) {
			return defaultValue;
		}

		return result;
	}

	/**
	 * 根据用户提供的json path，寻址Character对象
	 * 
	 * @return Character对象，如果path不存在或者Character不存在，返回null
	 */
	public Character getChar(final String path) {
		String result = this.getString(path);
		if (null == result) {
			return null;
		}

		try {
			return CharUtils.toChar(result);
		} catch (Exception e) {
			throw DataXException.asDataXException(
					CommonErrorCode.CONFIG_ERROR,
					String.format("任务读取配置文件出错. 因为配置文件路径[%s] 值非法，期望是字符类型: %s. 请检查您的配置并作出修改.", path,
							e.getMessage()));
		}
	}

	/**
	 * 根据用户提供的json path，寻址Boolean对象，如果对象不存在，返回默认Character对象
	 * 
	 * @return Character对象，如果path不存在或者Character不存在，返回默认Character对象
	 */
	public Character getChar(final String path, char defaultValue) {
		Character result = this.getChar(path);
		if (null == result) {
			return defaultValue;
		}
		return result;
	}

	/**
	 * 根据用户提供的json path，寻址Boolean对象
	 * 
	 * @return Boolean对象，如果path值非true,false ，将报错.特别注意：当 path 不存在时，会返回：null.
	 */
	public Boolean getBool(final String path) {
		String result = this.getString(path);

		if (null == result) {
			return null;
		} else if ("true".equalsIgnoreCase(result)) {
			return Boolean.TRUE;
		} else if ("false".equalsIgnoreCase(result)) {
			return Boolean.FALSE;
		} else {
			throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR,
					String.format("您提供的配置信息有误，因为从[%s]获取的值[%s]无法转换为bool类型. 请检查源表的配置并且做出相应的修改.",
							path, result));
		}

	}

	/**
	 * 根据用户提供的json path，寻址Boolean对象，如果对象不存在，返回默认Boolean对象
	 * 
	 * @return Boolean对象，如果path不存在或者Boolean不存在，返回默认Boolean对象
	 */
	public Boolean getBool(final String path, boolean defaultValue) {
		Boolean result = this.getBool(path);
		if (null == result) {
			return defaultValue;
		}
		return result;
	}

	/**
	 * 根据用户提供的json path，寻址Integer对象
	 * 
	 * @return Integer对象，如果path不存在或者Integer不存在，返回null
	 */
	public Integer getInt(final String path) {
		String result = this.getString(path);
		if (null == result) {
			return null;
		}

		try {
			return Integer.valueOf(result);
		} catch (Exception e) {
			throw DataXException.asDataXException(
					CommonErrorCode.CONFIG_ERROR,
					String.format("任务读取配置文件出错. 配置文件路径[%s] 值非法, 期望是整数类型: %s. 请检查您的配置并作出修改.", path,
							e.getMessage()));
		}
	}

	/**
	 * 根据用户提供的json path，寻址Integer对象，如果对象不存在，返回默认Integer对象
	 * 
	 * @return Integer对象，如果path不存在或者Integer不存在，返回默认Integer对象
	 */
	public Integer getInt(final String path, int defaultValue) {
		Integer object = this.getInt(path);
		if (null == object) {
			return defaultValue;
		}
		return object;
	}

	/**
	 * 根据用户提供的json path，寻址Long对象
	 * 
	 * @return Long对象，如果path不存在或者Long不存在，返回null
	 */
	public Long getLong(final String path) {
		String result = this.getString(path);
		if (StringUtils.isBlank(result)) {
			return null;
		}

		try {
			return Long.valueOf(result);
		} catch (Exception e) {
			throw DataXException.asDataXException(
					CommonErrorCode.CONFIG_ERROR,
					String.format("任务读取配置文件出错. 配置文件路径[%s] 值非法, 期望是整数类型: %s. 请检查您的配置并作出修改.", path,
							e.getMessage()));
		}
	}

	/**
	 * 根据用户提供的json path，寻址Long对象，如果对象不存在，返回默认Long对象
	 * 
	 * @return Long对象，如果path不存在或者Integer不存在，返回默认Long对象
	 */
	public Long getLong(final String path, long defaultValue) {
		Long result = this.getLong(path);
		if (null == result) {
			return defaultValue;
		}
		return result;
	}

	/**
	 * 根据用户提供的json path，寻址Double对象
	 * 
	 * @return Double对象，如果path不存在或者Double不存在，返回null
	 */
	public Double getDouble(final String path) {
		String result = this.getString(path);
		if (StringUtils.isBlank(result)) {
			return null;
		}

		try {
			return Double.valueOf(result);
		} catch (Exception e) {
			throw DataXException.asDataXException(
					CommonErrorCode.CONFIG_ERROR,
					String.format("任务读取配置文件出错. 配置文件路径[%s] 值非法, 期望是浮点类型: %s. 请检查您的配置并作出修改.", path,
							e.getMessage()));
		}
	}

	/**
	 * 根据用户提供的json path，寻址Double对象，如果对象不存在，返回默认Double对象
	 * 
	 * @return Double对象，如果path不存在或者Double不存在，返回默认Double对象
	 */
	public Double getDouble(final String path, double defaultValue) {
		Double result = this.getDouble(path);
		if (null == result) {
			return defaultValue;
		}
		return result;
	}

	/**
	 * 根据用户提供的json path，寻址List对象，如果对象不存在，返回null
	 */
	@SuppressWarnings("unchecked")
	public List<Object> getList(final String path) {
		List<Object> list = this.get(path, List.class);
		if (null == list) {
			return null;
		}
		return list;
	}

	public <T> List<T> getListWithJson(final String path, Class<T> t) {
		Object object = this.get(path, List.class);
		if (null == object) {
			return null;
		}

		return JSON.parseArray(JSON.toJSONString(object),t);
	}

	/**
	 * 根据用户提供的json path，寻址List对象，如果对象不存在，返回null
	 */
	@SuppressWarnings("unchecked")
	public <T> List<T> getList(final String path, Class<T> t) {
		Object object = this.get(path, List.class);
		if (null == object) {
			return null;
		}

		List<T> result = new ArrayList<T>();

		List<Object> origin = (List<Object>) object;
		for (final Object each : origin) {
			result.add((T) each);
		}

		return result;
	}

	/**
	 * 根据用户提供的json path，寻址List对象，如果对象不存在，返回默认List
	 */
	@SuppressWarnings("unchecked")
	public List<Object> getList(final String path,
			final List<Object> defaultList) {
		Object object = this.getList(path);
		if (null == object) {
			return defaultList;
		}
		return (List<Object>) object;
	}

	/**
	 * 根据用户提供的json path，寻址List对象，如果对象不存在，返回默认List
	 */
	public <T> List<T> getList(final String path, final List<T> defaultList,
			Class<T> t) {
		List<T> list = this.getList(path, t);
		if (null == list) {
			return defaultList;
		}
		return list;
	}

	/**
	 * 根据用户提供的json path，寻址包含Configuration的List，如果对象不存在，返回默认null
	 */
	public List<Configuration> getListConfiguration(final String path) {
		List<Object> lists = getList(path);
		if (lists == null) {
			return null;
		}

		List<Configuration> result = new ArrayList<Configuration>();
		for (final Object object : lists) {
			result.add(Configuration.from(Configuration.toJSONString(object)));
		}
		return result;
	}

	/**
	 * 根据用户提供的json path，寻址Map对象，如果对象不存在，返回null
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> getMap(final String path) {
		Map<String, Object> result = this.get(path, Map.class);
		if (null == result) {
			return null;
		}
		return result;
	}

	/**
	 * 根据用户提供的json path，寻址Map对象，如果对象不存在，返回null;
	 */
	@SuppressWarnings("unchecked")
	public <T> Map<String, T> getMap(final String path, Class<T> t) {
		Map<String, Object> map = this.get(path, Map.class);
		if (null == map) {
			return null;
		}

		Map<String, T> result = new HashMap<String, T>();
		for (final String key : map.keySet()) {
			result.put(key, (T) map.get(key));
		}

		return result;
	}

	/**
	 * 根据用户提供的json path，寻址Map对象，如果对象不存在，返回默认map
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> getMap(final String path,
			final Map<String, Object> defaultMap) {
		Object object = this.getMap(path);
		if (null == object) {
			return defaultMap;
		}
		return (Map<String, Object>) object;
	}

	/**
	 * 根据用户提供的json path，寻址Map对象，如果对象不存在，返回默认map
	 */
	public <T> Map<String, T> getMap(final String path,
			final Map<String, T> defaultMap, Class<T> t) {
		Map<String, T> result = getMap(path, t);
		if (null == result) {
			return defaultMap;
		}
		return result;
	}

	/**
	 * 根据用户提供的json path，寻址包含Configuration的Map，如果对象不存在，返回默认null
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Configuration> getMapConfiguration(final String path) {
		Map<String, Object> map = this.get(path, Map.class);
		if (null == map) {
			return null;
		}

		Map<String, Configuration> result = new HashMap<String, Configuration>();
		for (final String key : map.keySet()) {
			result.put(key, Configuration.from(Configuration.toJSONString(map
					.get(key))));
		}

		return result;
	}

	/**
	 * 根据用户提供的json path，寻址具体的对象，并转为用户提供的类型
	 * <p/>
	 * <br>
	 * <p/>
	 * NOTE: 目前仅支持Map以及List下标寻址, 例如:
	 * <p/>
	 * <br />
	 * <p/>
	 * 对于如下JSON
	 * <p/>
	 * {"a": {"b": {"c": [0,1,2,3]}}}
	 * <p/>
	 * config.get("") 返回整个Map <br>
	 * config.get("a") 返回a下属整个Map <br>
	 * config.get("a.b.c") 返回c对应的数组List <br>
	 * config.get("a.b.c[0]") 返回数字0
	 * 
	 * @return Java表示的JSON对象，如果转型失败，将抛出异常
	 */
	@SuppressWarnings("unchecked")
	public <T> T get(final String path, Class<T> clazz) {
		this.checkPath(path);
		return (T) this.get(path);
	}

	/**
	 * 格式化Configuration输出
	 */
	public String beautify() {
		return JSON.toJSONString(this.getInternal(),
				JSONWriter.Feature.PrettyFormat);
	}

	/**
	 * 根据用户提供的json path，插入指定对象，并返回之前存在的对象(如果存在)
	 * <p/>
	 * <br>
	 * <p/>
	 * 目前仅支持.以及数组下标寻址, 例如:
	 * <p/>
	 * <br />
	 * <p/>
	 * config.set("a.b.c[3]", object);
	 * <p/>
	 * <br>
	 * 对于插入对象，Configuration不做任何限制，但是请务必保证该对象是简单对象(包括Map<String,
	 * Object>、List<Object>)，不要使用自定义对象，否则后续对于JSON序列化等情况会出现未定义行为。
	 * 
	 * @param path
	 *            JSON path对象
	 * @param object
	 *            需要插入的对象
	 * @return Java表示的JSON对象
	 */
	public Object set(final String path, final Object object) {
		checkPath(path);

		Object result = this.get(path);

		setObject(path, extractConfiguration(object));

		return result;
	}

	/**
	 * 获取Configuration下所有叶子节点的key
	 * <p/>
	 * <br>
	 * <p/>
	 * 对于<br>
	 * <p/>
	 * {"a": {"b": {"c": [0,1,2,3]}}, "x": "y"}
	 * <p/>
	 * 下属的key包括: a.b.c[0],a.b.c[1],a.b.c[2],a.b.c[3],x
	 */
	public Set<String> getKeys() {
		Set<String> collect = new HashSet<String>();
		this.getKeysRecursive(this.getInternal(), "", collect);
		return collect;
	}

	/**
	 * 删除path对应的值，如果path不存在，将抛出异常。
	 */
	public Object remove(final String path) {
		final Object result = this.get(path);
		if (null == result) {
			throw DataXException.asDataXException(
					CommonErrorCode.RUNTIME_ERROR,
					String.format("配置文件对应Key[%s]并不存在，该情况是代码编程错误. 请联系DataX团队的同学.", path));
		}

		this.set(path, null);
		return result;
	}

	/**
	 * 合并其他Configuration，并修改两者冲突的KV配置
	 * 
	 * @param another
	 *            合并加入的第三方Configuration
	 * @param updateWhenConflict
	 *            当合并双方出现KV冲突时候，选择更新当前KV，或者忽略该KV
	 * @return 返回合并后对象
	 */
	public Configuration merge(final Configuration another,
			boolean updateWhenConflict) {
		Set<String> keys = another.getKeys();

		for (final String key : keys) {
			// 如果使用更新策略，凡是another存在的key，均需要更新
			if (updateWhenConflict) {
				this.set(key, another.get(key));
				continue;
			}

			// 使用忽略策略，只有another Configuration存在但是当前Configuration不存在的key，才需要更新
			boolean isCurrentExists = this.get(key) != null;
			if (isCurrentExists) {
				continue;
			}

			this.set(key, another.get(key));
		}
		return this;
	}

	@Override
	public String toString() {
		return this.toJSON();
	}

	/**
	 * 将Configuration作为JSON输出
	 */
	public String toJSON() {
		return Configuration.toJSONString(this.getInternal());
	}

	/**
	 * 拷贝当前Configuration，注意，这里使用了深拷贝，避免冲突
	 */
	public Configuration clone() {
		Configuration config = Configuration
				.from(Configuration.toJSONString(this.getInternal()));
        config.addSecretKeyPath(this.secretKeyPathSet);
        return config;
	}

    /**
     * 按照configuration要求格式的path
     * 比如：
     * a.b.c
     * a.b[2].c
     * @param path
     */
    public void addSecretKeyPath(String path) {
        if(StringUtils.isNotBlank(path)) {
            this.secretKeyPathSet.add(path);
        }
    }

    public void addSecretKeyPath(Set<String> pathSet) {
        if(pathSet != null) {
            this.secretKeyPathSet.addAll(pathSet);
        }
    }

    public void setSecretKeyPathSet(Set<String> keyPathSet) {
        if(keyPathSet != null) {
            this.secretKeyPathSet = keyPathSet;
        }
    }

    public boolean isSecretPath(String path) {
        return this.secretKeyPathSet.contains(path);
    }

	@SuppressWarnings("unchecked")
	void getKeysRecursive(final Object current, String path, Set<String> collect) {
		boolean isRegularElement = !(current instanceof Map || current instanceof List);
		if (isRegularElement) {
			collect.add(path);
			return;
		}

		boolean isMap = current instanceof Map;
		if (isMap) {
			Map<String, Object> mapping = ((Map<String, Object>) current);
			for (final String key : mapping.keySet()) {
				if (StringUtils.isBlank(path)) {
					getKeysRecursive(mapping.get(key), key.trim(), collect);
				} else {
					getKeysRecursive(mapping.get(key), path + "." + key.trim(),
							collect);
				}
			}
			return;
		}

		boolean isList = current instanceof List;
		if (isList) {
			List<Object> lists = (List<Object>) current;
			for (int i = 0; i < lists.size(); i++) {
				getKeysRecursive(lists.get(i), path + String.format("[%d]", i),
						collect);
			}
			return;
		}

		return;
	}

	public Object getInternal() {
		return this.root;
	}

	private void setObject(final String path, final Object object) {
		Object newRoot = setObjectRecursive(this.root, split2List(path), 0,
				object);

		if (isSuitForRoot(newRoot)) {
			this.root = newRoot;
			return;
		}

		throw DataXException.asDataXException(CommonErrorCode.RUNTIME_ERROR,
				String.format("值[%s]无法适配您提供[%s]， 该异常代表系统编程错误, 请联系DataX开发团队!",
						ToStringBuilder.reflectionToString(object), path));
	}

	@SuppressWarnings("unchecked")
	private Object extractConfiguration(final Object object) {
		if (object instanceof Configuration) {
			return extractFromConfiguration(object);
		}

		if (object instanceof List) {
			List<Object> result = new ArrayList<Object>();
			for (final Object each : (List<Object>) object) {
				result.add(extractFromConfiguration(each));
			}
			return result;
		}

		if (object instanceof Map) {
			Map<String, Object> result = new HashMap<String, Object>();
			for (final String key : ((Map<String, Object>) object).keySet()) {
				result.put(key,
						extractFromConfiguration(((Map<String, Object>) object)
								.get(key)));
			}
			return result;
		}

		return object;
	}

	private Object extractFromConfiguration(final Object object) {
		if (object instanceof Configuration) {
			return ((Configuration) object).getInternal();
		}

		return object;
	}

	Object buildObject(final List<String> paths, final Object object) {
		if (null == paths) {
			throw DataXException.asDataXException(
					CommonErrorCode.RUNTIME_ERROR,
					"Path不能为null，该异常代表系统编程错误, 请联系DataX开发团队 !");
		}

		if (1 == paths.size() && StringUtils.isBlank(paths.get(0))) {
			return object;
		}

		Object child = object;
		for (int i = paths.size() - 1; i >= 0; i--) {
			String path = paths.get(i);

			if (isPathMap(path)) {
				Map<String, Object> mapping = new HashMap<String, Object>();
				mapping.put(path, child);
				child = mapping;
				continue;
			}

			if (isPathList(path)) {
				List<Object> lists = new ArrayList<Object>(
						this.getIndex(path) + 1);
				expand(lists, this.getIndex(path) + 1);
				lists.set(this.getIndex(path), child);
				child = lists;
				continue;
			}

			throw DataXException.asDataXException(
					CommonErrorCode.RUNTIME_ERROR, String.format(
							"路径[%s]出现非法值类型[%s]，该异常代表系统编程错误, 请联系DataX开发团队! .",
							StringUtils.join(paths, "."), path));
		}

		return child;
	}

	@SuppressWarnings("unchecked")
	Object setObjectRecursive(Object current, final List<String> paths,
			int index, final Object value) {

		// 如果是已经超出path，我们就返回value即可，作为最底层叶子节点
		boolean isLastIndex = index == paths.size();
		if (isLastIndex) {
			return value;
		}

		String path = paths.get(index).trim();
		boolean isNeedMap = isPathMap(path);
		if (isNeedMap) {
			Map<String, Object> mapping;

			// 当前不是map，因此全部替换为map，并返回新建的map对象
			boolean isCurrentMap = current instanceof Map;
			if (!isCurrentMap) {
				mapping = new HashMap<String, Object>();
				mapping.put(
						path,
						buildObject(paths.subList(index + 1, paths.size()),
								value));
				return mapping;
			}

			// 当前是map，但是没有对应的key，也就是我们需要新建对象插入该map，并返回该map
			mapping = ((Map<String, Object>) current);
			boolean hasSameKey = mapping.containsKey(path);
			if (!hasSameKey) {
				mapping.put(
						path,
						buildObject(paths.subList(index + 1, paths.size()),
								value));
				return mapping;
			}

			// 当前是map，而且还竟然存在这个值，好吧，继续递归遍历
			current = mapping.get(path);
			mapping.put(path,
					setObjectRecursive(current, paths, index + 1, value));
			return mapping;
		}

		boolean isNeedList = isPathList(path);
		if (isNeedList) {
			List<Object> lists;
			int listIndexer = getIndex(path);

			// 当前是list，直接新建并返回即可
			boolean isCurrentList = current instanceof List;
			if (!isCurrentList) {
				lists = expand(new ArrayList<Object>(), listIndexer + 1);
				lists.set(
						listIndexer,
						buildObject(paths.subList(index + 1, paths.size()),
								value));
				return lists;
			}

			// 当前是list，但是对应的indexer是没有具体的值，也就是我们新建对象然后插入到该list，并返回该List
			lists = (List<Object>) current;
			lists = expand(lists, listIndexer + 1);

			boolean hasSameIndex = lists.get(listIndexer) != null;
			if (!hasSameIndex) {
				lists.set(
						listIndexer,
						buildObject(paths.subList(index + 1, paths.size()),
								value));
				return lists;
			}

			// 当前是list，并且存在对应的index，没有办法继续递归寻找
			current = lists.get(listIndexer);
			lists.set(listIndexer,
					setObjectRecursive(current, paths, index + 1, value));
			return lists;
		}

		throw DataXException.asDataXException(CommonErrorCode.RUNTIME_ERROR,
				"该异常代表系统编程错误, 请联系DataX开发团队 !");
	}

	private Object findObject(final String path) {
		boolean isRootQuery = StringUtils.isBlank(path);
		if (isRootQuery) {
			return this.root;
		}

		Object target = this.root;

		for (final String each : split2List(path)) {
			if (isPathMap(each)) {
				target = findObjectInMap(target, each);
				continue;
			} else {
				target = findObjectInList(target, each);
				continue;
			}
		}

		return target;
	}

	@SuppressWarnings("unchecked")
	private Object findObjectInMap(final Object target, final String index) {
		boolean isMap = (target instanceof Map);
		if (!isMap) {
			throw new IllegalArgumentException(String.format(
					"您提供的配置文件有误. 路径[%s]需要配置Json格式的Map对象，但该节点发现实际类型是[%s]. 请检查您的配置并作出修改.",
					index, target.getClass().toString()));
		}

		Object result = ((Map<String, Object>) target).get(index);
		if (null == result) {
			throw new IllegalArgumentException(String.format(
					"您提供的配置文件有误. 路径[%s]值为null，datax无法识别该配置. 请检查您的配置并作出修改.", index));
		}

		return result;
	}

	@SuppressWarnings({ "unchecked" })
	private Object findObjectInList(final Object target, final String each) {
		boolean isList = (target instanceof List);
		if (!isList) {
			throw new IllegalArgumentException(String.format(
					"您提供的配置文件有误. 路径[%s]需要配置Json格式的Map对象，但该节点发现实际类型是[%s]. 请检查您的配置并作出修改.",
					each, target.getClass().toString()));
		}

		String index = each.replace("[", "").replace("]", "");
		if (!StringUtils.isNumeric(index)) {
			throw new IllegalArgumentException(
					String.format(
							"系统编程错误，列表下标必须为数字类型，但该节点发现实际类型是[%s] ，该异常代表系统编程错误, 请联系DataX开发团队 !",
							index));
		}

		return ((List<Object>) target).get(Integer.valueOf(index));
	}

	private List<Object> expand(List<Object> list, int size) {
		int expand = size - list.size();
		while (expand-- > 0) {
			list.add(null);
		}
		return list;
	}

	private boolean isPathList(final String path) {
		return path.contains("[") && path.contains("]");
	}

	private boolean isPathMap(final String path) {
		return StringUtils.isNotBlank(path) && !isPathList(path);
	}

	private int getIndex(final String index) {
		return Integer.valueOf(index.replace("[", "").replace("]", ""));
	}

	private boolean isSuitForRoot(final Object object) {
		if (null != object && (object instanceof List || object instanceof Map)) {
			return true;
		}

		return false;
	}

	private String split(final String path) {
		return StringUtils.replace(path, "[", ".[");
	}

	private List<String> split2List(final String path) {
		return Arrays.asList(StringUtils.split(split(path), "."));
	}

	private void checkPath(final String path) {
		if (null == path) {
			throw new IllegalArgumentException(
					"系统编程错误, 该异常代表系统编程错误, 请联系DataX开发团队!.");
		}

		for (final String each : StringUtils.split(path, ".")) {
			if (StringUtils.isBlank(each)) {
				throw new IllegalArgumentException(String.format(
						"系统编程错误, 路径[%s]不合法, 路径层次之间不能出现空白字符 .", path));
			}
		}
	}

	@SuppressWarnings("unused")
	private String toJSONPath(final String path) {
		return (StringUtils.isBlank(path) ? "$" : "$." + path).replace("$.[",
				"$[");
	}

	private static void checkJSON(final String json) {
		if (StringUtils.isBlank(json)) {
			throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR,
					"配置信息错误. 因为您提供的配置信息不是合法的JSON格式, JSON不能为空白. 请按照标准json格式提供配置信息. ");
		}
	}

	private Configuration(final String json) {
		try {
			this.root = JSON.parse(json);
		} catch (Exception e) {
			throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR,
					String.format("配置信息错误. 您提供的配置信息不是合法的JSON格式: %s . 请按照标准json格式提供配置信息. ", e.getMessage()));
		}
	}

	private static String toJSONString(final Object object) {
		return JSON.toJSONString(object);
	}

	public Set<String> getSecretKeyPathSet() {
		return secretKeyPathSet;
	}
}
