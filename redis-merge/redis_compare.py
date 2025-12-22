import redis
import time
import logging
import traceback
import json

# 配置日志
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_redis_with_retry(config, max_retries=3, retry_delay=1):
    """
    连接Redis实例，支持重试机制
    
    :param config: Redis配置
    :param max_retries: 最大重试次数
    :param retry_delay: 重试延迟（秒）
    :return: Redis连接实例
    """
    retry_count = 0
    
    # 添加默认超时配置
    if 'socket_connect_timeout' not in config:
        config['socket_connect_timeout'] = 5
    if 'socket_timeout' not in config:
        config['socket_timeout'] = 5
    
    while retry_count < max_retries:
        try:
            r = redis.Redis(**config)
            # 测试连接
            r.ping()
            logger.info(f"成功连接Redis: {config.get('host', 'localhost')}:{config.get('port', 6379)}/db{config.get('db', 0)}")
            return r
        except Exception as e:
            retry_count += 1
            logger.warning(f"连接Redis失败 (尝试 {retry_count}/{max_retries}): {e}")
            if retry_count < max_retries:
                time.sleep(retry_delay)
    
    raise ConnectionError(f"无法连接Redis: {config.get('host', 'localhost')}:{config.get('port', 6379)}/db{config.get('db', 0)}")



def scan_keys(redis_client, pattern="*", count=1000):
    """
    使用SCAN命令批量获取键，避免阻塞Redis
    
    :param redis_client: Redis连接实例
    :param pattern: 键匹配模式
    :param count: 每次SCAN返回的键数量
    :return: 所有匹配的键集合
    """
    keys = set()
    cursor = 0
    
    while True:
        try:
            cursor, batch_keys = redis_client.scan(cursor=cursor, match=pattern, count=count)
            keys.update(batch_keys)
            # 打印当前批次获取的键数量
            logger.info(f"当前批次获取 {len(batch_keys)} 个键")
            # 打印当前扫描进度
            logger.info(f"当前扫描游标: {cursor}")
            # 打印当前获取的所有键
            logger.info(f"当前获取的键: {list(batch_keys)}")
            
            # 如果游标为0，表示扫描完成
            if cursor == 0:
                break
        except Exception as e:
            logger.error(f"扫描键失败: {e}")
            raise
    
    logger.info(f"获取到 {len(keys)} 个键")
    return keys


def get_key_type(redis_conn, key):
    """
    获取键的数据类型
    
    :param redis_conn: Redis连接
    :param key: 键名
    :return: 数据类型字符串
    """
    try:
        if not redis_conn.exists(key):
            return None
            
        key_type = redis_conn.type(key)
        # 处理decode_responses=True的情况
        if isinstance(key_type, bytes):
            return key_type.decode()
        return key_type
    except Exception as e:
        logger.error(f"获取键 {key} 类型失败: {e}")
        return None


def compare_and_select_latest(redis1_config, redis2_config, batch_size=1000):
    """
    对比两个Redis数据库，选择最新的value
    
    :param redis1_config: 第一个Redis配置
    :param redis2_config: 第二个Redis配置
    :param batch_size: 批量处理大小
    :return: 比较结果字典
    """
    try:
        # 连接两个Redis实例
        r1 = connect_redis_with_retry(redis1_config)
        r2 = connect_redis_with_retry(redis2_config)
        
        # 获取所有keys（使用scan避免阻塞）
        keys1 = scan_keys(r1, count=batch_size)
        keys2 = scan_keys(r2, count=batch_size)
        all_keys = keys1.union(keys2)
        total_keys = len(all_keys)
        logger.info(f"总共有 {total_keys} 个不同的键需要比较")
        
        results = {
            'only_in_db1': [],
            'only_in_db2': [],
            'conflict_resolved': [],
            'equal_values': [],
            'type_mismatch': []
        }
        
        processed_count = 0
        start_time = time.time()
        
        for key in all_keys:
            processed_count += 1
            
            # 获取类型
            type1 = get_key_type(r1, key)
            type2 = get_key_type(r2, key)
            
            # 定期记录进度
            if processed_count % 100 == 0 or processed_count == total_keys:
                elapsed_time = time.time() - start_time
                logger.info(f"已处理 {processed_count}/{total_keys} 个键 ({(processed_count/total_keys)*100:.1f}%)，耗时 {elapsed_time:.2f} 秒")
            
            # 处理不同数据类型的比较
            if not r1.exists(key) and r2.exists(key):
                results['only_in_db2'].append(key)
            elif r1.exists(key) and not r2.exists(key):
                results['only_in_db1'].append(key)
            else:
                # 两个数据库都有这个key
                if type1 != type2:
                    logger.warning(f"键 {key} 在两个数据库中的类型不同: db1={type1}, db2={type2}")
                    results['type_mismatch'].append((key, type1, type2))
                    continue
                
                # 根据类型选择最新值
                latest_value = select_latest_by_type(r1, r2, key, type1)
                
                if latest_value == "db1":
                    results['conflict_resolved'].append((key, "db1"))
                elif latest_value == "db2":
                    results['conflict_resolved'].append((key, "db2"))
                else:
                    results['equal_values'].append(key)
        
        logger.info(f"比较完成！总耗时 {time.time() - start_time:.2f} 秒")
        return results
        
    except Exception as e:
        logger.error(f"比较过程中发生错误: {e}")
        logger.debug(traceback.format_exc())
        raise

def try_parse_timestamp(value):
    """
    尝试从值中解析时间戳
    
    :param value: 要解析的值
    :return: 时间戳（浮点数）或None
    """
    if isinstance(value, (int, float)):
        return float(value)
    
    if isinstance(value, bytes):
        try:
            value = value.decode()
        except UnicodeDecodeError:
            return None
    
    if isinstance(value, str):
        # 尝试解析为数字
        try:
            return float(value)
        except (ValueError, TypeError):
            pass
        
        # 尝试解析为JSON
        try:
            data = json.loads(value)
            # 检查常见的时间戳字段
            for field in ['timestamp', 'update_time', 'create_time', 'last_modified', 'time']:
                if field in data and isinstance(data[field], (int, float, str)):
                    return try_parse_timestamp(data[field])
        except (json.JSONDecodeError, TypeError):
            pass
    
    return None


def select_latest_by_type(r1, r2, key, key_type, default_preference="db2"):
    """
    根据数据类型选择最新的value
    
    :param r1: Redis连接1
    :param r2: Redis连接2
    :param key: 键名
    :param key_type: 数据类型
    :param default_preference: 默认优先选择的数据库 ("db1" 或 "db2")
    :return: "equal"表示值相同，"db1"或"db2"表示选择哪个数据库的值
    """
    try:
        if key_type == "string":
            val1 = r1.get(key)
            val2 = r2.get(key)
            
            if val1 == val2:
                return "equal"
            else:
                # 尝试从值中解析时间戳
                ts1 = try_parse_timestamp(val1)
                ts2 = try_parse_timestamp(val2)
                
                if ts1 is not None and ts2 is not None:
                    return "db1" if ts1 > ts2 else "db2"
                
                # 尝试获取对象的最后访问时间
                try:
                    idletime1 = r1.object("idletime", key)
                    idletime2 = r2.object("idletime", key)
                    # idletime越小表示最近被访问过，优先选择
                    return "db1" if idletime1 < idletime2 else "db2"
                except Exception:
                    # 如果无法获取idletime，使用默认偏好
                    return default_preference
    
        elif key_type == "hash":
            val1 = r1.hgetall(key)
            val2 = r2.hgetall(key)
            
            if val1 == val2:
                return "equal"
            else:
                # 检查常见的时间戳字段
                timestamp_fields = ['update_time', 'create_time', 'last_modified', 'timestamp', 'time']
                
                for field in timestamp_fields:
                    if field in val1 and field in val2:
                        ts1 = try_parse_timestamp(val1[field])
                        ts2 = try_parse_timestamp(val2[field])
                        
                        if ts1 is not None and ts2 is not None:
                            return "db1" if ts1 > ts2 else "db2"
                
                # 如果没有时间字段或解析失败，比较字段数量
                if len(val1) != len(val2):
                    return "db1" if len(val1) > len(val2) else "db2"
                
                # 比较字段值的更新时间（如果有）
                updated_fields = 0
                for field in set(val1.keys()) & set(val2.keys()):
                    if val1[field] != val2[field]:
                        updated_fields += 1
                
                # 如果有更新的字段，使用默认偏好
                return default_preference
    
        elif key_type == "list":
            val1 = r1.lrange(key, 0, -1)
            val2 = r2.lrange(key, 0, -1)
            
            if val1 == val2:
                return "equal"
            else:
                # 比较列表长度，选择更长的列表
                return "db1" if len(val1) > len(val2) else "db2"
    
        elif key_type == "set":
            val1 = r1.smembers(key)
            val2 = r2.smembers(key)
            
            if val1 == val2:
                return "equal"
            else:
                # 比较集合大小，选择更大的集合
                if len(val1) != len(val2):
                    return "db1" if len(val1) > len(val2) else "db2"
                else:
                    # 如果集合大小相同，比较最后访问时间
                    try:
                        idletime1 = r1.object("idletime", key)
                        idletime2 = r2.object("idletime", key)
                        # idletime越小表示最近被访问过，优先选择
                        return "db1" if idletime1 < idletime2 else "db2"
                    except Exception:
                        # 如果无法获取idletime，使用默认偏好
                        return default_preference
    
        elif key_type == "zset":
            val1 = r1.zrange(key, 0, -1, withscores=True)
            val2 = r2.zrange(key, 0, -1, withscores=True)
            
            if val1 == val2:
                return "equal"
            else:
                # 比较有序集合大小，选择更大的集合
                return "db1" if len(val1) > len(val2) else "db2"
    
        elif key_type == "stream":
            # 获取流的最后一个条目
            val1 = r1.xrevrange(key, count=1)
            val2 = r2.xrevrange(key, count=1)
            
            if val1 == val2:
                return "equal"
            else:
                # 比较流的长度
                len1 = r1.xlen(key)
                len2 = r2.xlen(key)
                return "db1" if len1 > len2 else "db2"
    
        elif key_type == "geo":
            # 获取所有地理位置
            val1 = r1.geopos(key, r1.georadius(key, 0, 0, 10000, unit='km', withdist=False, withcoord=False))
            val2 = r2.geopos(key, r2.georadius(key, 0, 0, 10000, unit='km', withdist=False, withcoord=False))
            
            if val1 == val2:
                return "equal"
            else:
                # 比较地理位置数量
                return "db1" if len(val1) > len(val2) else "db2"
    
        logger.warning(f"未处理的数据类型: {key_type}，默认选择db2")
    except Exception as e:
        logger.error(f"处理键 {key} 时出错: {e}")
    
    # 默认选择配置的数据库
    return default_preference

def display_results(results):
    """
    显示比较结果
    
    :param results: 比较结果字典
    """
    logger.info("===== Redis数据比较结果 ====")
    logger.info(f"仅存在于DB1的键: {len(results['only_in_db1'])}")
    logger.info(f"仅存在于DB2的键: {len(results['only_in_db2'])}")
    logger.info(f"需要合并的键: {len(results['conflict_resolved'])}")
    logger.info(f"值相同的键: {len(results['equal_values'])}")
    logger.info(f"类型不匹配的键: {len(results['type_mismatch'])}")
    
    # 显示类型不匹配的键
    if results['type_mismatch']:
        logger.info("\n类型不匹配的键示例:")
        for key, type1, type2 in results['type_mismatch'][:5]:
            logger.info(f"  键: {key} → DB1类型: {type1}, DB2类型: {type2}")
    
    # 显示需要合并的键
    if results['conflict_resolved']:
        logger.info("\n需要合并的键示例:")
        for key, source in results['conflict_resolved'][:10]:
            logger.info(f"  键: {key} ← 选择 {source}")

def merge_redis_data(redis1_config, redis2_config, compare_results, merge_direction="db1_to_db2"):
    """
    将比较结果合并到目标数据库
    
    :param redis1_config: 第一个Redis配置
    :param redis2_config: 第二个Redis配置
    :param compare_results: 比较结果字典
    :param merge_direction: 合并方向 ("db1_to_db2" 或 "db2_to_db1")
    :return: 合并结果字典
    """
    try:
        # 连接两个Redis实例
        r1 = connect_redis_with_retry(redis1_config)
        r2 = connect_redis_with_retry(redis2_config)
        
        # 根据合并方向确定源数据库和目标数据库
        if merge_direction == "db1_to_db2":
            source_db = r1
            target_db = r2
            source_name = "DB1"
            target_name = "DB2"
            # 需要合并的键：仅存在于DB1的键 + 冲突解决后选择DB1的键
            keys_to_merge = compare_results['only_in_db1'] + [key for key, source in compare_results['conflict_resolved'] if source == "db1"]
        elif merge_direction == "db2_to_db1":
            source_db = r2
            target_db = r1
            source_name = "DB2"
            target_name = "DB1"
            # 需要合并的键：仅存在于DB2的键 + 冲突解决后选择DB2的键
            keys_to_merge = compare_results['only_in_db2'] + [key for key, source in compare_results['conflict_resolved'] if source == "db2"]
        else:
            raise ValueError(f"不支持的合并方向: {merge_direction}")
        
        total_keys = len(keys_to_merge)
        logger.info(f"开始合并 {total_keys} 个键，从 {source_name} 到 {target_name}")
        
        merge_results = {
            'total_keys': total_keys,
            'successfully_merged': 0,
            'merge_failed': 0,
            'failed_keys': {}
        }
        
        if total_keys == 0:
            logger.info("没有需要合并的键")
            return merge_results
        
        processed_count = 0
        start_time = time.time()
        
        for key in keys_to_merge:
            processed_count += 1
            
            try:
                # 获取键的类型
                key_type = get_key_type(source_db, key)
                
                # 根据键类型执行不同的合并操作
                if key_type == "string":
                    value = source_db.get(key)
                    target_db.set(key, value)
                    logger.info(f"合并键: {key}, 类型: string, 由DB1到DB2")
                
                elif key_type == "hash":
                    # 获取所有hash字段
                    hash_data = source_db.hgetall(key)
                    if hash_data:
                        # 合并字段（目标数据库中已存在的字段会被覆盖，新字段会被添加）
                        for subkey, value in hash_data.items():
                            target_db.hset(key, subkey, value)
                            logger.info(f"合并键: {key}, 类型: hash{subkey}, 由DB1到DB2")
                    logger.info(f"完成合并哈希键: {key}, 共{len(hash_data)}个字段")
                
                elif key_type == "list":
                    # 获取所有列表元素
                    list_data = source_db.lrange(key, 0, -1)
                    if list_data:
                        # 追加列表元素到目标数据库（保留原有内容）
                        for i, item in enumerate(list_data):
                            target_db.rpush(key, item)
                            logger.info(f"合并键: {key}, 类型: list[{i}], 由DB1到DB2")
                    logger.info(f"完成合并列表键: {key}, 共{len(list_data)}个元素")
                
                elif key_type == "set":
                    # 获取所有集合元素
                    set_data = source_db.smembers(key)
                    if set_data:
                        # 添加集合元素到目标数据库（利用集合去重特性，保留原有内容）
                        for item in set_data:
                            target_db.sadd(key, item)
                            logger.info(f"合并键: {key}, 类型: set{item}, 由DB1到DB2")
                    logger.info(f"完成合并集合键: {key}, 共{len(set_data)}个元素")
                
                elif key_type == "zset":
                    # 获取所有有序集合元素和分数
                    zset_data = source_db.zrange(key, 0, -1, withscores=True)
                    if zset_data:
                        # 先删除目标数据库中的有序集合，然后重新添加所有元素
                        target_db.delete(key)
                        for item, score in zset_data:
                            target_db.zadd(key, {item: score})
                            logger.info(f"合并键: {key}, 类型: zset{item}, 分数: {score}, 由DB1到DB2")
                    logger.info(f"完成合并有序集合键: {key}, 共{len(zset_data)}个元素")
                
                elif key_type == "stream":
                    # 获取流的所有条目
                    stream_data = []
                    entries = source_db.xrange(key, count=1000)
                    while entries:
                        stream_data.extend(entries)
                        last_id = entries[-1][0]
                        entries = source_db.xrange(key, min=last_id, count=1000)
                    
                    if stream_data:
                        # 先删除目标数据库中的流，然后重新添加所有条目
                        target_db.delete(key)
                        for entry_id, entry_data in stream_data:
                            target_db.xadd(key, entry_data, id=entry_id)
                            logger.info(f"合并键: {key}, 类型: stream[{entry_id}], 由DB1到DB2")
                    logger.info(f"完成合并流键: {key}, 共{len(stream_data)}个条目")
                
                elif key_type == "geo":
                    # 获取所有地理位置
                    geo_data = source_db.georadius(key, 0, 0, 10000, unit="km", withdist=True)
                    if geo_data:
                        # 先删除目标数据库中的地理位置集合，然后重新添加所有元素
                        target_db.delete(key)
                        for member, dist in geo_data:
                            # 从源数据库获取精确的经纬度
                            lon_lat = source_db.geopos(key, member)[0]
                            if lon_lat:
                                target_db.geoadd(key, lon_lat, member)
                                logger.info(f"合并键: {key}, 类型: geo[{member}], 由DB1到DB2")
                    logger.info(f"完成合并地理位置键: {key}, 共{len(geo_data)}个成员")
                
                else:
                    logger.warning(f"不支持的数据类型 {key_type}，键: {key}")
                    continue
                
                merge_results['successfully_merged'] += 1
                
                # 定期记录进度
                if processed_count % 100 == 0 or processed_count == total_keys:
                    elapsed_time = time.time() - start_time
                    logger.info(f"已合并 {processed_count}/{total_keys} 个键 ({(processed_count/total_keys)*100:.1f}%)，耗时 {elapsed_time:.2f} 秒")
                    
            except Exception as e:
                logger.error(f"合并键 {key} 失败: {e}")
                logger.debug(traceback.format_exc())
                merge_results['merge_failed'] += 1
                merge_results['failed_keys'][key] = str(e)
        
        elapsed_time = time.time() - start_time
        logger.info(f"合并完成！总耗时 {elapsed_time:.2f} 秒")
        logger.info(f"成功合并: {merge_results['successfully_merged']} 个键")
        logger.info(f"合并失败: {merge_results['merge_failed']} 个键")
        
        if merge_results['failed_keys']:
            # 只显示前10个失败的键
            first_10_failed = list(merge_results['failed_keys'].keys())[:10]
            logger.warning(f"合并失败的键: {first_10_failed}...")
        
        return merge_results
        
    except Exception as e:
        logger.error(f"合并过程中发生错误: {e}")
        logger.debug(traceback.format_exc())
        raise

def merge_to_new_db(redis1_config, redis2_config, redis3_config, batch_size=100, default_preference="db2"):
    """
    将DB1和DB2的数据合并到新的DB3中，选择每个键的最新版本
    
    :param redis1_config: 第一个Redis配置（DB1）
    :param redis2_config: 第二个Redis配置（DB2）
    :param redis3_config: 第三个Redis配置（DB3，目标数据库）
    :param batch_size: 批量处理大小
    :param default_preference: 当无法确定最新版本时，默认优先选择的数据库 ("db1" 或 "db2")
    :return: 合并结果字典
    """
    try:
        # 连接三个Redis实例
        r1 = connect_redis_with_retry(redis1_config)
        r2 = connect_redis_with_retry(redis2_config)
        r3 = connect_redis_with_retry(redis3_config)
        
        logger.info(f"开始将DB1和DB2的数据合并到DB3")
        
        # 获取DB1和DB2的所有键
        keys1 = scan_keys(r1, count=batch_size)
        keys2 = scan_keys(r2, count=batch_size)
        all_keys = keys1.union(keys2)
        total_keys = len(all_keys)
        logger.info(f"总共有 {total_keys} 个不同的键需要处理")
        
        merge_results = {
            'total_keys': total_keys,
            'successfully_merged': 0,
            'merge_failed': 0,
            'failed_keys': {}
        }
        
        if total_keys == 0:
            logger.info("没有需要合并的键")
            return merge_results
        
        processed_count = 0
        start_time = time.time()
        
        for key in all_keys:
            processed_count += 1
            
            try:
                # 检查键是否存在于两个数据库中
                exists_in_r1 = r1.exists(key)
                exists_in_r2 = r2.exists(key)
                
                if not exists_in_r1 and exists_in_r2:
                    # 仅存在于DB2
                    key_type = get_key_type(r2, key)
                    logger.debug(f"键 {key} 仅存在于DB2")
                    
                    if key_type == "string":
                        value = r2.get(key)
                        r3.set(key, value)
                        logger.debug(f"合并字符串键: {key}")
                    elif key_type == "hash":
                        hash_data = r2.hgetall(key)
                        if hash_data:
                            r3.hset(key, mapping=hash_data)
                        logger.debug(f"合并哈希键: {key}")
                    elif key_type == "list":
                        list_data = r2.lrange(key, 0, -1)
                        if list_data:
                            for item in list_data:
                                r3.rpush(key, item)
                        logger.debug(f"合并列表键: {key}")
                    elif key_type == "set":
                        set_data = r2.smembers(key)
                        if set_data:
                            for item in set_data:
                                r3.sadd(key, item)
                        logger.debug(f"合并集合键: {key}")
                    elif key_type == "zset":
                        zset_data = r2.zrange(key, 0, -1, withscores=True)
                        if zset_data:
                            for item, score in zset_data:
                                r3.zadd(key, {item: score})
                        logger.debug(f"合并有序集合键: {key}")
                    elif key_type == "stream":
                        stream_data = []
                        entries = r2.xrange(key, count=1000)
                        while entries:
                            stream_data.extend(entries)
                            last_id = entries[-1][0]
                            entries = r2.xrange(key, min=last_id, count=1000)
                        if stream_data:
                            for entry_id, entry_data in stream_data:
                                r3.xadd(key, entry_data, id=entry_id)
                        logger.debug(f"合并流键: {key}")
                    elif key_type == "geo":
                        geo_data = r2.georadius(key, 0, 0, 10000, unit="km", withdist=True)
                        if geo_data:
                            for member, dist in geo_data:
                                lon_lat = r2.geopos(key, member)[0]
                                if lon_lat:
                                    r3.geoadd(key, lon_lat, member)
                        logger.debug(f"合并地理位置键: {key}")
                    else:
                        logger.warning(f"不支持的数据类型 {key_type}，键: {key}")
                        continue
                
                elif exists_in_r1 and not exists_in_r2:
                    # 仅存在于DB1
                    key_type = get_key_type(r1, key)
                    logger.debug(f"键 {key} 仅存在于DB1")
                    
                    if key_type == "string":
                        value = r1.get(key)
                        r3.set(key, value)
                        logger.debug(f"合并字符串键: {key}")
                    elif key_type == "hash":
                        hash_data = r1.hgetall(key)
                        if hash_data:
                            r3.hset(key, mapping=hash_data)
                        logger.debug(f"合并哈希键: {key}")
                    elif key_type == "list":
                        list_data = r1.lrange(key, 0, -1)
                        if list_data:
                            for item in list_data:
                                r3.rpush(key, item)
                        logger.debug(f"合并列表键: {key}")
                    elif key_type == "set":
                        set_data = r1.smembers(key)
                        if set_data:
                            for item in set_data:
                                r3.sadd(key, item)
                        logger.debug(f"合并集合键: {key}")
                    elif key_type == "zset":
                        zset_data = r1.zrange(key, 0, -1, withscores=True)
                        if zset_data:
                            for item, score in zset_data:
                                r3.zadd(key, {item: score})
                        logger.debug(f"合并有序集合键: {key}")
                    elif key_type == "stream":
                        stream_data = []
                        entries = r1.xrange(key, count=1000)
                        while entries:
                            stream_data.extend(entries)
                            last_id = entries[-1][0]
                            entries = r1.xrange(key, min=last_id, count=1000)
                        if stream_data:
                            for entry_id, entry_data in stream_data:
                                r3.xadd(key, entry_data, id=entry_id)
                        logger.debug(f"合并流键: {key}")
                    elif key_type == "geo":
                        geo_data = r1.georadius(key, 0, 0, 10000, unit="km", withdist=True)
                        if geo_data:
                            for member, dist in geo_data:
                                lon_lat = r1.geopos(key, member)[0]
                                if lon_lat:
                                    r3.geoadd(key, lon_lat, member)
                        logger.debug(f"合并地理位置键: {key}")
                    else:
                        logger.warning(f"不支持的数据类型 {key_type}，键: {key}")
                        continue
                
                else:
                    # 键存在于两个数据库中
                    type1 = get_key_type(r1, key)
                    type2 = get_key_type(r2, key)
                    
                    if type1 != type2:
                        # 类型不匹配，使用默认偏好
                        source_db = r1 if default_preference == "db1" else r2
                        key_type = get_key_type(source_db, key)
                        logger.warning(f"键 {key} 在两个数据库中的类型不同: DB1={type1}, DB2={type2}，默认选择{default_preference}")
                        
                        if key_type == "string":
                            value = source_db.get(key)
                            r3.set(key, value)
                            logger.debug(f"合并字符串键: {key}")
                        elif key_type == "hash":
                            hash_data = source_db.hgetall(key)
                            if hash_data:
                                r3.hset(key, mapping=hash_data)
                            logger.debug(f"合并哈希键: {key}")
                        elif key_type == "list":
                            list_data = source_db.lrange(key, 0, -1)
                            if list_data:
                                for item in list_data:
                                    r3.rpush(key, item)
                            logger.debug(f"合并列表键: {key}")
                        elif key_type == "set":
                            set_data = source_db.smembers(key)
                            if set_data:
                                for item in set_data:
                                    r3.sadd(key, item)
                            logger.debug(f"合并集合键: {key}")
                        elif key_type == "zset":
                            zset_data = source_db.zrange(key, 0, -1, withscores=True)
                            if zset_data:
                                for item, score in zset_data:
                                    r3.zadd(key, {item: score})
                            logger.debug(f"合并有序集合键: {key}")
                        elif key_type == "stream":
                            stream_data = []
                            entries = source_db.xrange(key, count=1000)
                            while entries:
                                stream_data.extend(entries)
                                last_id = entries[-1][0]
                                entries = source_db.xrange(key, min=last_id, count=1000)
                            if stream_data:
                                for entry_id, entry_data in stream_data:
                                    r3.xadd(key, entry_data, id=entry_id)
                            logger.debug(f"合并流键: {key}")
                        elif key_type == "geo":
                            geo_data = source_db.georadius(key, 0, 0, 10000, unit="km", withdist=True)
                            if geo_data:
                                for member, dist in geo_data:
                                    lon_lat = source_db.geopos(key, member)[0]
                                    if lon_lat:
                                        r3.geoadd(key, lon_lat, member)
                            logger.debug(f"合并地理位置键: {key}")
                        else:
                            logger.warning(f"不支持的数据类型 {key_type}，键: {key}")
                            continue
                    
                    else:
                        # 类型相同，根据类型执行不同的合并操作
                        if type1 == "string":
                            # 字符串类型，选择最新值
                            latest_source = select_latest_by_type(r1, r2, key, type1, default_preference)
                            source_db = r1 if latest_source == "db1" else r2
                            value = source_db.get(key)
                            r3.set(key, value)
                            logger.debug(f"合并字符串键: {key} ← 选择 {latest_source}")
                        elif type1 == "hash":
                            # 哈希类型，合并字段（保留两个数据库的所有字段，默认偏好的数据库中的字段覆盖冲突的字段）
                            hash_data1 = r1.hgetall(key)
                            hash_data2 = r2.hgetall(key)
                            
                            # 先合并非默认偏好的数据库字段，再合并默认偏好的数据库字段（默认偏好的字段会覆盖冲突的字段）
                            merged_hash = {}
                            if default_preference == "db1":
                                merged_hash.update(hash_data2)
                                merged_hash.update(hash_data1)
                            else:
                                merged_hash.update(hash_data1)
                                merged_hash.update(hash_data2)
                            
                            if merged_hash:
                                r3.hset(key, mapping=merged_hash)
                            logger.debug(f"合并哈希键: {key} ← 合并两个数据库的字段")
                        elif type1 == "list":
                            # 列表类型，合并两个列表的所有元素（DB1的元素在前，DB2的元素在后）
                            list_data1 = r1.lrange(key, 0, -1)
                            list_data2 = r2.lrange(key, 0, -1)
                            
                            if list_data1:
                                for item in list_data1:
                                    r3.rpush(key, item)
                            if list_data2:
                                for item in list_data2:
                                    r3.rpush(key, item)
                            logger.debug(f"合并列表键: {key} ← 合并两个数据库的列表元素")
                        elif type1 == "set":
                            # 集合类型，合并两个集合的元素（利用集合的去重特性）
                            set_data1 = r1.smembers(key)
                            set_data2 = r2.smembers(key)
                            
                            if set_data1:
                                for item in set_data1:
                                    r3.sadd(key, item)
                            if set_data2:
                                for item in set_data2:
                                    r3.sadd(key, item)
                            logger.debug(f"合并集合键: {key} ← 合并两个数据库的集合元素")
                        elif type1 == "zset":
                            # 有序集合类型，选择最新值
                            latest_source = select_latest_by_type(r1, r2, key, type1, default_preference)
                            source_db = r1 if latest_source == "db1" else r2
                            zset_data = source_db.zrange(key, 0, -1, withscores=True)
                            if zset_data:
                                for item, score in zset_data:
                                    r3.zadd(key, {item: score})
                            logger.debug(f"合并有序集合键: {key} ← 选择 {latest_source}")
                        elif type1 == "stream":
                            # 流类型，选择最新值
                            latest_source = select_latest_by_type(r1, r2, key, type1, default_preference)
                            source_db = r1 if latest_source == "db1" else r2
                            stream_data = []
                            entries = source_db.xrange(key, count=1000)
                            while entries:
                                stream_data.extend(entries)
                                last_id = entries[-1][0]
                                entries = source_db.xrange(key, min=last_id, count=1000)
                            if stream_data:
                                for entry_id, entry_data in stream_data:
                                    r3.xadd(key, entry_data, id=entry_id)
                            logger.debug(f"合并流键: {key} ← 选择 {latest_source}")
                        elif type1 == "geo":
                            # 地理位置类型，合并两个地理位置集合的元素
                            geo_data1 = r1.georadius(key, 0, 0, 10000, unit="km", withdist=True)
                            geo_data2 = r2.georadius(key, 0, 0, 10000, unit="km", withdist=True)
                            
                            # 合并DB1的地理位置
                            if geo_data1:
                                for member, dist in geo_data1:
                                    lon_lat = r1.geopos(key, member)[0]
                                    if lon_lat:
                                        r3.geoadd(key, lon_lat, member)
                            # 合并DB2的地理位置
                            if geo_data2:
                                for member, dist in geo_data2:
                                    lon_lat = r2.geopos(key, member)[0]
                                    if lon_lat:
                                        r3.geoadd(key, lon_lat, member)
                            logger.debug(f"合并地理位置键: {key} ← 合并两个数据库的地理位置")
                        else:
                            logger.warning(f"不支持的数据类型 {type1}，键: {key}")
                            continue
                
                merge_results['successfully_merged'] += 1
                
                # 定期记录进度
                if processed_count % 100 == 0 or processed_count == total_keys:
                    elapsed_time = time.time() - start_time
                    logger.info(f"已合并 {processed_count}/{total_keys} 个键 ({(processed_count/total_keys)*100:.1f}%)，耗时 {elapsed_time:.2f} 秒")
                    
            except Exception as e:
                logger.error(f"合并键 {key} 失败: {e}")
                logger.debug(traceback.format_exc())
                merge_results['merge_failed'] += 1
                merge_results['failed_keys'][key] = str(e)
        
        elapsed_time = time.time() - start_time
        logger.info(f"合并完成！总耗时 {elapsed_time:.2f} 秒")
        logger.info(f"成功合并: {merge_results['successfully_merged']} 个键")
        logger.info(f"合并失败: {merge_results['merge_failed']} 个键")
        
        if merge_results['failed_keys']:
            # 只显示前10个失败的键
            first_10_failed = list(merge_results['failed_keys'].keys())[:10]
            logger.warning(f"合并失败的键: {first_10_failed}...")
        
        return merge_results
        
    except Exception as e:
        logger.error(f"合并过程中发生错误: {e}")
        logger.debug(traceback.format_exc())
        raise


# 使用示例
if __name__ == "__main__":
    """
    Redis数据合并工具使用示例
    
    功能说明：
    1. 对比两个Redis数据库中的所有键
    2. 支持多种数据类型：string、hash、list、set、zset、stream、geo
    3. 智能选择最新值的策略：
       - 首先检查值或字段中是否包含时间戳
       - 然后检查对象的最后访问时间
       - 最后根据默认偏好选择
    4. 提供详细的比较结果统计
    5. 支持将选择的最新值合并到目标数据库
       - 支持从DB1到DB2或从DB2到DB1的合并方向
       - 支持所有受支持的数据类型
       - 提供合并进度显示和结果统计
       - 处理合并过程中的错误和异常
    
    配置说明：
    - host: Redis服务器地址
    - port: Redis服务器端口
    - db: 数据库索引
    - decode_responses: 是否自动解码响应
    - socket_connect_timeout: 连接超时时间
    - socket_timeout: 操作超时时间
    """
    try:
        # 示例1: 比较本地两个不同数据库
        logger.info("=== 示例1: 比较本地两个不同数据库 ===")
        redis1_config = {
            'host': 'localhost',
            'port': 6379,
            'db': 11,
            'decode_responses': True,  # 自动解码
            'socket_connect_timeout': 5,
            'socket_timeout': 5
        }
        
        redis2_config = {
            'host': 'localhost',
            'port': 6379,
            'db': 12,
            'decode_responses': True,
            'socket_connect_timeout': 5,
            'socket_timeout': 5
        }
        
        results = compare_and_select_latest(redis1_config, redis2_config, batch_size=1000)
        display_results(results)
        
        # 示例1.1: 将DB1的最新数据合并到DB2
        logger.info("\n=== 示例1.1: 将DB1的最新数据合并到DB2 ===")
        merge_results = merge_redis_data(redis1_config, redis2_config, results, merge_direction="db1_to_db2")
        logger.info(f"合并完成！成功合并 {merge_results['successfully_merged']} 个键")
        
        # 示例1.2: 将DB2的最新数据合并到DB1
        logger.info("\n=== 示例1.2: 将DB2的最新数据合并到DB1 ===")
        merge_results = merge_redis_data(redis1_config, redis2_config, results, merge_direction="db2_to_db1")
        logger.info(f"合并完成！成功合并 {merge_results['successfully_merged']} 个键")
        
        # 示例1.3: 将DB1和DB2的数据合并到新的DB3中
        logger.info("\n=== 示例1.3: 将DB1和DB2的数据合并到新的DB3 ===")
        redis3_config = {
            'host': 'localhost',
            'port': 6379,
            'db': 6,  # 使用新的数据库
            'decode_responses': True,
            'socket_connect_timeout': 5,
            'socket_timeout': 5
        }
        
        # 在DB1和DB2中创建一些测试数据
        r1 = connect_redis_with_retry(redis1_config)
        r2 = connect_redis_with_retry(redis2_config)
        
        # 清理DB3
        r3 = connect_redis_with_retry(redis3_config)
        keys = r3.keys()
        if keys:
            r3.delete(*keys)
            logger.info("已清理DB3的现有数据")
        
        # 调用合并函数
        merge_results = merge_to_new_db(redis1_config, redis2_config, redis3_config, default_preference="db2")
        logger.info(f"合并完成！成功合并 {merge_results['successfully_merged']} 个键")
        
        # 验证结果
        logger.info("\n=== 验证DB3的合并结果 ===")
        total_keys = r3.keys()
        logger.info(f"DB3中总共有 {len(total_keys)} 个键")

        
    except Exception as e:
        logger.error(f"执行过程中发生错误: {e}")
        logger.debug(traceback.format_exc())
        exit(1)