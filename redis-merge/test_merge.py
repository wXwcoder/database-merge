import redis
import time
from redis_compare import merge_to_new_db, connect_redis_with_retry

def setup_test_data():
    """在DB1和DB2中创建测试数据"""
    # 连接到Redis数据库
    config1 = {'host': 'localhost', 'port': 6379, 'db': 4, 'decode_responses': True}
    config2 = {'host': 'localhost', 'port': 6379, 'db': 5, 'decode_responses': True}
    
    r1 = connect_redis_with_retry(config1)
    r2 = connect_redis_with_retry(config2)
    
    # 清理测试数据
    r1.flushdb()
    r2.flushdb()
    
    print("=== 设置测试数据 ===")
    
    # DB1中的数据
    print("\nDB1 数据:")
    
    # String
    r1.set('user:1:name', 'Alice')
    r1.setex('session:123', 3600, 'session_data_123')
    print(f"  String: user:1:name = {r1.get('user:1:name')}")
    print(f"  Expiring String: session:123 = {r1.get('session:123')}")
    
    # Hash
    r1.hset('user:1', 'name', 'Alice')
    r1.hset('user:1', 'email', 'alice@example.com')
    r1.hset('user:1', 'age', 30)
    r1.hset('user:1', '444', '444')
    print(f"  Hash: user:1 = {r1.hgetall('user:1')}")
    
    # List
    r1.rpush('tasks:1', 'task1', 'task2', 'task3')
    print(f"  List: tasks:1 = {r1.lrange('tasks:1', 0, -1)}")
    
    # Set
    r1.sadd('tags:1', 'python', 'redis', 'database')
    # Common set (same key as DB2 but different content)
    r1.sadd('tags:common', 'java', 'spring', 'hibernate')
    print(f"  Set: tags:1 = {r1.smembers('tags:1')}")
    print(f"  Set: tags:common = {r1.smembers('tags:common')}")
    
    # DB2中的数据
    print("\nDB2 数据:")
    
    # String (与DB1相同的键，但值不同)
    r2.set('user:1:name', 'Bob')
    r2.set('user:2:name', 'Charlie')
    print(f"  String: user:1:name = {r2.get('user:1:name')}")
    print(f"  String: user:2:name = {r2.get('user:2:name')}")
    
    # Hash (与DB1相同的键，但字段更多)
    r2.hset('user:1', 'name', 'Bob')
    r2.hset('user:1', 'email', 'bob@example.com')
    r2.hset('user:1', 'age', 25)
    r2.hset('user:1', 'city', 'New York')
    r2.hset('user:1', '555', '555')
    print(f"  Hash: user:1 = {r2.hgetall('user:1')}")
    
    # List (与DB1不同的键)
    r2.rpush('tasks:2', 'taskA', 'taskB')
    r2.rpush('tasks:1', 'taskA', 'taskB')
    print(f"  List: tasks:2 = {r2.lrange('tasks:2', 0, -1)}")
    
    # Set (与DB1相同的键，但元素更多)
    r2.sadd('tags:1', 'python', 'redis', 'database', 'programming')
    # Common set (same key as DB1 but different content)
    r2.sadd('tags:common', 'javascript', 'react', 'vue')
    print(f"  Set: tags:1 = {r2.smembers('tags:1')}")
    print(f"  Set: tags:common = {r2.smembers('tags:common')}")
    
    return config1, config2

def test_merge():
    """测试将DB1和DB2的数据合并到DB3"""
    # 设置测试数据
    config1, config2 = setup_test_data()
    
    # 配置DB3
    config3 = {'host': 'localhost', 'port': 6379, 'db': 6, 'decode_responses': True}
    r3 = connect_redis_with_retry(config3)
    
    # 清理DB3
    r3.flushdb()
    
    print("\n=== 开始合并测试 ===")
    
    # 调用合并函数
    merge_results = merge_to_new_db(config1, config2, config3, default_preference="db2")
    
    print(f"\n=== 合并结果统计 ===")
    print(f"成功合并: {merge_results['successfully_merged']} 个键")
    print(f"合并失败: {merge_results['merge_failed']} 个键")
    
    print("\n=== 验证DB3中的数据 ===")
    
    # 获取DB3中的所有键
    keys = r3.keys()
    print(f"DB3 中的键总数: {len(keys)}")
    print(f"DB3 中的键: {keys}")
    
    print("\n详细数据:")
    
    # 检查每个键
    for key in sorted(keys):
        key_type = r3.type(key).decode('utf-8') if hasattr(r3.type(key), 'decode') else r3.type(key)
        print(f"\n  {key} ({key_type}):")
        
        if key_type == 'string':
            value = r3.get(key)
            print(f"    值: {value}")
        elif key_type == 'hash':
            value = r3.hgetall(key)
            print(f"    字段: {value}")
        elif key_type == 'list':
            value = r3.lrange(key, 0, -1)
            print(f"    元素: {value}")
        elif key_type == 'set':
            value = r3.smembers(key)
            print(f"    元素: {value}")
        elif key_type == 'zset':
            value = r3.zrange(key, 0, -1, withscores=True)
            print(f"    元素: {value}")
    
    # 验证预期结果
    print("\n=== 验证预期结果 ===")
    
    # 1. user:1:name 应该来自DB2 (因为default_preference是db2)
    assert r3.get('user:1:name') == 'Bob', f"user:1:name 应该是 'Bob'，但实际是 {r3.get('user:1:name')}"
    print("✓ user:1:name 正确合并自 DB2")
    
    # 2. user:2:name 应该来自DB2
    assert r3.get('user:2:name') == 'Charlie', f"user:2:name 应该是 'Charlie'，但实际是 {r3.get('user:2:name')}"
    print("✓ user:2:name 正确合并自 DB2")
    
    # 3. session:123 应该来自DB1 (DB2中不存在)
    assert r3.get('session:123') == 'session_data_123', f"session:123 应该是 'session_data_123'，但实际是 {r3.get('session:123')}"
    print("✓ session:123 正确合并自 DB1")
    
    # 4. user:1 hash 应该来自DB2
    user1 = r3.hgetall('user:1')
    assert user1['name'] == 'Bob', f"user:1 name 应该是 'Bob'，但实际是 {user1['name']}"
    assert user1['email'] == 'bob@example.com', f"user:1 email 应该是 'bob@example.com'，但实际是 {user1['email']}"
    assert user1['age'] == '25', f"user:1 age 应该是 '25'，但实际是 {user1['age']}"
    assert user1['city'] == 'New York', f"user:1 city 应该是 'New York'，但实际是 {user1['city']}"
    print("✓ user:1 hash 正确合并自 DB2")
    
    # 5. tasks:1 应该合并了DB1和DB2的元素
    tasks1 = r3.lrange('tasks:1', 0, -1)
    assert tasks1 == ['task1', 'task2', 'task3', 'taskA', 'taskB'], f"tasks:1 应该是 ['task1', 'task2', 'task3', 'taskA', 'taskB']，但实际是 {tasks1}"
    print("✓ tasks:1 list 正确合并了 DB1 和 DB2 的元素")
    
    # 6. tasks:2 应该来自DB2
    tasks2 = r3.lrange('tasks:2', 0, -1)
    assert tasks2 == ['taskA', 'taskB'], f"tasks:2 应该是 ['taskA', 'taskB']，但实际是 {tasks2}"
    print("✓ tasks:2 list 正确合并自 DB2")
    
    # 7. tags:1 应该来自DB2
    tags1 = r3.smembers('tags:1')
    assert tags1 == {'python', 'redis', 'database', 'programming'}, f"tags:1 应该是 {'python', 'redis', 'database', 'programming'}，但实际是 {tags1}"
    print("✓ tags:1 set 正确合并自 DB2")
    
    # 8. tags:common 应该合并了DB1和DB2的元素
    tags_common = r3.smembers('tags:common')
    expected_tags = {'java', 'spring', 'hibernate', 'javascript', 'react', 'vue'}
    assert tags_common == expected_tags, f"tags:common 应该是 {expected_tags}，但实际是 {tags_common}"
    print("✓ tags:common set 正确合并了 DB1 和 DB2 的元素")
    
    print("\n✅ 所有测试通过！")
    
    # 清理测试数据
    #r1.flushdb()
    #r2.flushdb()
    #r3.flushdb()
    print("\n测试数据已清理")

if __name__ == "__main__":
    try:
        setup_test_data()
        test_merge()
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()