#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MongoDB分片状态分析工具
分析分片集合的状态并提供优化建议
"""

import json
import argparse
import sys
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure


class ShardingAnalyzer:
    """分片状态分析器"""
    
    def __init__(self, config_file=None, connection_string=None, database=None):
        """初始化分析器"""
        self.client = None
        self.database_name = database
        
        if config_file:
            self.load_config(config_file)
        elif connection_string:
            self.connect_with_string(connection_string, database)
        else:
            raise ValueError("必须提供配置文件或连接字符串")
    
    def load_config(self, config_file):
        """从配置文件加载连接配置"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            mongodb_config = config.get('mongodb', {})
            
            # 构建连接字符串
            if mongodb_config.get('connection_type') == 'sharded_cluster':
                hosts = mongodb_config.get('hosts', [])
                host_strings = [f"{host['host']}:{host['port']}" for host in hosts]
                connection_string = f"mongodb://{','.join(host_strings)}/"
            else:
                host = mongodb_config.get('host', 'localhost')
                port = mongodb_config.get('port', 27017)
                connection_string = f"mongodb://{host}:{port}/"
            
            # 添加认证信息
            username = mongodb_config.get('username')
            password = mongodb_config.get('password')
            if username and password:
                connection_string = connection_string.replace('mongodb://', 
                    f'mongodb://{username}:{password}@')
            
            self.database_name = mongodb_config.get('database', 'admin')
            self.connect_with_string(connection_string, self.database_name)
            
        except Exception as e:
            print(f"加载配置文件失败: {e}")
            sys.exit(1)
    
    def connect_with_string(self, connection_string, database=None):
        """使用连接字符串连接MongoDB"""
        try:
            self.client = MongoClient(connection_string)
            self.client.admin.command('ping')
            if database:
                self.database_name = database
            print("✓ MongoDB连接成功")
        except ConnectionFailure as e:
            print(f"✗ MongoDB连接失败: {e}")
            sys.exit(1)
    
    def analyze_collection_sharding(self, collection_name):
        """分析集合的分片状态"""
        try:
            config_db = self.client.config
            ns = f"{self.database_name}.{collection_name}"
            
            # 检查集合是否分片
            collection_info = config_db.collections.find_one({'_id': ns, 'dropped': {'$ne': True}})
            
            if not collection_info:
                print(f"✗ 集合 '{collection_name}' 未分片或不存在")
                return
            
            print(f"\n=== 集合 '{collection_name}' 分片分析 ===")
            
            # 获取分片键信息
            shard_key = collection_info.get('key', {})
            print(f"分片键: {shard_key}")
            
            # 获取chunk信息
            chunks = list(config_db.chunks.find({'ns': ns}))
            chunk_count = len(chunks)
            
            # 统计各分片的chunk数量
            chunk_count_by_shard = {}
            for chunk in chunks:
                shard = chunk['shard']
                chunk_count_by_shard[shard] = chunk_count_by_shard.get(shard, 0) + 1
            
            print(f"总Chunk数量: {chunk_count}")
            
            # 获取chunk大小配置
            try:
                chunk_size_doc = config_db.settings.find_one({'_id': 'chunksize'})
                chunk_size_mb = chunk_size_doc.get('value', 64) if chunk_size_doc else 64
                print(f"Chunk大小配置: {chunk_size_mb} MB")
            except:
                chunk_size_mb = 64
                print(f"Chunk大小配置: {chunk_size_mb} MB (默认)")
            
            # 获取集合数据统计
            db = self.client[self.database_name]
            coll_stats = db.command('collStats', collection_name)
            
            data_size_mb = coll_stats.get('size', 0) / (1024 * 1024)
            doc_count = coll_stats.get('count', 0)
            
            print(f"数据大小: {data_size_mb:.2f} MB")
            print(f"文档数量: {doc_count}")
            
            # 分析分片状态
            self._analyze_sharding_status(chunk_count, data_size_mb, doc_count, chunk_size_mb, chunk_count_by_shard)
            
            # 提供建议
            self._provide_recommendations(chunk_count, data_size_mb, doc_count, chunk_size_mb)
            
        except OperationFailure as e:
            print(f"分析失败: {e}")
    
    def _analyze_sharding_status(self, chunk_count, data_size_mb, doc_count, chunk_size_mb, chunk_count_by_shard):
        """分析分片状态"""
        print("\n--- 分片状态分析 ---")
        
        # 检查chunk数量
        if chunk_count == 0:
            print("❌ 没有chunk - 集合可能未正确分片")
        elif chunk_count == 1:
            print("⚠️  只有1个chunk - 数据未分布在多个分片上")
            
            # 检查数据量是否足够
            if data_size_mb < chunk_size_mb * 0.8:  # 80%阈值
                print(f"  📊 数据量({data_size_mb:.2f}MB)小于chunk大小({chunk_size_mb}MB)的80%")
                print(f"  💡 需要更多数据才能触发自动分片")
            else:
                print(f"  ✅ 数据量足够，但平衡器可能未运行")
        else:
            print(f"✅ 有{chunk_count}个chunk分布在多个分片上")
            
            # 检查分布是否均衡
            if len(chunk_count_by_shard) > 1:
                max_chunks = max(chunk_count_by_shard.values())
                min_chunks = min(chunk_count_by_shard.values())
                imbalance_ratio = max_chunks / min_chunks if min_chunks > 0 else float('inf')
                
                if imbalance_ratio > 1.5:
                    print(f"⚠️  chunk分布不均衡，最大/最小比例: {imbalance_ratio:.2f}")
                else:
                    print("✅ chunk分布均衡")
        
        # 检查分片数量
        shard_count = len(chunk_count_by_shard)
        if shard_count == 1:
            print("⚠️  数据只分布在1个分片上")
        else:
            print(f"✅ 数据分布在{shard_count}个分片上")
    
    def _provide_recommendations(self, chunk_count, data_size_mb, doc_count, chunk_size_mb):
        """提供优化建议"""
        print("\n--- 优化建议 ---")
        
        if chunk_count <= 1:
            if data_size_mb < chunk_size_mb * 0.5:
                print("1. 📈 增加数据量")
                print(f"   - 当前数据量: {data_size_mb:.2f}MB")
                print(f"   - 建议达到: {chunk_size_mb}MB以上")
                print(f"   - 需要增加约 {chunk_size_mb - data_size_mb:.2f}MB数据")
            
            print("2. ⚙️  手动分片")
            print("   - 使用 sh.splitAt() 或 sh.splitFind() 手动分割chunk")
            print("   - 示例: sh.splitAt('xsdk_v2_test.ug_user', {_id: ObjectId()})")
            
            if data_size_mb < 10:  # 数据量很小
                print("3. 🔧 临时调整chunk大小")
                print("   - 降低chunk大小阈值以触发分片")
                print("   - db.settings.update({_id:'chunksize'}, {$set:{value:1}}, {upsert:true})")
        
        # 通用建议
        print("4. 🔄 检查平衡器状态")
        print("   - 确保平衡器正在运行: sh.getBalancerState()")
        print("   - 检查是否有活跃迁移: sh.isBalancerRunning()")
        
        print("5. 📊 监控分片性能")
        print("   - 使用 db.ug_user.getShardDistribution() 监控分布")
        print("   - 定期检查分片状态")
    
    def check_balancer_status(self):
        """检查平衡器状态"""
        try:
            result = self.client.admin.command('balancerStatus')
            
            print("\n=== 平衡器状态 ===")
            print(f"平衡器运行中: {'是' if result.get('mode', '') == 'full' else '否'}")
            print(f"平衡器启用: {'是' if result.get('inBalancerRound', False) else '否'}")
            
            # 检查活跃迁移
            locks = list(self.client.config.locks.find({'state': 2}))
            print(f"活跃迁移数量: {len(locks)}")
            
            if len(locks) > 0:
                print("当前有迁移任务正在进行")
            else:
                print("当前没有活跃的迁移任务")
            
            return result
        except OperationFailure as e:
            print(f"检查平衡器状态失败: {e}")
            return None


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='MongoDB分片状态分析工具')
    parser.add_argument('--config', '-c', help='配置文件路径', default='config.json')
    parser.add_argument('--connection-string', '-s', help='MongoDB连接字符串')
    parser.add_argument('--database', '-d', help='数据库名称')
    parser.add_argument('--collection', '-coll', required=True, help='要分析的集合名称')
    
    args = parser.parse_args()
    
    try:
        # 创建分析器实例
        if args.connection_string:
            analyzer = ShardingAnalyzer(connection_string=args.connection_string, 
                                      database=args.database)
        else:
            analyzer = ShardingAnalyzer(config_file=args.config, 
                                      database=args.database)
        
        # 分析集合分片状态
        analyzer.analyze_collection_sharding(args.collection)
        
        # 检查平衡器状态
        analyzer.check_balancer_status()
        
    except Exception as e:
        print(f"程序执行出错: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()