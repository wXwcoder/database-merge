#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MongoDB分片检查工具
用于检查MongoDB分片集群的状态、分片分布情况和平衡器状态
"""

import json
import argparse
import sys
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure


class MongoDBShardChecker:
    """MongoDB分片检查器"""
    
    def __init__(self, config_file=None, connection_string=None, database=None):
        """
        初始化MongoDB分片检查器
        
        Args:
            config_file: 配置文件路径
            connection_string: MongoDB连接字符串
            database: 数据库名称
        """
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
                # 单节点连接
                host = mongodb_config.get('host', 'localhost')
                port = mongodb_config.get('port', 27017)
                connection_string = f"mongodb://{host}:{port}/"
            
            # 添加认证信息
            username = mongodb_config.get('username')
            password = mongodb_config.get('password')
            if username and password:
                connection_string = connection_string.replace('mongodb://', 
                    f'mongodb://{username}:{password}@')
            
            # 添加连接选项
            options = mongodb_config.get('connection_options', {})
            if options:
                option_strings = [f"{k}={v}" for k, v in options.items()]
                connection_string += '?' + '&'.join(option_strings)
            
            self.database_name = mongodb_config.get('database', 'admin')
            self.connect_with_string(connection_string, self.database_name)
            
        except Exception as e:
            print(f"加载配置文件失败: {e}")
            sys.exit(1)
    
    def connect_with_string(self, connection_string, database=None):
        """使用连接字符串连接MongoDB"""
        try:
            self.client = MongoClient(connection_string)
            # 测试连接
            self.client.admin.command('ping')
            if database:
                self.database_name = database
            print("✓ MongoDB连接成功")
        except ConnectionFailure as e:
            print(f"✗ MongoDB连接失败: {e}")
            sys.exit(1)
    
    def check_balancer_state(self):
        """检查平衡器状态"""
        try:
            result = self.client.admin.command('balancerStatus')
            print("\n=== 平衡器状态 ===")
            print(f"平衡器运行中: {'是' if result.get('mode', '') == 'full' else '否'}")
            print(f"平衡器启用: {'是' if result.get('inBalancerRound', False) else '否'}")
            
            # 检查是否有正在进行的迁移
            locks = self.client.config.locks.find({'state': 2})  # state=2表示活跃
            active_migrations = list(locks)
            print(f"活跃迁移数量: {len(active_migrations)}")
            
            return result
        except OperationFailure as e:
            print(f"✗ 检查平衡器状态失败: {e}")
            return None
    
    def check_shard_distribution(self, collection_name=None):
        """检查分片分布情况"""
        try:
            config_db = self.client.config
            
            if collection_name:
                # 检查特定集合的分片分布
                ns = f"{self.database_name}.{collection_name}"
                
                # 检查集合是否分片
                collection_info = config_db.collections.find_one({'_id': ns, 'dropped': {'$ne': True}})
                
                if not collection_info:
                    print(f"集合 '{collection_name}' 未分片或不存在")
                    return None
                
                # 获取chunk分布
                chunks = list(config_db.chunks.find({'ns': ns}))
                
                if not chunks:
                    print(f"集合 '{collection_name}' 没有chunk信息")
                    return None
                
                # 统计各分片的chunk数量
                chunk_count_by_shard = {}
                for chunk in chunks:
                    shard = chunk['shard']
                    chunk_count_by_shard[shard] = chunk_count_by_shard.get(shard, 0) + 1
                
                print(f"\n=== 集合 '{collection_name}' 分片分布 ===")
                print(f"总Chunk数量: {len(chunks)}")
                
                for shard, count in chunk_count_by_shard.items():
                    percentage = (count / len(chunks)) * 100 if len(chunks) > 0 else 0
                    print(f"  {shard}: {count} 个chunks ({percentage:.1f}%)")
                
                return chunk_count_by_shard
            else:
                # 检查所有分片集合
                collections = list(config_db.collections.find({'dropped': {'$ne': True}}))
                
                print("\n=== 所有分片集合的分片分布 ===")
                
                for collection in collections:
                    ns = collection['_id']
                    db_name, coll_name = ns.split('.', 1)
                    
                    if db_name == self.database_name:
                        chunks = list(config_db.chunks.find({'ns': ns}))
                        
                        if chunks:
                            chunk_count_by_shard = {}
                            for chunk in chunks:
                                shard = chunk['shard']
                                chunk_count_by_shard[shard] = chunk_count_by_shard.get(shard, 0) + 1
                            
                            print(f"\n集合: {coll_name}")
                            print(f"  总Chunk数量: {len(chunks)}")
                            
                            for shard, count in chunk_count_by_shard.items():
                                percentage = (count / len(chunks)) * 100
                                print(f"  {shard}: {count} 个chunks ({percentage:.1f}%)")
                        else:
                            print(f"\n集合: {coll_name}")
                            print("  无chunk信息")
                
                return None
        except OperationFailure as e:
            print(f"✗ 检查分片分布失败: {e}")
            return None
    
    def check_shard_status(self):
        """检查分片状态"""
        try:
            # 检查是否支持分片
            is_master = self.client.admin.command('isMaster')
            
            print("\n=== 分片状态 ===")
            print(f"集群类型: {'分片集群' if is_master.get('msg') == 'isdbgrid' else '副本集' if is_master.get('setName') else '单节点'}")
            
            if is_master.get('msg') == 'isdbgrid':
                # 这是分片集群
                print("✓ 当前连接的是MongoDB分片集群")
                
                # 获取分片列表
                shards = self.client.config.shards.find()
                print("\n分片列表:")
                for shard in shards:
                    shard_name = shard['_id']
                    shard_host = shard['host']
                    state = shard.get('state', 1)  # 1=正常
                    print(f"  {shard_name}: {shard_host} (状态: {'正常' if state == 1 else '异常'})")
                
                # 检查配置服务器
                config_servers = list(self.client.config.shards.find({'host': {'$regex': 'config'}}))
                config_count = len(config_servers)
                print(f"配置服务器数量: {config_count}")
                
                # 检查mongos路由器
                mongos_info = self.client.admin.command('hostInfo')
                print(f"当前连接节点: {mongos_info.get('system', {}).get('hostname', '未知')}")
                
                return shards
            else:
                print("⚠ 当前连接的不是分片集群")
                return None
                
        except OperationFailure as e:
            print(f"✗ 检查分片状态失败: {e}")
            return None
    
    def check_chunk_distribution(self, collection_name=None):
        """检查chunk分布情况"""
        try:
            config_db = self.client.config
            
            if collection_name:
                # 检查特定集合的chunk分布
                chunks = config_db.chunks.find({'ns': f"{self.database_name}.{collection_name}"})
                
                print(f"\n=== 集合 '{collection_name}' 的Chunk分布 ===")
                chunk_count_by_shard = {}
                
                for chunk in chunks:
                    shard = chunk['shard']
                    chunk_count_by_shard[shard] = chunk_count_by_shard.get(shard, 0) + 1
                
                total_chunks = sum(chunk_count_by_shard.values())
                print(f"总Chunk数量: {total_chunks}")
                for shard, count in chunk_count_by_shard.items():
                    percentage = (count / total_chunks) * 100 if total_chunks > 0 else 0
                    print(f"  {shard}: {count} 个chunks ({percentage:.1f}%)")
                
                return chunk_count_by_shard
            else:
                # 检查所有分片集合的chunk分布
                collections = config_db.collections.find({'dropped': {'$ne': True}})
                
                print("\n=== 所有分片集合的Chunk分布 ===")
                for collection in collections:
                    ns = collection['_id']
                    db_name, coll_name = ns.split('.', 1)
                    
                    if db_name == self.database_name:
                        chunks = list(config_db.chunks.find({'ns': ns}))
                        chunk_count = len(chunks)
                        
                        if chunk_count > 0:
                            print(f"\n集合: {coll_name}")
                            print(f"  总Chunk数量: {chunk_count}")
                            
                            # 重新查询获取分片分布
                            chunks.rewind()
                            chunk_count_by_shard = {}
                            for chunk in chunks:
                                shard = chunk['shard']
                                chunk_count_by_shard[shard] = chunk_count_by_shard.get(shard, 0) + 1
                            
                            for shard, count in chunk_count_by_shard.items():
                                percentage = (count / chunk_count) * 100
                                print(f"  {shard}: {count} 个chunks ({percentage:.1f}%)")
                
                return None
        except OperationFailure as e:
            print(f"✗ 检查Chunk分布失败: {e}")
            return None
    
    def run_comprehensive_check(self, collection_name=None):
        """运行全面的分片检查"""
        print(f"=== MongoDB分片检查报告 ===")
        print(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"数据库: {self.database_name}")
        
        # 1. 检查平衡器状态
        self.check_balancer_state()
        
        # 2. 检查分片状态
        self.check_shard_status()
        
        # 3. 检查分片分布
        self.check_shard_distribution(collection_name)
        
        # 4. 检查chunk分布
        self.check_chunk_distribution(collection_name)
        
        print("\n=== 检查完成 ===")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='MongoDB分片检查工具')
    parser.add_argument('--config', '-c', help='配置文件路径', default='config.json')
    parser.add_argument('--connection-string', '-s', help='MongoDB连接字符串')
    parser.add_argument('--database', '-d', help='数据库名称')
    parser.add_argument('--collection', '-coll', help='指定集合名称（可选）')
    parser.add_argument('--check-type', '-t', 
                       choices=['all', 'balancer', 'shard-status', 'distribution', 'chunks'],
                       default='all', help='检查类型')
    
    args = parser.parse_args()
    
    try:
        # 创建检查器实例
        if args.connection_string:
            checker = MongoDBShardChecker(connection_string=args.connection_string, 
                                        database=args.database)
        else:
            checker = MongoDBShardChecker(config_file=args.config, 
                                        database=args.database)
        
        # 根据检查类型执行相应的检查
        if args.check_type == 'all':
            checker.run_comprehensive_check(args.collection)
        elif args.check_type == 'balancer':
            checker.check_balancer_state()
        elif args.check_type == 'shard-status':
            checker.check_shard_status()
        elif args.check_type == 'distribution':
            checker.check_shard_distribution(args.collection)
        elif args.check_type == 'chunks':
            checker.check_chunk_distribution(args.collection)
        
    except Exception as e:
        print(f"程序执行出错: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()