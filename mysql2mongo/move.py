#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL到MongoDB数据迁移工具
支持扩展多种MySQL表结构的数据迁移
"""

import os
import sys
import time
import logging
import argparse
import json
from datetime import datetime, date, time as datetime_time
from typing import Dict, List, Any, Optional
import urllib.parse
import pymysql
import pymongo
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from pymongo import ReplaceOne


class MigrationLogger:
    """迁移日志记录器"""
    
    def __init__(self, log_file: str = "migration.log"):
        """
        初始化日志记录器
        
        Args:
            log_file: 日志文件路径
        """
        self.log_file = log_file
        self.setup_logging()
    
    def setup_logging(self):
        """配置日志系统"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def info(self, message: str):
        """记录信息日志"""
        self.logger.info(message)
    
    def error(self, message: str):
        """记录错误日志"""
        self.logger.error(message)
    
    def warning(self, message: str):
        """记录警告日志"""
        self.logger.warning(message)


class MySQLConnector:
    """MySQL数据库连接器"""
    
    def __init__(self, config: Dict[str, Any], logger: MigrationLogger):
        """
        初始化MySQL连接器
        
        Args:
            config: MySQL配置信息
            logger: 日志记录器
        """
        self.config = config
        self.logger = logger
        self.connection = None
    
    def connect(self) -> bool:
        """连接到MySQL数据库"""
        try:
            self.connection = pymysql.connect(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database'],
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            self.logger.info(f"成功连接到MySQL数据库: {self.config['host']}:{self.config['port']}")
            return True
        except Exception as e:
            self.logger.error(f"连接MySQL数据库失败: {str(e)}")
            return False
    
    def disconnect(self):
        """断开MySQL连接"""
        if self.connection:
            self.connection.close()
            self.logger.info("已断开MySQL连接")
    
    def get_table_count(self, table_name: str) -> int:
        """获取表记录总数"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                result = cursor.fetchone()
                return result['count']
        except Exception as e:
            self.logger.error(f"获取表 {table_name} 记录数失败: {str(e)}")
            return 0
    
    def fetch_data(self, table_name: str, batch_size: int = 1000, offset: int = 0) -> List[Dict]:
        """
        分批获取表数据
        
        Args:
            table_name: 表名
            batch_size: 批次大小
            offset: 偏移量
            
        Returns:
            数据列表
        """
        try:
            with self.connection.cursor() as cursor:
                query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
                cursor.execute(query)
                return cursor.fetchall()
        except Exception as e:
            self.logger.error(f"获取表 {table_name} 数据失败: {str(e)}")
            return []
    
    def fetch_data_by_ids(self, table_name: str, ids: List[str]) -> List[Dict]:
        """
        根据ID列表获取表数据
        
        Args:
            table_name: 表名
            ids: ID列表
            
        Returns:
            数据列表
        """
        if not ids:
            return []
            
        try:
            with self.connection.cursor() as cursor:
                # 构建IN查询，限制每个查询的ID数量以避免SQL过长
                max_ids_per_query = 1000
                all_results = []
                
                for i in range(0, len(ids), max_ids_per_query):
                    batch_ids = ids[i:i + max_ids_per_query]
                    id_list = ",".join([f"'{id_}'" for id_ in batch_ids])
                    query = f"SELECT * FROM {table_name} WHERE id IN ({id_list})"
                    cursor.execute(query)
                    batch_results = cursor.fetchall()
                    all_results.extend(batch_results)
                
                return all_results
        except Exception as e:
            self.logger.error(f"根据ID列表获取表 {table_name} 数据失败: {str(e)}")
            return []


class MongoDBConnector:
    """MongoDB数据库连接器"""
    
    def __init__(self, config: Dict[str, Any], logger: MigrationLogger):
        """
        初始化MongoDB连接器
        
        Args:
            config: MongoDB配置信息
            logger: 日志记录器
        """
        self.config = config
        self.logger = logger
        self.client = None
        self.database = None
    
    def connect(self) -> bool:
        """连接到MongoDB数据库"""
        try:
            # 转义用户名和密码中的特殊字符
            username = urllib.parse.quote_plus(self.config.get('username', ''))
            # 如果密码已经包含URL编码字符，避免双重编码
            raw_password = self.config.get('password', '')
            if '%' in raw_password:
                # 密码可能已经编码过，直接使用
                password = raw_password
            else:
                # 对密码进行URL编码
                password = urllib.parse.quote_plus(raw_password)
            
            # 构建MongoDB连接字符串
            connection_string = self._build_connection_string(username, password)
            
            # 构建连接选项
            connection_options = self._build_connection_options()
            
            self.logger.info(f"尝试连接MongoDB: {connection_string.replace(password, '***')}")
            
            # 创建MongoDB客户端
            self.client = MongoClient(connection_string, **connection_options)
            self.database = self.client[self.config['database']]
            
            # 测试连接（使用更可靠的测试方法）
            self.client.admin.command('ping')
            
            # 验证数据库访问权限
            collections = self.database.list_collection_names()
            self.logger.info(f"成功连接到MongoDB数据库，数据库: {self.config['database']}")
            self.logger.info(f"可访问的集合数量: {len(collections)}")
            
            return True
        except Exception as e:
            self.logger.error(f"连接MongoDB数据库失败: {str(e)}")
            # 提供更详细的错误信息
            if "Authentication failed" in str(e):
                self.logger.error("认证失败，请检查用户名、密码和认证数据库配置")
            elif "timed out" in str(e).lower():
                self.logger.error("连接超时，请检查网络连接和服务器状态")
            elif "No primary" in str(e):
                self.logger.error("无法找到主节点，请检查分片集群状态")
            return False
    
    def _build_connection_string(self, username: str, password: str) -> str:
        """
        构建MongoDB连接字符串
        
        Args:
            username: 转义后的用户名
            password: 转义后的密码
            
        Returns:
            连接字符串
        """
        connection_type = self.config.get('connection_type', 'single')
        
        if connection_type == 'sharded_cluster':
            # 分片集群连接
            return self._build_sharded_cluster_connection_string(username, password)
        elif connection_type == 'replica_set':
            # 副本集连接
            return self._build_replica_set_connection_string(username, password)
        else:
            # 单节点连接（兼容旧配置）
            return self._build_single_node_connection_string(username, password)
    
    def _build_sharded_cluster_connection_string(self, username: str, password: str) -> str:
        """
        构建分片集群连接字符串
        
        Args:
            username: 转义后的用户名
            password: 转义后的密码
            
        Returns:
            分片集群连接字符串
        """
        # 构建主机列表
        hosts = []
        for host_config in self.config.get('hosts', []):
            host = host_config.get('host', '')
            port = host_config.get('port', 27017)
            hosts.append(f"{host}:{port}")
        
        if not hosts:
            # 回退到单节点配置
            return self._build_single_node_connection_string(username, password)
        
        # 构建连接字符串
        if username and password:
            connection_string = f"mongodb://{username}:{password}@{','.join(hosts)}/"
        else:
            connection_string = f"mongodb://{','.join(hosts)}/"
        
        # 添加认证数据库
        auth_source = self.config.get('auth_source', 'admin')
        connection_string += f"?authSource={auth_source}"
        
        # 添加其他连接选项
        connection_options = self.config.get('connection_options', {})
        options_list = []
        for key, value in connection_options.items():
            if key not in ['serverSelectionTimeoutMS', 'connectTimeoutMS', 'socketTimeoutMS']:
                # 将布尔值转换为小写字符串
                if isinstance(value, bool):
                    value = str(value).lower()
                options_list.append(f"{key}={value}")
        
        if options_list:
            if '?' in connection_string:
                connection_string += "&" + "&".join(options_list)
            else:
                connection_string += "?" + "&".join(options_list)
        
        return connection_string
    
    def _build_replica_set_connection_string(self, username: str, password: str) -> str:
        """
        构建副本集连接字符串
        
        Args:
            username: 转义后的用户名
            password: 转义后的密码
            
        Returns:
            副本集连接字符串
        """
        # 构建主机列表
        hosts = []
        for host_config in self.config.get('hosts', []):
            host = host_config.get('host', '')
            port = host_config.get('port', 27017)
            hosts.append(f"{host}:{port}")
        
        if not hosts:
            # 回退到单节点配置
            return self._build_single_node_connection_string(username, password)
        
        # 构建连接字符串
        if username and password:
            connection_string = f"mongodb://{username}:{password}@{','.join(hosts)}/"
        else:
            connection_string = f"mongodb://{','.join(hosts)}/"
        
        # 添加副本集名称
        replica_set = self.config.get('replica_set', '')
        if replica_set:
            connection_string += f"?replicaSet={replica_set}"
        
        # 添加认证数据库
        auth_source = self.config.get('auth_source', 'admin')
        if '?' in connection_string:
            connection_string += f"&authSource={auth_source}"
        else:
            connection_string += f"?authSource={auth_source}"
        
        return connection_string
    
    def _build_single_node_connection_string(self, username: str, password: str) -> str:
        """
        构建单节点连接字符串（兼容旧配置）
        
        Args:
            username: 转义后的用户名
            password: 转义后的密码
            
        Returns:
            单节点连接字符串
        """
        # 兼容旧配置格式
        host = self.config.get('host', 'localhost')
        port = self.config.get('port', 27017)
        
        if username and password:
            connection_string = f"mongodb://{username}:{password}@{host}:{port}/"
            
            # 添加认证数据库参数（通常为admin）
            auth_source = self.config.get('auth_source', 'admin')
            connection_string += f"?authSource={auth_source}"
        else:
            connection_string = f"mongodb://{host}:{port}/"
        
        return connection_string
    
    def _build_connection_options(self) -> Dict[str, Any]:
        """
        构建连接选项
        
        Returns:
            连接选项字典
        """
        options = {}
        connection_options = self.config.get('connection_options', {})
        
        # 设置默认超时选项
        options['serverSelectionTimeoutMS'] = connection_options.get('serverSelectionTimeoutMS', 10000)
        options['connectTimeoutMS'] = connection_options.get('connectTimeoutMS', 10000)
        options['socketTimeoutMS'] = connection_options.get('socketTimeoutMS', 45000)
        
        # 添加其他连接选项
        for key, value in connection_options.items():
            if key not in ['serverSelectionTimeoutMS', 'connectTimeoutMS', 'socketTimeoutMS']:
                options[key] = value
        
        return options
    
    def disconnect(self):
        """断开MongoDB连接"""
        if self.client:
            self.client.close()
            self.logger.info("已断开MongoDB连接")
    
    def insert_batch(self, collection_name: str, data: List[Dict], max_retries: int = 3) -> bool:
        """
        批量插入数据到MongoDB
        
        Args:
            collection_name: 集合名称
            data: 数据列表
            max_retries: 最大重试次数
            
        Returns:
            是否成功
        """
        for attempt in range(max_retries + 1):
            try:
                if not data:
                    return True
                
                collection = self.database[collection_name]
                
                # 使用ReplaceOne + upsert=True替代insert_many
                # 这样可以避免重复键错误，同时确保数据完整性
                bulk_operations = []
                for doc in data:
                    # 确保每个文档都有_id字段
                    if '_id' not in doc:
                        self.logger.warning(f"文档缺少_id字段，跳过: {doc}")
                        continue
                    
                    # 构建upsert操作
                    filter_doc = {'_id': doc['_id']}
                    bulk_operations.append(
                        ReplaceOne(filter_doc, doc, upsert=True)
                    )
                
                if not bulk_operations:
                    self.logger.warning(f"没有有效的批量操作，跳过插入")
                    return True
                
                # 执行批量操作
                result = collection.bulk_write(bulk_operations, ordered=False)
                
                # 统计处理结果
                inserted_count = result.upserted_count
                modified_count = result.modified_count
                matched_count = result.matched_count
                
                self.logger.info(f"成功处理 {len(bulk_operations)} 条数据到集合 {collection_name}")
                self.logger.info(f"插入新记录: {inserted_count}, 更新现有记录: {modified_count}, 匹配记录: {matched_count}")
                
                return True
                
            except BulkWriteError as e:
                # 处理批量写入错误
                failed_count = len(e.details['writeErrors'])
                success_count = len(data) - failed_count
                
                if success_count > 0:
                    self.logger.warning(f"批量操作部分失败，成功处理 {success_count} 条数据，失败 {failed_count} 条")
                    # 输出前几个错误详情
                    for i, error in enumerate(e.details['writeErrors'][:3]):
                        self.logger.warning(f"错误 {i+1}: {error.get('errmsg', str(error))}")
                    return True
                else:
                    self.logger.error(f"批量操作完全失败，所有 {len(data)} 条数据处理失败")
                    # 输出错误详情
                    for i, error in enumerate(e.details['writeErrors'][:5]):
                        self.logger.error(f"错误详情 {i+1}: {error.get('errmsg', str(error))}")
                    return False
                    
            except Exception as e:
                if attempt < max_retries:
                    wait_time = 2 ** attempt  # 指数退避
                    self.logger.warning(f"数据处理失败，第 {attempt + 1} 次重试，等待 {wait_time} 秒: {str(e)}")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"处理数据到集合 {collection_name} 失败，已达最大重试次数: {str(e)}")
                    return False
        return False
    
    def get_collection_count(self, collection_name: str) -> int:
        """
        获取集合文档数量
        
        Args:
            collection_name: 集合名称
            
        Returns:
            文档数量
        """
        try:
            collection = self.database[collection_name]
            return collection.count_documents({})
        except Exception as e:
            self.logger.error(f"获取集合 {collection_name} 文档数量失败: {str(e)}")
            return 0
    
    def verify_data_integrity(self, collection_name: str, expected_count: int) -> bool:
        """
        验证数据完整性
        
        Args:
            collection_name: 集合名称
            expected_count: 期望的文档数量
            
        Returns:
            是否完整
        """
        actual_count = self.get_collection_count(collection_name)
        if actual_count == expected_count:
            self.logger.info(f"数据完整性验证通过: {collection_name} ({actual_count}/{expected_count})")
            return True
        else:
            self.logger.error(f"数据完整性验证失败: {collection_name} ({actual_count}/{expected_count})")
            return False


class DataTransformer:
    """数据转换器"""
    
    def __init__(self, logger: MigrationLogger):
        """
        初始化数据转换器
        
        Args:
            logger: 日志记录器
        """
        self.logger = logger
        
    def transform_ug_order(self, mysql_data: List[Dict]) -> List[Dict]:
        """
        转换ug_order表数据
        
        Args:
            mysql_data: MySQL原始数据
            
        Returns:
            转换后的MongoDB文档
        """
        transformed_data = []
        
        for record in mysql_data:
            # 转换数据类型和字段名
            doc = {
                '_id': str(record.get('id')),  # MongoDB使用_id作为主键
                'uid': str(record.get('uid')),
                'appID': record.get('appID', 0),
                'channelID': record.get('channelID', 0),
                'payType': record.get('payType', 0),
                'orderType': record.get('orderType', 0),
                'status': record.get('status', 0),
                'notifyStatus': record.get('notifyStatus', 0),
                'notifyContent': record.get('notifyContent'),
                'refundStatus': record.get('refundStatus', 0),
                'refundType': record.get('refundType', 0),
                'refundDesc': record.get('refundDesc'),
                'cpOrderID': record.get('cpOrderID'),
                'extra': record.get('extra'),
                'payNotifyUrl': record.get('payNotifyUrl'),
                'price': record.get('price', 0),
                'realPrice': record.get('realPrice', 0),
                'coinPrice': record.get('coinPrice', 0),
                'costCoinNum': record.get('costCoinNum', 0),
                'currency': record.get('currency'),
                'platformOrderID': record.get('platformOrderID'),
                'platformUserID': record.get('platformUserID'),
                'platformPrice': record.get('platformPrice'),
                'productID': record.get('productID'),
                'productName': record.get('productName'),
                'productDesc': record.get('productDesc'),
                'roleID': record.get('roleID'),
                'roleName': record.get('roleName'),
                'roleLevel': record.get('roleLevel'),
                'serverID': record.get('serverID'),
                'serverName': record.get('serverName'),
                'vip': record.get('vip'),
                'ip': record.get('ip'),
                'deviceID': record.get('deviceID'),
                'userCreateTime': record.get('userCreateTime'),
                'createTime': record.get('createTime'),
                'finishTime': record.get('finishTime'),
                'updateTime': record.get('updateTime'),
                'migrationTime': datetime.now(),  # 添加迁移时间戳
                'source': 'mysql_ug_order'  # 标识数据来源
            }
            
            # 清理空值
            doc = {k: v for k, v in doc.items() if v is not None}
            transformed_data.append(doc)
        
        return transformed_data
    
    def transform_ug_user(self, mysql_data: List[Dict]) -> List[Dict]:
        """
        转换ug_user表数据
        
        Args:
            mysql_data: MySQL原始数据
            
        Returns:
            转换后的MongoDB文档
        """
        transformed_data = []
        
        for record in mysql_data:
            # 转换数据类型和字段名
            doc = {
                '_id': str(record.get('id')),  # MongoDB使用_id作为主键
                'name': record.get('name', ''),
                'phoneNum': record.get('phoneNum', ''),
                'loginName': record.get('loginName', ''),
                'password': record.get('password', ''),
                'lastLoginTime': record.get('lastLoginTime'),
                'createTime': record.get('createTime'),
                'updateTime': record.get('updateTime'),
                'appID': record.get('appID', 0),
                'accountType': record.get('accountType', 0),
                'deviceID': record.get('deviceID'),
                'ip': record.get('ip'),
                'channelID': record.get('channelID', 0),
                'realName': record.get('realName'),
                'idCard': record.get('idCard'),
                'coinNum': record.get('coinNum', 0),
                'migrationTime': datetime.now(),  # 添加迁移时间戳
                'source': 'mysql_ug_user'  # 标识数据来源
            }
            
            # 清理空值
            doc = {k: v for k, v in doc.items() if v is not None}
            transformed_data.append(doc)
        
        return transformed_data

    def transform_ug_id_card_config(self, mysql_data: List[Dict]) -> List[Dict]:
        """
        转换ug_id_card_config表数据
        
        Args:
            mysql_data: MySQL原始数据
            
        Returns:
            转换后的MongoDB文档
        """
        transformed_data = []
        
        for record in mysql_data:
            # 转换数据类型和字段名
            # 注意：ug_id_card_config表使用appID作为主键，而不是id字段
            doc = {
                '_id': str(record.get('appID')),  # 使用appID作为MongoDB主键
                'appID': record.get('appID', 0),
                'rnAppID': record.get('rnAppID', ''),
                'rnSecretKey': record.get('rnSecretKey', ''),
                'rnBizID': record.get('rnBizID', ''),
                'rnState': record.get('rnState', 0),
                'rnVerifyType': record.get('rnVerifyType', 0),
                'migrationTime': datetime.now(),  # 添加迁移时间戳
                'source': 'mysql'  # 标识数据来源
            }
            
            # 清理空值
            doc = {k: v for k, v in doc.items() if v is not None}
            transformed_data.append(doc)
        
        return transformed_data
    
    def transform_data(self, table_name: str, mysql_data: List[Dict]) -> List[Dict]:
        """
        根据表名选择对应的转换方法
        
        Args:
            table_name: 表名
            mysql_data: MySQL原始数据
            
        Returns:
            转换后的MongoDB文档
        """
        transform_methods = {
            'ug_order': self.transform_ug_order,
            'ug_user': self.transform_ug_user,
            'ug_id_card_config': self.transform_ug_id_card_config
        }
        
        if table_name in transform_methods:
            return transform_methods[table_name](mysql_data)
        else:
            # 默认转换方法
            self.logger.warning(f"未找到表 {table_name} 的专用转换方法，使用默认转换")
            return self.default_transform(mysql_data)
    
    def default_transform(self, mysql_data: List[Dict]) -> List[Dict]:
        """
        默认数据转换方法
        
        Args:
            mysql_data: MySQL原始数据
            
        Returns:
            转换后的MongoDB文档
        """
        transformed_data = []
        
        for record in mysql_data:
            doc = {}
            for key, value in record.items():
                # 转换主键
                if key == 'id':
                    doc['_id'] = str(value)
                else:
                    # 处理Python date类型，转换为datetime
                    if isinstance(value, date) and not isinstance(value, datetime):
                        # 将date转换为datetime，时间设为00:00:00
                        doc[key] = datetime.combine(value, datetime_time.min)
                    # 处理其他不可序列化的Python类型
                    elif isinstance(value, (set, frozenset)):
                        # 将set转换为list
                        doc[key] = list(value)
                    elif isinstance(value, (bytes, bytearray)):
                        # 将bytes转换为base64字符串
                        import base64
                        doc[key] = base64.b64encode(value).decode('utf-8')
                    elif hasattr(value, '__dict__'):
                        # 处理自定义对象，转换为字典
                        doc[key] = value.__dict__
                    else:
                        doc[key] = value
            
            # 添加迁移元数据
            doc['migrationTime'] = datetime.now()
            doc['source'] = 'mysql'
            
            transformed_data.append(doc)
        
        return transformed_data


class MigrationManager:
    """迁移管理器"""
    
    def __init__(self, config_file: str = "config.json"):
        """
        初始化迁移管理器
        
        Args:
            config_file: 配置文件路径
        """
        self.config_file = config_file
        self.config = self.load_config()
        self.logger = MigrationLogger(self.config.get('log_file', 'migration.log'))
        self.mysql_connector = MySQLConnector(self.config['mysql'], self.logger)
        self.mongo_connector = MongoDBConnector(self.config['mongodb'], self.logger)
        self.transformer = DataTransformer(self.logger)
        
        # 进度文件路径
        self.progress_file = self.config.get('progress_file', 'migration_progress.json')
        self.progress = self.load_progress()
        
    def load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            # 创建默认配置
            default_config = {
                "mysql": {
                    "host": "localhost",
                    "port": 3306,
                    "user": "root",
                    "password": "password",
                    "database": "test_db"
                },
                "mongodb": {
                    "host": "localhost",
                    "port": 27017,
                    "database": "test_db"
                },
                "migration": {
                    "batch_size": 1000,
                    "tables": ["ug_order"]
                },
                "log_file": "migration.log"
            }
            
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"已创建默认配置文件: {self.config_file}")
            return default_config
    
    def load_progress(self) -> Dict[str, Any]:
        """加载迁移进度"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            self.logger.warning(f"加载进度文件失败: {str(e)}")
        
        return {}
    
    def save_progress(self, table_name: str, offset: int, migrated_count: int):
        """保存迁移进度"""
        try:
            self.progress[table_name] = {
                'offset': offset,
                'migrated_count': migrated_count,
                'last_update': datetime.now().isoformat()
            }
            
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.warning(f"保存进度文件失败: {str(e)}")
    
    def clear_progress(self, table_name: str = None):
        """清除迁移进度"""
        try:
            if table_name:
                if table_name in self.progress:
                    del self.progress[table_name]
            else:
                self.progress = {}
            
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.warning(f"清除进度文件失败: {str(e)}")
    
    def migrate_table(self, table_name: str, resume: bool = True) -> bool:
        """
        迁移单个表
        
        Args:
            table_name: 表名
            resume: 是否从断点继续迁移
            
        Returns:
            是否成功
        """
        self.logger.info(f"开始迁移表: {table_name}")
        
        # 获取总记录数
        total_count = self.mysql_connector.get_table_count(table_name)
        if total_count == 0:
            self.logger.warning(f"表 {table_name} 为空，跳过迁移")
            return True
        
        self.logger.info(f"表 {table_name} 共有 {total_count} 条记录")
        
        batch_size = self.config['migration'].get('batch_size', 1000)
        migrated_count = 0
        start_time = time.time()
        
        # 检查是否有断点进度
        start_offset = 0
        if resume and table_name in self.progress:
            progress_info = self.progress[table_name]
            start_offset = progress_info.get('offset', 0)
            migrated_count = progress_info.get('migrated_count', 0)
            self.logger.info(f"从断点继续迁移: 偏移量 {start_offset}, 已迁移 {migrated_count} 条")
        
        # 记录已迁移的批次，用于回滚
        migrated_batches = []
        
        # 分批迁移数据
        for offset in range(start_offset, total_count, batch_size):
            # 从MySQL获取数据
            mysql_data = self.mysql_connector.fetch_data(table_name, batch_size, offset)
            if not mysql_data:
                self.logger.warning(f"从表 {table_name} 获取数据为空，偏移量: {offset}")
                continue
            
            # 转换数据格式
            mongo_data = self.transformer.transform_data(table_name, mysql_data)
            
            # 插入到MongoDB
            collection_name = table_name  # 使用表名作为集合名
            success = self.mongo_connector.insert_batch(collection_name, mongo_data)
            if success:
                migrated_count += len(mongo_data)
                # 记录成功迁移的批次
                migrated_batches.append({
                    'offset': offset,
                    'count': len(mongo_data),
                    'batch_size': batch_size
                })
                
                # 保存进度
                self.save_progress(table_name, offset + batch_size, migrated_count)
                
                progress = (migrated_count / total_count) * 100
                elapsed_time = time.time() - start_time
                estimated_total_time = (elapsed_time / migrated_count) * total_count if migrated_count > 0 else 0
                remaining_time = estimated_total_time - elapsed_time
                
                self.logger.info(
                    f"进度: {migrated_count}/{total_count} ({progress:.1f}%) - "
                    f"已用时间: {elapsed_time:.1f}s - 预计剩余时间: {remaining_time:.1f}s"
                )
            else:
                self.logger.error(f"迁移表 {table_name} 失败，偏移量: {offset}")
                # 执行回滚操作
                self.rollback_table(table_name, migrated_batches)
                # 清除进度
                self.clear_progress(table_name)
                return False
        
        # 验证数据完整性（增加重试机制）
        verification_attempts = 3
        verification_passed = False
        
        for attempt in range(verification_attempts):
            if self.mongo_connector.verify_data_integrity(table_name, total_count):
                verification_passed = True
                break
            elif attempt < verification_attempts - 1:
                wait_time = 5 * (attempt + 1)  # 递增等待时间：5秒、10秒
                self.logger.warning(f"数据完整性验证失败，第{attempt+1}次重试，等待 {wait_time} 秒...")
                time.sleep(wait_time)
            else:
                self.logger.error(f"表 {table_name} 数据完整性验证失败，已达最大重试次数")
        
        if not verification_passed:
            self.rollback_table(table_name, migrated_batches)
            # 清除进度
            self.clear_progress(table_name)
            return False
        
        # 迁移完成，清除进度
        self.clear_progress(table_name)
        
        total_time = time.time() - start_time
        self.logger.info(f"表 {table_name} 迁移完成，共迁移 {migrated_count} 条记录，耗时 {total_time:.1f} 秒")
        return True
    
    def rollback_table(self, table_name: str, migrated_batches: List[Dict]) -> bool:
        """
        回滚表迁移操作
        
        Args:
            table_name: 表名
            migrated_batches: 已迁移的批次信息
            
        Returns:
            是否成功
        """
        # self.logger.info(f"开始回滚表 {table_name} 的迁移操作")
        
        # try:
        #     collection = self.mongo_connector.database[table_name]
        #     deleted_count = 0
            
        #     # 如果无法精确识别，则删除整个集合（安全起见）
        #     if migrated_batches:
        #         # 尝试通过批次信息删除特定数据
        #         # 这里简化处理，实际应该根据具体业务逻辑实现
        #         result = collection.delete_many({'source': 'mysql'})
        #         deleted_count = result.deleted_count
        #     else:
        #         # 没有批次信息，删除整个集合
        #         result = collection.delete_many({})
        #         deleted_count = result.deleted_count
            
        #     self.logger.info(f"已回滚表 {table_name}，删除 {deleted_count} 条记录")
        #     return True
        # except Exception as e:
        #     self.logger.error(f"回滚表 {table_name} 失败: {str(e)}")
        #     return False
    
    def migrate_all_tables(self) -> bool:
        """迁移所有配置的表"""
        self.logger.info("开始数据迁移任务")
        
        # 连接数据库
        if not self.mysql_connector.connect():
            return False
        
        if not self.mongo_connector.connect():
            self.mysql_connector.disconnect()
            return False
        
        try:
            # 迁移每个表
            tables = self.config['migration'].get('tables', [])
            success_count = 0
            
            for table_name in tables:
                if self.migrate_table(table_name):
                    success_count += 1
                else:
                    self.logger.error(f"表 {table_name} 迁移失败")
            
            self.logger.info(f"迁移任务完成，成功迁移 {success_count}/{len(tables)} 个表")
            return success_count == len(tables)
            
        finally:
            # 断开数据库连接
            self.mysql_connector.disconnect()
            self.mongo_connector.disconnect()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='MySQL到MongoDB数据迁移工具')
    parser.add_argument('--config', '-c', default='config.json', help='配置文件路径')
    parser.add_argument('--table', '-t', help='指定要迁移的表名（覆盖配置文件）')
    parser.add_argument('--batch-size', '-b', type=int, help='批次大小（覆盖配置文件）')
    
    args = parser.parse_args()
    
    # 创建迁移管理器
    manager = MigrationManager(args.config)
    
    # 覆盖配置（如果提供了命令行参数）
    if args.table:
        manager.config['migration']['tables'] = [args.table]
    
    if args.batch_size:
        manager.config['migration']['batch_size'] = args.batch_size
    
    # 执行迁移
    success = manager.migrate_all_tables()
    
    if success:
        print("数据迁移完成！")
        sys.exit(0)
    else:
        print("数据迁移失败，请查看日志文件了解详情")
        sys.exit(1)


if __name__ == "__main__":
    main()