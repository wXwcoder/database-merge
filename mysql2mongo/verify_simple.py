#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化版数据迁移验证工具
专注于三个核心验证步骤：
1. 检测迁移的MySQL和MongoDB数据数量是否一致
2. 检测每条数据内容是否一致（忽略迁移元数据）
3. 对迁移后数据不一致的数据进行重新迁移
"""

import json
import sys
from datetime import datetime
from move import MySQLConnector, MongoDBConnector, MigrationLogger


def verify_data_count(mysql_connector, mongo_connector, table_name):
    """
    验证MySQL和MongoDB数据数量一致性
    
    Args:
        mysql_connector: MySQL连接器
        mongo_connector: MongoDB连接器
        table_name: 表名
        
    Returns:
        (是否通过, MySQL记录数, MongoDB记录数, 差异数量)
    """
    mysql_count = mysql_connector.get_table_count(table_name)
    mongo_count = mongo_connector.get_collection_count(table_name)
    
    is_consistent = mysql_count == mongo_count
    difference = abs(mysql_count - mongo_count)
    
    return is_consistent, mysql_count, mongo_count, difference


def verify_data_content(mysql_connector, mongo_connector, table_name, batch_size=1000):
    """
    验证数据内容一致性（忽略迁移元数据）
    
    Args:
        mysql_connector: MySQL连接器
        mongo_connector: MongoDB连接器
        table_name: 表名
        batch_size: 批次处理大小
        
    Returns:
        (是否通过, 验证详情, 不一致记录列表)
    """
    mysql_count = mysql_connector.get_table_count(table_name)
    
    if mysql_count == 0:
        return True, "空表，跳过内容验证", []
    
    print(f"开始验证表 {table_name} 的数据内容，共 {mysql_count:,} 条记录...")
    
    collection = mongo_connector.database[table_name]
    
    # 排除迁移元数据字段
    metadata_fields = ['migrationTime', 'source', '_id']
    
    inconsistent_records = []
    all_passed = True
    processed_count = 0
    
    # 特殊表的主键映射
    primary_key_mapping = {
        'ug_id_card_config': 'appID'  # ug_id_card_config表使用appID作为主键
    }
    
    # 获取当前表的主键字段
    primary_key_field = primary_key_mapping.get(table_name, 'id')
    
    # 分批处理所有记录
    for offset in range(0, mysql_count, batch_size):
        current_batch_size = min(batch_size, mysql_count - offset)
        
        # 从MySQL获取当前批次数据
        mysql_data = mysql_connector.fetch_data(table_name, current_batch_size, offset)
        
        # 获取当前批次的所有ID（根据主键字段）
        mysql_ids = [str(doc.get(primary_key_field)) for doc in mysql_data]
        
        # 从MongoDB批量获取对应数据
        mongo_docs = {}
        cursor = collection.find({'_id': {'$in': mysql_ids}})
        for doc in cursor:
            mongo_docs[doc['_id']] = doc
        
        # 逐条比较数据内容
        for mysql_doc in mysql_data:
            mysql_id = str(mysql_doc.get(primary_key_field))
            mongo_doc = mongo_docs.get(mysql_id)
            
            if not mongo_doc:
                inconsistent_records.append({
                    'mysql_id': mysql_id,
                    'status': 'missing',
                    'error': 'MongoDB中找不到对应记录'
                })
                all_passed = False
                continue
            
            # 过滤掉迁移元数据字段
            mysql_filtered = {k: v for k, v in mysql_doc.items() 
                             if k not in metadata_fields and k != 'id'}
            mongo_filtered = {k: v for k, v in mongo_doc.items() 
                             if k not in metadata_fields}
            
            # 比较数据内容
            if mysql_filtered != mongo_filtered:
                # 找出不一致的字段
                differences = []
                all_keys = set(mysql_filtered.keys()) | set(mongo_filtered.keys())
                
                for key in all_keys:
                    mysql_val = mysql_filtered.get(key)
                    mongo_val = mongo_filtered.get(key)
                    
                    if mysql_val != mongo_val:
                        differences.append({
                            'field': key,
                            'mysql_value': mysql_val,
                            'mongo_value': mongo_val
                        })
                
                inconsistent_records.append({
                    'mysql_id': mysql_id,
                    'status': 'inconsistent',
                    'differences': differences
                })
                all_passed = False
        
        processed_count += len(mysql_data)
        print(f"  已处理 {processed_count:,}/{mysql_count:,} 条记录...")
    
    if all_passed:
        return True, f"数据内容验证通过: {mysql_count}条记录全部一致", []
    else:
        missing_count = sum(1 for r in inconsistent_records if r['status'] == 'missing')
        inconsistent_count = sum(1 for r in inconsistent_records if r['status'] == 'inconsistent')
        return False, f"数据内容验证失败: {missing_count}条缺失, {inconsistent_count}条不一致", inconsistent_records


def repair_inconsistent_data(mysql_connector, mongo_connector, table_name, inconsistent_records):
    """
    修复不一致的数据
    
    Args:
        mysql_connector: MySQL连接器
        mongo_connector: MongoDB连接器
        table_name: 表名
        inconsistent_records: 不一致记录列表
        
    Returns:
        (修复成功数量, 修复失败数量)
    """
    if not inconsistent_records:
        return 0, 0
    
    print(f"开始修复表 {table_name} 的不一致数据，共 {len(inconsistent_records)} 条记录...")
    
    collection = mongo_connector.database[table_name]
    repaired_count = 0
    failed_count = 0
    
    # 特殊表的主键映射
    primary_key_mapping = {
        'ug_id_card_config': 'appID'  # ug_id_card_config表使用appID作为主键
    }
    
    # 获取当前表的主键字段
    primary_key_field = primary_key_mapping.get(table_name, 'id')
    
    # 按类型分组处理
    missing_records = [r for r in inconsistent_records if r['status'] == 'missing']
    inconsistent_data = [r for r in inconsistent_records if r['status'] == 'inconsistent']
    
    # 修复缺失记录
    if missing_records:
        print(f"  修复缺失记录: {len(missing_records)} 条")
        missing_ids = [r['mysql_id'] for r in missing_records]
        
        # 从MySQL获取缺失数据
        mysql_data = mysql_connector.fetch_data_by_ids(table_name, missing_ids)
        
        for mysql_doc in mysql_data:
            try:
                # 转换为MongoDB格式
                mongo_doc = {}
                for key, value in mysql_doc.items():
                    if key == primary_key_field:
                        mongo_doc['_id'] = str(value)
                    else:
                        mongo_doc[key] = value
                
                # 添加迁移元数据
                mongo_doc['source'] = 'mysql'
                mongo_doc['migrationTime'] = datetime.now()
                
                # 插入到MongoDB
                collection.insert_one(mongo_doc)
                repaired_count += 1
            except Exception as e:
                print(f"    修复失败 (ID: {mysql_doc.get(primary_key_field)}): {e}")
                failed_count += 1
    
    # 修复不一致记录
    if inconsistent_data:
        print(f"  修复不一致记录: {len(inconsistent_data)} 条")
        inconsistent_ids = [r['mysql_id'] for r in inconsistent_data]
        
        # 从MySQL获取不一致数据
        mysql_data = mysql_connector.fetch_data_by_ids(table_name, inconsistent_ids)
        
        for mysql_doc in mysql_data:
            try:
                # 转换为MongoDB格式
                mongo_doc = {}
                for key, value in mysql_doc.items():
                    if key == primary_key_field:
                        mongo_doc['_id'] = str(value)
                    else:
                        mongo_doc[key] = value
                
                # 更新迁移元数据
                mongo_doc['source'] = 'mysql'
                mongo_doc['migrationTime'] = datetime.now()
                
                # 替换MongoDB中的记录
                collection.replace_one({'_id': mongo_doc['_id']}, mongo_doc)
                repaired_count += 1
            except Exception as e:
                print(f"    修复失败 (ID: {mysql_doc.get(primary_key_field)}): {e}")
                failed_count += 1
    
    print(f"修复完成: ✅ {repaired_count}条成功, ❌ {failed_count}条失败")
    return repaired_count, failed_count


def repair_missing_data_count(mysql_connector, mongo_connector, table_name, batch_size=1000):
    """
    修复数据数量不一致的问题
    
    Args:
        mysql_connector: MySQL连接器
        mongo_connector: MongoDB连接器
        table_name: 表名
        batch_size: 批次处理大小
        
    Returns:
        (修复成功数量, 修复失败数量)
    """
    import time
    
    print(f"开始修复表 {table_name} 的数据数量不一致问题...")
    start_time = time.time()
    
    # 获取MySQL和MongoDB的记录数
    mysql_count = mysql_connector.get_table_count(table_name)
    mongo_count = mongo_connector.get_collection_count(table_name)
    
    if mysql_count == mongo_count:
        print(f"  ✅ 表 {table_name} 记录数量一致，无需修复")
        return 0, 0
    
    print(f"  检测到数据差异: MySQL={mysql_count:,}, MongoDB={mongo_count:,}")
    print(f"  预计需要扫描 {mysql_count:,} 条记录...")
    
    collection = mongo_connector.database[table_name]
    repaired_count = 0
    failed_count = 0
    
    # 特殊表的主键映射
    primary_key_mapping = {
        'ug_id_card_config': 'appID'  # ug_id_card_config表使用appID作为主键
    }
    
    # 获取当前表的主键字段
    primary_key_field = primary_key_mapping.get(table_name, 'id')
    
    # 获取MongoDB中已有的所有ID
    print("  正在获取MongoDB现有ID集合...")
    existing_ids = set()
    try:
        cursor = collection.find({}, {'_id': 1})
        total_mongo_ids = 0
        for doc in cursor:
            existing_ids.add(doc['_id'])
            total_mongo_ids += 1
            if total_mongo_ids % 10000 == 0:
                print(f"    已加载 {total_mongo_ids:,} 个ID...")
        print(f"  ✅ 已获取MongoDB现有ID集合: {total_mongo_ids:,} 个ID")
    except Exception as e:
        print(f"  ❌ 获取MongoDB现有ID失败: {e}")
        return 0, 1
    
    # 分批扫描MySQL数据，找出缺失的记录
    print(f"  开始扫描MySQL数据，批次大小: {batch_size:,}")
    
    total_processed = 0
    last_progress_time = start_time
    
    for offset in range(0, mysql_count, batch_size):
        current_time = time.time()
        
        # 每30秒显示一次进度
        if current_time - last_progress_time > 30:
            progress = (offset / mysql_count) * 100
            elapsed_time = current_time - start_time
            estimated_total_time = (elapsed_time / offset) * mysql_count if offset > 0 else 0
            remaining_time = estimated_total_time - elapsed_time if estimated_total_time > elapsed_time else 0
            
            print(f"    进度: {offset:,}/{mysql_count:,} ({progress:.1f}%) - "
                  f"已修复: {repaired_count:,} - "
                  f"耗时: {elapsed_time:.0f}s - "
                  f"预计剩余: {remaining_time:.0f}s")
            last_progress_time = current_time
        
        current_batch_size = min(batch_size, mysql_count - offset)
        
        # 从MySQL获取当前批次数据
        mysql_data = mysql_connector.fetch_data(table_name, current_batch_size, offset)
        
        # 批量处理缺失记录
        missing_records = []
        for mysql_doc in mysql_data:
            mysql_id = str(mysql_doc.get(primary_key_field))
            
            # 检查该ID是否在MongoDB中存在
            if mysql_id not in existing_ids:
                missing_records.append(mysql_doc)
        
        # 批量插入缺失记录
        if missing_records:
            try:
                # 转换为MongoDB格式
                mongo_docs = []
                for mysql_doc in missing_records:
                    mongo_doc = {}
                    for key, value in mysql_doc.items():
                        if key == primary_key_field:
                            mongo_doc['_id'] = str(value)
                        else:
                            mongo_doc[key] = value
                    
                    # 添加迁移元数据
                    mongo_doc['source'] = 'mysql'
                    mongo_doc['migrationTime'] = datetime.now()
                    mongo_docs.append(mongo_doc)
                
                # 批量插入到MongoDB
                if mongo_docs:
                    collection.insert_many(mongo_docs, ordered=False)
                    repaired_count += len(mongo_docs)
                    print(f"    批量修复 {len(mongo_docs)} 条缺失记录")
                    
            except Exception as e:
                print(f"    批量修复失败: {e}")
                # 回退到逐条插入
                for mysql_doc in missing_records:
                    try:
                        mysql_id = str(mysql_doc.get(primary_key_field))
                        mongo_doc = {}
                        for key, value in mysql_doc.items():
                            if key == primary_key_field:
                                mongo_doc['_id'] = str(value)
                            else:
                                mongo_doc[key] = value
                        
                        mongo_doc['source'] = 'mysql'
                        mongo_doc['migrationTime'] = datetime.now()
                        
                        collection.insert_one(mongo_doc)
                        repaired_count += 1
                    except Exception as single_error:
                        print(f"      单条修复失败 (ID: {mysql_id}): {single_error}")
                        failed_count += 1
        
        total_processed += len(mysql_data)
    
    total_time = time.time() - start_time
    print(f"数据数量修复完成: ✅ {repaired_count}条成功, ❌ {failed_count}条失败, 总耗时: {total_time:.1f}s")
    return repaired_count, failed_count


def verify_and_repair(config_file: str = "config.json", auto_repair: bool = True):
    """
    执行简化版数据迁移验证
    
    Args:
        config_file: 配置文件路径
        auto_repair: 是否自动修复发现的问题
        
    Returns:
        是否全部验证通过
    """
    
    # 加载配置
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except Exception as e:
        print(f"加载配置文件失败: {e}")
        return False
    
    # 初始化日志和连接器
    logger = MigrationLogger("verify_simple.log")
    mysql_connector = MySQLConnector(config['mysql'], logger)
    mongo_connector = MongoDBConnector(config['mongodb'], logger)
    
    # 连接数据库
    if not mysql_connector.connect():
        print("连接MySQL数据库失败")
        return False
    
    if not mongo_connector.connect():
        print("连接MongoDB数据库失败")
        mysql_connector.disconnect()
        return False
    
    try:
        # 获取要验证的表列表
        tables = config['verify'].get('tables', [])
        
        print("=" * 60)
        print("简化版数据迁移验证报告")
        if auto_repair:
            print("（自动修复模式 - 支持数量不一致修复）")
        print("=" * 60)
        
        all_passed = True
        verification_summary = {}
        
        for table_name in tables:
            print(f"\n表名: {table_name}")
            print("-" * 40)
            
            # 步骤1: 验证数据数量一致性
            count_passed, mysql_count, mongo_count, difference = verify_data_count(
                mysql_connector, mongo_connector, table_name
            )
            
            print("步骤1 - 数据数量验证:")
            print(f"  MySQL记录数: {mysql_count:,}")
            print(f"  MongoDB记录数: {mongo_count:,}")
            print(f"  一致性: {'✅ 通过' if count_passed else '❌ 失败'}")
            
            if not count_passed:
                print(f"  ❌ 差异数量: {difference:,}")
                all_passed = False
            
            # 步骤2: 自动修复数据数量不一致（如果启用）
            if auto_repair and not count_passed:
                print("\n步骤2 - 自动修复数据数量不一致:")
                repaired_count, failed_count = repair_missing_data_count(
                    mysql_connector, mongo_connector, table_name
                )
                
                if failed_count == 0 and repaired_count > 0:
                    print("  ✅ 数据数量不一致修复完成")
                    # 重新验证数据数量
                    count_passed, mysql_count, mongo_count, difference = verify_data_count(
                        mysql_connector, mongo_connector, table_name
                    )
                    if count_passed:
                        print(f"  ✅ 重新验证: MySQL={mysql_count:,}, MongoDB={mongo_count:,}")
                    else:
                        print(f"  ❌ 重新验证失败: 仍有 {difference:,} 条差异")
                elif failed_count > 0:
                    print(f"  ❌ 数据数量修复失败: {failed_count}条记录")
            
            # 如果数量仍然不一致，跳过内容验证
            if not count_passed:
                verification_summary[table_name] = {
                    'count_verification': False,
                    'content_verification': '跳过',
                    'inconsistent_records': []
                }
                continue
            
            # 步骤3: 验证数据内容一致性
            content_passed, content_message, inconsistent_records = verify_data_content(
                mysql_connector, mongo_connector, table_name
            )
            
            print("\n步骤3 - 数据内容验证:")
            print(f"  {content_message}")
            
            if not content_passed:
                missing_count = sum(1 for r in inconsistent_records if r['status'] == 'missing')
                inconsistent_count = sum(1 for r in inconsistent_records if r['status'] == 'inconsistent')
                print(f"  ❌ 问题记录: {missing_count}条缺失, {inconsistent_count}条不一致")
                all_passed = False
            
            # 步骤4: 自动修复数据内容不一致（如果启用）
            if auto_repair and not content_passed:
                print("\n步骤4 - 自动修复数据内容不一致:")
                repaired_count, failed_count = repair_inconsistent_data(
                    mysql_connector, mongo_connector, table_name, inconsistent_records
                )
                
                if failed_count == 0:
                    print("  ✅ 所有不一致数据修复完成")
                    # 重新验证内容一致性
                    content_passed, content_message, _ = verify_data_content(
                        mysql_connector, mongo_connector, table_name
                    )
                    if content_passed:
                        all_passed = True
                else:
                    print(f"  ❌ 修复失败: {failed_count}条记录")
            
            verification_summary[table_name] = {
                'count_verification': count_passed,
                'content_verification': content_passed,
                'inconsistent_records': inconsistent_records
            }
            
            table_passed = count_passed and content_passed
            print(f"\n表 {table_name} 验证结果: {'✅ 全部通过' if table_passed else '❌ 存在失败'}")
            print("-" * 40)
        
        # 生成汇总报告
        print("\n" + "=" * 60)
        print("验证汇总报告")
        print("=" * 60)
        
        passed_tables = sum(1 for details in verification_summary.values() 
                           if details['count_verification'] and details['content_verification'])
        
        print(f"总表数: {len(tables)}")
        print(f"通过表数: {passed_tables}")
        print(f"失败表数: {len(tables) - passed_tables}")
        print(f"整体通过率: {passed_tables/len(tables)*100:.1f}%")
        
        # 显示详细失败信息
        if not all_passed:
            print("\n详细失败信息:")
            for table_name, details in verification_summary.items():
                if not (details['count_verification'] and details['content_verification']):
                    print(f"\n表 {table_name}:")
                    if not details['count_verification']:
                        print("  ❌ 数据数量不一致")
                    if not details['content_verification']:
                        inconsistent_count = len(details['inconsistent_records'])
                        print(f"  ❌ 数据内容不一致: {inconsistent_count}条记录")
        
        if all_passed:
            print("\n🎉 所有表的数据迁移验证通过！")
            if auto_repair:
                print("🔧 自动修复功能已成功处理所有问题")
        else:
            print("\n⚠️  部分表的数据迁移验证失败")
            if auto_repair:
                print("🔧 自动修复功能已尝试修复，但仍有部分问题无法解决")
        
        return all_passed
        
    finally:
        # 断开数据库连接
        mysql_connector.disconnect()
        mongo_connector.disconnect()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='简化版数据迁移验证工具')
    parser.add_argument('--config', default='config.json', help='配置文件路径')
    parser.add_argument('--no-repair', action='store_false', dest='repair', help='禁用自动修复')
    
    args = parser.parse_args()
    
    success = verify_and_repair(args.config, args.repair)
    sys.exit(0 if success else 1)