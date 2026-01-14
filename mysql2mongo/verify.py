#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据迁移完整性验证工具
用于验证MySQL到MongoDB数据迁移的完整性
支持迁移元数据字段验证、数据内容验证和字段映射验证
支持自动修复迁移失败或遗漏的数据
"""

import json
import sys
import random
from datetime import datetime, timedelta
from migration_tool import MySQLConnector, MongoDBConnector, MigrationLogger


def verify_migration_metadata(collection, table_name, expected_source='mysql'):
    """
    验证迁移元数据字段完整性
    
    Args:
        collection: MongoDB集合对象
        table_name: 表名
        expected_source: 期望的source字段值
        
    Returns:
        (是否通过, 错误信息)
    """
    total_count = collection.count_documents({})
    
    if total_count == 0:
        return True, "空表，跳过元数据验证"
    
    # 验证source字段
    source_count = collection.count_documents({'source': expected_source})
    if source_count != total_count:
        return False, f"source字段验证失败: {source_count}/{total_count}条记录source字段正确"
    
    # 验证migrationTime字段
    time_count = collection.count_documents({
        'migrationTime': {'$type': 'date'}
    })
    if time_count != total_count:
        return False, f"migrationTime字段验证失败: {time_count}/{total_count}条记录时间格式正确"
    
    # 验证迁移时间合理性（最近7天内）
    recent_time = datetime.now() - timedelta(days=7)
    recent_count = collection.count_documents({
        'migrationTime': {'$gte': recent_time}
    })
    if recent_count != total_count:
        return True, f"部分数据迁移时间较旧: {recent_count}/{total_count}条记录在最近7天内"
    
    return True, f"迁移元数据验证通过: {total_count}条记录"


def verify_data_content_complete(mysql_connector, mongo_connector, table_name, batch_size=1000):
    """
    完整验证数据内容一致性（排除迁移元数据）
    
    Args:
        mysql_connector: MySQL连接器
        mongo_connector: MongoDB连接器
        table_name: 表名
        batch_size: 批次处理大小
        
    Returns:
        (是否通过, 错误信息, 验证详情)
    """
    # 获取MySQL总记录数
    total_count = mysql_connector.get_table_count(table_name)
    
    if total_count == 0:
        return True, "空表，跳过内容验证", {}
    
    print(f"开始完整验证表 {table_name} 的数据内容，共 {total_count:,} 条记录...")
    
    collection = mongo_connector.database[table_name]
    
    # 排除迁移元数据字段进行比较
    metadata_fields = ['migrationTime', 'source', '_id']
    
    comparison_results = []
    all_passed = True
    processed_count = 0
    
    # 分批处理所有记录
    for offset in range(0, total_count, batch_size):
        current_batch_size = min(batch_size, total_count - offset)
        
        # 从MySQL获取当前批次数据
        mysql_data = mysql_connector.fetch_data(table_name, current_batch_size, offset)
        
        # 获取当前批次的所有ID
        mysql_ids = [str(doc.get('id')) for doc in mysql_data]
        
        # 从MongoDB批量获取对应数据
        mongo_docs = {}
        cursor = collection.find({'_id': {'$in': mysql_ids}})
        for doc in cursor:
            mongo_docs[doc['_id']] = doc
        
        # 逐条比较数据内容
        for i, mysql_doc in enumerate(mysql_data):
            mysql_id = str(mysql_doc.get('id'))
            mongo_doc = mongo_docs.get(mysql_id)
            
            if not mongo_doc:
                comparison_results.append({
                    'index': processed_count + i,
                    'mysql_id': mysql_id,
                    'status': '❌ 失败',
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
            if mysql_filtered == mongo_filtered:
                comparison_results.append({
                    'index': processed_count + i,
                    'mysql_id': mysql_id,
                    'status': '✅ 通过',
                    'details': f"{len(mysql_filtered)}个字段一致"
                })
            else:
                # 找出不一致的字段
                differences = []
                all_keys = set(mysql_filtered.keys()) | set(mongo_filtered.keys())
                
                for key in all_keys:
                    mysql_val = mysql_filtered.get(key)
                    mongo_val = mongo_filtered.get(key)
                    
                    if mysql_val != mongo_val:
                        differences.append(f"{key}: MySQL={mysql_val}, MongoDB={mongo_val}")
                
                comparison_results.append({
                    'index': processed_count + i,
                    'mysql_id': mysql_id,
                    'status': '❌ 失败',
                    'error': f"{len(differences)}个字段不一致",
                    'differences': differences[:3]  # 只显示前3个差异
                })
                all_passed = False
        
        processed_count += len(mysql_data)
        print(f"  已处理 {processed_count:,}/{total_count:,} 条记录...")
    
    if all_passed:
        return True, f"数据内容完整验证通过: {total_count}条记录全部一致", comparison_results
    else:
        failed_count = sum(1 for r in comparison_results if r['status'] == '❌ 失败')
        return False, f"数据内容验证失败: {failed_count}/{total_count}条记录不一致", comparison_results


def verify_field_mapping(table_name, table_mappings_file="table_mappings.json"):
    """
    验证字段映射正确性
    
    Args:
        table_name: 表名
        table_mappings_file: 字段映射配置文件
        
    Returns:
        (是否通过, 错误信息, 映射详情)
    """
    try:
        with open(table_mappings_file, 'r', encoding='utf-8') as f:
            table_mappings = json.load(f)
    except Exception as e:
        return False, f"加载字段映射文件失败: {e}", {}
    
    if table_name not in table_mappings:
        return True, "未找到字段映射配置，跳过验证", {}
    
    mappings = table_mappings[table_name].get('transformations', {})
    
    if not mappings:
        return True, "无字段映射配置，跳过验证", {}
    
    mapping_results = []
    all_passed = True
    
    for field, mapping in mappings.items():
        target_field = mapping.get('target', field)
        field_type = mapping.get('type', '未知')
        
        mapping_results.append({
            'source_field': field,
            'target_field': target_field,
            'field_type': field_type,
            'status': '✅ 配置正确'
        })
    
    return True, f"字段映射验证通过: {len(mapping_results)}个字段", mapping_results


def verify_migration(config_file: str = "config.json", auto_repair: bool = False):
    """
    验证数据迁移完整性
    
    Args:
        config_file: 配置文件路径
        auto_repair: 是否自动修复发现的问题
    """
    
    # 加载配置
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except Exception as e:
        print(f"加载配置文件失败: {e}")
        return False
    
    # 初始化日志和连接器
    logger = MigrationLogger("verify.log")
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
        
        # 获取修复配置
        repair_config = config.get('repair', {
            'repair_missing': True,
            'repair_inconsistent': True,
            'repair_metadata': True
        })
        
        print("=" * 80)
        print("数据迁移完整性验证报告")
        if auto_repair:
            print("（自动修复模式）")
        print("=" * 80)
        
        all_passed = True
        verification_details = {}
        repair_summary = {}
        
        for table_name in tables:
            print(f"\n表名: {table_name}")
            print("-" * 50)
            
            # 获取MySQL记录数
            mysql_count = mysql_connector.get_table_count(table_name)
            
            # 获取MongoDB记录数
            mongo_count = mongo_connector.get_collection_count(table_name)
            
            # 基础验证：记录数量一致性
            base_consistent = mysql_count == mongo_count
            base_status = "✅ 通过" if base_consistent else "❌ 失败"
            
            print(f"基础验证:")
            print(f"  MySQL记录数: {mysql_count:,}")
            print(f"  MongoDB记录数: {mongo_count:,}")
            print(f"  一致性验证: {base_status}")
            
            if not base_consistent:
                print(f"  ❌ 差异数量: {abs(mysql_count - mongo_count):,}")
                
                # 自动修复：遗漏数据
                if auto_repair:
                    print("\n🔧 开始自动修复遗漏数据...")
                    repair_success, repair_details = auto_repair_data(
                        mysql_connector, mongo_connector, table_name, repair_config
                    )
                    repair_summary[table_name] = repair_details
                    
                    # 重新验证基础一致性
                    mongo_count_after_repair = mongo_connector.get_collection_count(table_name)
                    base_consistent_after_repair = mysql_count == mongo_count_after_repair
                    
                    if base_consistent_after_repair:
                        print(f"✅ 修复后基础验证通过")
                        base_consistent = True
                    else:
                        print(f"❌ 修复后基础验证仍然失败")
                        all_passed = False
                else:
                    all_passed = False
            
            # 如果基础验证失败且未修复，跳过其他验证
            if not base_consistent:
                verification_details[table_name] = {
                    'base_verification': False,
                    'metadata_verification': '跳过',
                    'content_verification': '跳过',
                    'mapping_verification': '跳过'
                }
                continue
            
            # 迁移元数据验证
            collection = mongo_connector.database[table_name]
            metadata_passed, metadata_message = verify_migration_metadata(collection, table_name)
            metadata_status = "✅ 通过" if metadata_passed else "❌ 失败"
            
            print(f"迁移元数据验证: {metadata_status}")
            print(f"  {metadata_message}")
            
            # 自动修复：迁移元数据
            if not metadata_passed and auto_repair:
                print("\n🔧 开始自动修复迁移元数据...")
                repaired, failed = repair_migration_metadata(mongo_connector, table_name)
                if failed == 0:
                    metadata_passed = True
                    print("✅ 迁移元数据修复完成")
                else:
                    print("❌ 迁移元数据修复失败")
            
            # 数据内容完整验证（仅对非空表进行）
            content_passed, content_message, content_details = True, "空表，跳过", {}
            if mysql_count > 0:
                content_passed, content_message, content_details = verify_data_content_complete(
                    mysql_connector, mongo_connector, table_name, batch_size=1000
                )
            
            content_status = "✅ 通过" if content_passed else "❌ 失败"
            print(f"数据内容验证: {content_status}")
            print(f"  {content_message}")
            
            # 自动修复：不一致数据
            if not content_passed and auto_repair:
                print("\n🔧 开始自动修复不一致数据...")
                repaired, failed = repair_inconsistent_data(mysql_connector, mongo_connector, table_name)
                if failed == 0:
                    content_passed = True
                    print("✅ 不一致数据修复完成")
                else:
                    print("❌ 不一致数据修复失败")
            
            # 字段映射验证
            mapping_passed, mapping_message, mapping_details = verify_field_mapping(table_name)
            mapping_status = "✅ 通过" if mapping_passed else "❌ 失败"
            
            print(f"字段映射验证: {mapping_status}")
            print(f"  {mapping_message}")
            
            # 汇总验证结果
            table_passed = base_consistent and metadata_passed and content_passed and mapping_passed
            if not table_passed:
                all_passed = False
            
            verification_details[table_name] = {
                'base_verification': base_consistent,
                'metadata_verification': metadata_passed,
                'content_verification': content_passed,
                'mapping_verification': mapping_passed,
                'content_details': content_details,
                'mapping_details': mapping_details
            }
            
            print(f"\n表 {table_name} 验证结果: {'✅ 全部通过' if table_passed else '❌ 存在失败'}")
            print("-" * 50)
        
        # 生成汇总报告
        print("\n" + "=" * 80)
        print("验证汇总报告")
        if auto_repair:
            print("（包含自动修复结果）")
        print("=" * 80)
        
        passed_tables = sum(1 for details in verification_details.values() 
                           if details['base_verification'] and 
                              details['metadata_verification'] and 
                              details['content_verification'] and 
                              details['mapping_verification'])
        
        print(f"总表数: {len(tables)}")
        print(f"通过表数: {passed_tables}")
        print(f"失败表数: {len(tables) - passed_tables}")
        print(f"整体通过率: {passed_tables/len(tables)*100:.1f}%")
        
        # 显示修复结果
        if auto_repair and repair_summary:
            print("\n" + "-" * 50)
            print("自动修复结果")
            print("-" * 50)
            
            total_repaired = 0
            total_failed = 0
            
            for table_name, details in repair_summary.items():
                table_repaired = (details.get('missing_repaired', 0) + 
                                details.get('inconsistent_repaired', 0) + 
                                details.get('metadata_repaired', 0))
                table_failed = (details.get('missing_failed', 0) + 
                              details.get('inconsistent_failed', 0) + 
                              details.get('metadata_failed', 0))
                
                total_repaired += table_repaired
                total_failed += table_failed
                
                if table_repaired > 0 or table_failed > 0:
                    print(f"表 {table_name}:")
                    if details.get('missing_repaired', 0) > 0:
                        print(f"  遗漏数据修复: ✅ {details['missing_repaired']}条")
                    if details.get('missing_failed', 0) > 0:
                        print(f"  遗漏数据修复: ❌ {details['missing_failed']}条失败")
                    if details.get('inconsistent_repaired', 0) > 0:
                        print(f"  不一致数据修复: ✅ {details['inconsistent_repaired']}条")
                    if details.get('inconsistent_failed', 0) > 0:
                        print(f"  不一致数据修复: ❌ {details['inconsistent_failed']}条失败")
                    if details.get('metadata_repaired', 0) > 0:
                        print(f"  迁移元数据修复: ✅ {details['metadata_repaired']}条")
                    if details.get('metadata_failed', 0) > 0:
                        print(f"  迁移元数据修复: ❌ {details['metadata_failed']}条失败")
            
            print(f"\n总计修复: ✅ {total_repaired}条成功, ❌ {total_failed}条失败")
        
        if all_passed:
            print("\n🎉 所有表的数据迁移完整性验证通过！")
            if auto_repair:
                print("🔧 自动修复功能已成功处理所有问题")
        else:
            print("\n⚠️  部分表的数据迁移完整性验证失败")
            if auto_repair:
                print("🔧 自动修复功能已尝试修复，但仍有部分问题无法解决")
            
            print("\n详细失败信息:")
            for table_name, details in verification_details.items():
                if not (details['base_verification'] and 
                       details['metadata_verification'] and 
                       details['content_verification'] and 
                       details['mapping_verification']):
                    print(f"\n表 {table_name}:")
                    if not details['base_verification']:
                        print("  ❌ 基础验证失败")
                    if not details['metadata_verification']:
                        print("  ❌ 迁移元数据验证失败")
                    if not details['content_verification']:
                        print("  ❌ 数据内容验证失败")
                    if not details['mapping_verification']:
                        print("  ❌ 字段映射验证失败")
        
        return all_passed
        
    finally:
        # 断开数据库连接
        mysql_connector.disconnect()
        mongo_connector.disconnect()


def check_progress():
    """检查迁移进度文件"""
    progress_file = "migration_progress.json"
    
    try:
        with open(progress_file, 'r', encoding='utf-8') as f:
            progress = json.load(f)
        
        print("=" * 80)
        print("迁移进度检查")
        print("=" * 80)
        
        if progress:
            for table_name, info in progress.items():
                print(f"表名: {table_name}")
                print(f"  当前偏移量: {info.get('offset', 0):,}")
                print(f"  已迁移数量: {info.get('migrated_count', 0):,}")
                print(f"  最后更新时间: {info.get('last_update', '未知')}")
                print("-" * 50)
            print("⚠️  存在未完成的迁移进度，可以使用断点续传功能继续迁移")
        else:
            print("✅ 没有未完成的迁移进度")
        
    except FileNotFoundError:
        print("✅ 迁移进度文件不存在，表示没有未完成的迁移任务")
    except Exception as e:
        print(f"❌ 读取进度文件失败: {e}")


def repair_missing_data(mysql_connector, mongo_connector, table_name, batch_size=1000):
    """
    修复遗漏的数据
    
    Args:
        mysql_connector: MySQL连接器
        mongo_connector: MongoDB连接器
        table_name: 表名
        batch_size: 批次大小
        
    Returns:
        (修复成功数量, 修复失败数量)
    """
    import time
    
    print(f"开始修复表 {table_name} 的遗漏数据...")
    start_time = time.time()
    
    # 获取MySQL总记录数
    mysql_count = mysql_connector.get_table_count(table_name)
    
    # 获取MongoDB总记录数
    mongo_count = mongo_connector.get_collection_count(table_name)
    
    if mysql_count == mongo_count:
        print(f"✅ 表 {table_name} 记录数量一致，无需修复")
        return 0, 0
    
    print(f"检测到数据差异: MySQL={mysql_count:,}, MongoDB={mongo_count:,}")
    print(f"预计需要处理 {mysql_count:,} 条记录...")
    
    # 获取MongoDB中已有的ID集合
    collection = mongo_connector.database[table_name]
    existing_ids = set()
    
    try:
        print("正在获取MongoDB现有ID集合...")
        cursor = collection.find({}, {'_id': 1})
        total_mongo_ids = 0
        for doc in cursor:
            existing_ids.add(doc['_id'])
            total_mongo_ids += 1
            if total_mongo_ids % 10000 == 0:
                print(f"  已加载 {total_mongo_ids:,} 个ID...")
        print(f"✅ 已获取MongoDB现有ID集合: {total_mongo_ids:,} 个ID")
    except Exception as e:
        print(f"❌ 获取MongoDB现有ID失败: {e}")
        return 0, 1
    
    # 分批获取MySQL数据并检查遗漏
    offset = 0
    repaired_count = 0
    failed_count = 0
    total_processed = 0
    last_progress_time = time.time()
    
    print(f"开始扫描MySQL数据，批次大小: {batch_size:,}")
    
    while offset < mysql_count:
        current_time = time.time()
        
        # 每30秒显示一次进度
        if current_time - last_progress_time > 30:
            progress = (offset / mysql_count) * 100
            elapsed_time = current_time - start_time
            estimated_total_time = (elapsed_time / offset) * mysql_count if offset > 0 else 0
            remaining_time = estimated_total_time - elapsed_time if estimated_total_time > elapsed_time else 0
            
            print(f"  进度: {offset:,}/{mysql_count:,} ({progress:.1f}%) - "
                  f"已修复: {repaired_count:,} - "
                  f"耗时: {elapsed_time:.0f}s - "
                  f"预计剩余: {remaining_time:.0f}s")
            last_progress_time = current_time
        
        # 获取当前批次数据
        batch_start_time = time.time()
        mysql_data = mysql_connector.fetch_data(table_name, batch_size, offset)
        fetch_time = time.time() - batch_start_time
        
        if not mysql_data:
            print(f"  已处理完所有数据，总处理: {total_processed:,} 条")
            break
        
        # 筛选出遗漏的数据
        missing_data = []
        check_start_time = time.time()
        
        for doc in mysql_data:
            mysql_id = str(doc.get('id'))
            if mysql_id not in existing_ids:
                # 创建新文档（排除MySQL的id字段，只保留正确的字段映射）
                new_doc = {}
                
                # 复制MySQL文档的所有字段，但排除id字段
                for key, value in doc.items():
                    if key != 'id':  # 排除MySQL的id字段
                        new_doc[key] = value
                
                # 设置MongoDB的主键和迁移元数据
                new_doc['_id'] = mysql_id
                new_doc['source'] = 'mysql'
                new_doc['migrationTime'] = datetime.now()
                missing_data.append(new_doc)
        
        check_time = time.time() - check_start_time
        total_processed += len(mysql_data)
        
        # 插入遗漏的数据
        if missing_data:
            insert_start_time = time.time()
            try:
                result = collection.insert_many(missing_data, ordered=False)
                repaired_count += len(result.inserted_ids)
                insert_time = time.time() - insert_start_time
                
                print(f"  批次 {offset//batch_size + 1}: "
                      f"获取数据 {fetch_time:.2f}s, "
                      f"检查遗漏 {check_time:.2f}s, "
                      f"插入 {insert_time:.2f}s - "
                      f"修复 {len(result.inserted_ids)} 条遗漏数据")
                
            except BulkWriteError as e:
                # 部分插入成功的情况
                inserted_count = len(e.details.get('writeErrors', []))
                repaired_count += inserted_count
                failed_count += len(missing_data) - inserted_count
                print(f"  部分修复失败: 成功{inserted_count}条, 失败{len(missing_data) - inserted_count}条")
            except Exception as e:
                failed_count += len(missing_data)
                print(f"❌ 批量插入失败: {e}")
        else:
            # 没有遗漏数据，显示进度
            if total_processed % (batch_size * 10) == 0:  # 每10个批次显示一次
                print(f"  批次 {offset//batch_size + 1}: "
                      f"获取数据 {fetch_time:.2f}s, "
                      f"检查遗漏 {check_time:.2f}s - "
                      f"无遗漏数据")
        
        offset += batch_size
    
    total_time = time.time() - start_time
    print(f"✅ 表 {table_name} 修复完成: "
          f"成功{repaired_count:,}条, "
          f"失败{failed_count:,}条, "
          f"总耗时{total_time:.1f}秒")
    
    return repaired_count, failed_count


def repair_inconsistent_data(mysql_connector, mongo_connector, table_name, batch_size=1000):
    """
    修复不一致的数据（全量检查）
    
    Args:
        mysql_connector: MySQL连接器
        mongo_connector: MongoDB连接器
        table_name: 表名
        batch_size: 批次大小
        
    Returns:
        (修复成功数量, 修复失败数量)
    """
    import time
    
    print(f"开始全量检查并修复表 {table_name} 的不一致数据...")
    start_time = time.time()
    
    # 获取MySQL总记录数
    total_count = mysql_connector.get_table_count(table_name)
    
    if total_count == 0:
        print(f"✅ 表 {table_name} 为空，无需修复")
        return 0, 0
    
    print(f"全量检查 {total_count:,} 条记录，批次大小: {batch_size:,}")
    
    # 从MongoDB获取对应数据并检查不一致
    collection = mongo_connector.database[table_name]
    
    # 排除迁移元数据字段进行比较
    metadata_fields = ['migrationTime', 'source', '_id']
    
    repaired_count = 0
    failed_count = 0
    checked_count = 0
    offset = 0
    last_progress_time = time.time()
    
    while offset < total_count:
        current_time = time.time()
        
        # 每10秒显示一次进度（更频繁的反馈）
        if current_time - last_progress_time > 10:
            progress = (offset / total_count) * 100
            elapsed_time = current_time - start_time
            estimated_total_time = (elapsed_time / offset) * total_count if offset > 0 else 0
            remaining_time = estimated_total_time - elapsed_time if estimated_total_time > elapsed_time else 0
            
            # 使用进度条样式显示
            bar_length = 40
            filled_length = int(bar_length * offset // total_count)
            bar = '█' * filled_length + '░' * (bar_length - filled_length)
            
            print(f"\r  [{bar}] {offset:,}/{total_count:,} ({progress:.1f}%) - "
                  f"已修复: {repaired_count:,} - "
                  f"耗时: {elapsed_time:.0f}s - "
                  f"预计剩余: {remaining_time:.0f}s", end='', flush=True)
            last_progress_time = current_time
        
        # 获取当前批次数据
        batch_start_time = time.time()
        mysql_data = mysql_connector.fetch_data(table_name, batch_size, offset)
        fetch_time = time.time() - batch_start_time
        
        if not mysql_data:
            print(f"\n  已处理完所有数据，总检查: {checked_count:,} 条")
            break
        
        check_start_time = time.time()
        batch_inconsistent_count = 0
        batch_consistent_count = 0
        batch_inconsistent_ids = []  # 存储不一致记录的ID
        
        for mysql_doc in mysql_data:
            checked_count += 1
            
            # 获取对应的MongoDB文档
            mysql_id = str(mysql_doc.get('id'))
            mongo_doc = collection.find_one({'_id': mysql_id})
            
            if not mongo_doc:
                # 记录不存在，由遗漏修复功能处理
                continue
            
            # 过滤掉迁移元数据字段
            # 注意：MySQL的id字段对应MongoDB的_id字段，都需要过滤
            mysql_filtered = {k: v for k, v in mysql_doc.items() 
                             if k not in metadata_fields and k != 'id'}
            mongo_filtered = {k: v for k, v in mongo_doc.items() 
                             if k not in metadata_fields and k != '_id'}
            
            # 检查过滤后字段差异
            mysql_keys = set(mysql_filtered.keys())
            mongo_keys = set(mongo_filtered.keys())
            
            # 比较数据内容
            if mysql_filtered != mongo_filtered:
                batch_inconsistent_count += 1
                batch_inconsistent_ids.append(mysql_id)
                
                # 如果字段不一致，显示详细信息
                if mysql_keys != mongo_keys:
                    missing_in_mongo = mysql_keys - mongo_keys
                    missing_in_mysql = mongo_keys - mysql_keys
                    print(f"\n  ⚠️  记录 {mysql_id} 字段名不一致:")
                    print(f"     MongoDB缺失字段: {list(missing_in_mongo)}")
                    print(f"     MySQL缺失字段: {list(missing_in_mysql)}")
                
                # 详细分析数据差异
                differences = []
                # 只比较两个字典中都存在的字段（交集）
                common_keys = mysql_keys.intersection(mongo_keys)
                
                for key in common_keys:
                    mysql_val = mysql_filtered[key]  # 直接访问，因为key肯定存在
                    mongo_val = mongo_filtered[key]  # 直接访问，因为key肯定存在
                    
                    if mysql_val != mongo_val:
                        # 尝试类型转换后再比较
                        try:
                            # 处理日期时间类型
                            if isinstance(mysql_val, datetime) and isinstance(mongo_val, datetime):
                                if mysql_val == mongo_val:
                                    continue
                            
                            # 处理数字类型
                            if isinstance(mysql_val, (int, float)) and isinstance(mongo_val, (int, float)):
                                if float(mysql_val) == float(mongo_val):
                                    continue
                            
                            # 处理字符串类型
                            if isinstance(mysql_val, str) and isinstance(mongo_val, str):
                                if mysql_val.strip() == mongo_val.strip():
                                    continue
                        except:
                            pass
                        
                        differences.append(f"{key}: MySQL={mysql_val} ({type(mysql_val).__name__}), "
                                          f"MongoDB={mongo_val} ({type(mongo_val).__name__})")
                
                # 显示完整的数据对比
                print(f"\n  🔍 记录 {mysql_id} 数据不一致:")
                
                # 显示MySQL完整数据（排除迁移元数据）
                print(f"     MySQL数据:")
                for key, value in mysql_filtered.items():
                    value_str = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                    print(f"       {key}: {value_str} ({type(value).__name__})")
                
                # 显示MongoDB完整数据（排除迁移元数据）
                print(f"     MongoDB数据:")
                for key, value in mongo_filtered.items():
                    value_str = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                    print(f"       {key}: {value_str} ({type(value).__name__})")
                
                # 显示具体差异
                if differences:
                    print(f"     🔍 具体差异 ({len(differences)}个):")
                    for diff in differences[:5]:  # 最多显示5个差异
                        print(f"       {diff}")
                    if len(differences) > 5:
                        print(f"       ... 还有{len(differences)-5}个差异")
                else:
                    print(f"     ℹ️  字段一致但数据不一致（可能是字段缺失导致）")
                
                # 数据不一致，需要修复
                try:
                    # 创建更新文档（排除MySQL的id字段，只保留正确的字段映射）
                    update_doc = {}
                    
                    # 复制MySQL文档的所有字段，但排除id字段
                    for key, value in mysql_doc.items():
                        if key != 'id':  # 排除MySQL的id字段
                            update_doc[key] = value
                    
                    # 设置MongoDB的主键和迁移元数据
                    update_doc['_id'] = mysql_id
                    update_doc['source'] = 'mysql'
                    update_doc['migrationTime'] = datetime.now()
                    
                    # 使用replace_one替换整个文档
                    result = collection.replace_one({'_id': mysql_id}, update_doc)
                    
                    if result.modified_count > 0:
                        repaired_count += 1
                        if repaired_count % 50 == 0:  # 每修复50条显示一次
                            print(f"\n  🔧 已修复记录 {mysql_id} (累计: {repaired_count:,})")
                    else:
                        failed_count += 1
                        print(f"\n  ❌ 修复记录 {mysql_id} 失败")
                        
                except Exception as e:
                    failed_count += 1
                    print(f"\n  ❌ 修复记录 {mysql_id} 时出错: {e}")
            else:
                batch_consistent_count += 1
        
        check_time = time.time() - check_start_time
        
        # 每批次都显示处理信息（更频繁的反馈）
        batch_num = offset//batch_size + 1
        print(f"\n  批次 {batch_num}: "
              f"获取数据 {fetch_time:.2f}s, "
              f"检查数据 {check_time:.2f}s - "
              f"一致: {batch_consistent_count}, 不一致: {batch_inconsistent_count}")
        
        # 显示不一致记录的详细信息（最多显示5个）
        if batch_inconsistent_count > 0:
            print(f"     不一致记录ID: {', '.join(batch_inconsistent_ids[:5])}")
            if batch_inconsistent_count > 5:
                print(f"     ... 还有{batch_inconsistent_count - 5}个不一致记录")
        
        # 每处理10000条记录显示一次详细统计
        if checked_count % 10000 == 0:
            print(f"  📊 累计统计: 检查{checked_count:,}条, 修复{repaired_count:,}条, 失败{failed_count:,}条")
        
        offset += batch_size
    
    total_time = time.time() - start_time
    print(f"✅ 表 {table_name} 全量数据修复完成: "
          f"检查{checked_count:,}条, "
          f"成功{repaired_count:,}条, "
          f"失败{failed_count:,}条, "
          f"总耗时{total_time:.1f}秒")
    
    if failed_count == 0:
        print(f"🎉 表 {table_name} 所有数据一致性验证通过！")
    else:
        print(f"⚠️  表 {table_name} 存在 {failed_count:,} 条数据修复失败")
    
    return repaired_count, failed_count


def repair_migration_metadata(mongo_connector, table_name):
    """
    修复迁移元数据
    
    Args:
        mongo_connector: MongoDB连接器
        table_name: 表名
        
    Returns:
        (修复成功数量, 修复失败数量)
    """
    print(f"开始修复表 {table_name} 的迁移元数据...")
    
    collection = mongo_connector.database[table_name]
    
    # 检查并修复缺失的迁移元数据
    repaired_count = 0
    failed_count = 0
    
    try:
        # 修复缺失source字段的记录
        result_source = collection.update_many(
            {'source': {'$exists': False}}, 
            {'$set': {'source': 'mysql'}}
        )
        
        # 修复缺失migrationTime字段的记录
        result_time = collection.update_many(
            {'migrationTime': {'$exists': False}}, 
            {'$set': {'migrationTime': datetime.now()}}
        )
        
        repaired_count = result_source.modified_count + result_time.modified_count
        
        if result_source.modified_count > 0:
            print(f"  修复了 {result_source.modified_count} 条记录的source字段")
        
        if result_time.modified_count > 0:
            print(f"  修复了 {result_time.modified_count} 条记录的migrationTime字段")
        
        if repaired_count == 0:
            print(f"✅ 表 {table_name} 的迁移元数据完整，无需修复")
            
    except Exception as e:
        failed_count = 1
        print(f"❌ 修复迁移元数据失败: {e}")
    
    return repaired_count, failed_count


def auto_repair_data(mysql_connector, mongo_connector, table_name, repair_config):
    """
    自动修复数据
    
    Args:
        mysql_connector: MySQL连接器
        mongo_connector: MongoDB连接器
        table_name: 表名
        repair_config: 修复配置
        
    Returns:
        (是否修复成功, 修复详情)
    """
    print(f"\n开始自动修复表 {table_name}...")
    print("-" * 50)
    
    repair_details = {
        'missing_repaired': 0,
        'missing_failed': 0,
        'inconsistent_repaired': 0,
        'inconsistent_failed': 0,
        'metadata_repaired': 0,
        'metadata_failed': 0
    }
    
    all_success = True
    
    # 修复遗漏数据
    if repair_config.get('repair_missing', True):
        repaired, failed = repair_missing_data(mysql_connector, mongo_connector, table_name)
        repair_details['missing_repaired'] = repaired
        repair_details['missing_failed'] = failed
        if failed > 0:
            all_success = False
    
    # 修复不一致数据
    if repair_config.get('repair_inconsistent', True):
        repaired, failed = repair_inconsistent_data(mysql_connector, mongo_connector, table_name, batch_size=1000)
        repair_details['inconsistent_repaired'] = repaired
        repair_details['inconsistent_failed'] = failed
        if failed > 0:
            all_success = False
    
    # 修复迁移元数据
    if repair_config.get('repair_metadata', True):
        repaired, failed = repair_migration_metadata(mongo_connector, table_name)
        repair_details['metadata_repaired'] = repaired
        repair_details['metadata_failed'] = failed
        if failed > 0:
            all_success = False
    
    print(f"表 {table_name} 自动修复完成")
    print("-" * 50)
    
    return all_success, repair_details


def repair_only_mode(config_file: str = "config.json"):
    """
    仅执行修复模式
    
    Args:
        config_file: 配置文件路径
        
    Returns:
        (是否修复成功)
    """
    # 加载配置
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except Exception as e:
        print(f"加载配置文件失败: {e}")
        return False
    
    # 初始化日志和连接器
    logger = MigrationLogger("verify.log")
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
        # 获取要修复的表列表
        tables = config['verify'].get('tables', [])
        
        # 获取修复配置
        repair_config = config.get('repair', {
            'repair_missing': True,
            'repair_inconsistent': True,
            'repair_metadata': True
        })
        
        print("=" * 80)
        print("数据修复模式")
        print("=" * 80)
        
        all_success = True
        repair_summary = {}
        
        for table_name in tables:
            print(f"\n表名: {table_name}")
            print("-" * 50)
            
            # 执行自动修复
            repair_success, repair_details = auto_repair_data(
                mysql_connector, mongo_connector, table_name, repair_config
            )
            
            repair_summary[table_name] = repair_details
            
            if not repair_success:
                all_success = False
            
            print(f"\n表 {table_name} 修复结果: {'✅ 成功' if repair_success else '❌ 存在失败'}")
            print("-" * 50)
        
        # 生成修复汇总报告
        print("\n" + "=" * 80)
        print("修复汇总报告")
        print("=" * 80)
        
        total_repaired = 0
        total_failed = 0
        
        for table_name, details in repair_summary.items():
            table_repaired = (details.get('missing_repaired', 0) + 
                            details.get('inconsistent_repaired', 0) + 
                            details.get('metadata_repaired', 0))
            table_failed = (details.get('missing_failed', 0) + 
                          details.get('inconsistent_failed', 0) + 
                          details.get('metadata_failed', 0))
            
            total_repaired += table_repaired
            total_failed += table_failed
            
            print(f"\n表 {table_name}:")
            if details.get('missing_repaired', 0) > 0:
                print(f"  遗漏数据修复: ✅ {details['missing_repaired']}条")
            if details.get('missing_failed', 0) > 0:
                print(f"  遗漏数据修复: ❌ {details['missing_failed']}条失败")
            if details.get('inconsistent_repaired', 0) > 0:
                print(f"  不一致数据修复: ✅ {details['inconsistent_repaired']}条")
            if details.get('inconsistent_failed', 0) > 0:
                print(f"  不一致数据修复: ❌ {details['inconsistent_failed']}条失败")
            if details.get('metadata_repaired', 0) > 0:
                print(f"  迁移元数据修复: ✅ {details['metadata_repaired']}条")
            if details.get('metadata_failed', 0) > 0:
                print(f"  迁移元数据修复: ❌ {details['metadata_failed']}条失败")
        
        print(f"\n总计修复: ✅ {total_repaired}条成功, ❌ {total_failed}条失败")
        
        if all_success:
            print("\n🎉 所有表的数据修复完成！")
        else:
            print("\n⚠️  部分表的数据修复存在失败")
        
        return all_success
        
    finally:
        # 断开数据库连接
        mysql_connector.disconnect()
        mongo_connector.disconnect()


if __name__ == "__main__":
    # 解析命令行参数
    import argparse
    
    parser = argparse.ArgumentParser(description='数据迁移完整性验证工具')
    parser.add_argument('config_file', nargs='?', default='config.json', 
                       help='配置文件路径 (默认: config.json)')
    parser.add_argument('--auto-repair', action='store_true', 
                       help='自动修复发现的问题')
    parser.add_argument('--repair-only', action='store_true',
                       help='仅执行修复，不进行验证')
    
    args = parser.parse_args()
    
    print("开始数据迁移完整性验证...")
    if args.auto_repair:
        print("（自动修复模式已启用）")
    if args.repair_only:
        print("（仅执行修复模式）")
    print()
    
    # 检查迁移进度
    check_progress()
    print()
    
    # 验证数据完整性
    if args.repair_only:
        # 仅执行修复模式
        success = repair_only_mode(args.config_file)
    else:
        # 正常验证模式（可选自动修复）
        success = verify_migration(args.config_file, args.auto_repair)
    
    sys.exit(0 if success else 1)