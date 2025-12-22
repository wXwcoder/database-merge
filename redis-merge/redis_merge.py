#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Redis数据库合并工具

该脚本用于将DB1和DB2的所有键（包括集合）合并到DB3
支持完善的日志记录和错误处理
"""

import redis
import time
import logging
import argparse
from redis_compare import merge_to_new_db, connect_redis_with_retry

# 设置redis_compare模块的日志级别为DEBUG
logging.getLogger('redis_compare').setLevel(logging.DEBUG)

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('production_merge_test.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)





def main():
    """
    主函数 - 执行Redis数据库合并
    """
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='Redis数据库合并工具 - 合并DB1和DB2到DB3')
    parser.add_argument('--host1', default='localhost', help='Redis服务器地址')
    parser.add_argument('--host2', default='localhost', help='Redis服务器地址')
    parser.add_argument('--host3', default='localhost', help='Redis服务器地址')
    parser.add_argument('--port', type=int, default=6379, help='Redis服务器端口')
    parser.add_argument('--db1', type=int, default=0, help='源数据库1编号')
    parser.add_argument('--db2', type=int, default=1, help='源数据库2编号')
    parser.add_argument('--db3', type=int, default=10, help='目标数据库编号')
    parser.add_argument('--preference', default='db2', choices=['db1', 'db2'], help='默认偏好数据库')
    parser.add_argument('--batch-size', type=int, default=100, help='批量处理大小')
    args = parser.parse_args()
    
    try:
        # 配置Redis连接 - 设置decode_responses=False以避免UTF-8解码错误
        config1 = {'host': args.host1, 'port': args.port, 'db': args.db1, 'decode_responses': False}
        config2 = {'host': args.host2, 'port': args.port, 'db': args.db2, 'decode_responses': False}
        config3 = {'host': args.host3, 'port': args.port, 'db': args.db3, 'decode_responses': False}
        
        logger.info("=== Redis数据库合并开始 ===")
        logger.info(f"源数据库1: {args.host1}:{args.port}/{args.db1}")
        logger.info(f"源数据库2: {args.host2}:{args.port}/{args.db2}")
        logger.info(f"目标数据库: {args.host3}:{args.port}/{args.db3}")
        logger.info(f"默认偏好: {args.preference}")
        logger.info(f"批量处理大小: {args.batch_size}")
        
        # 连接DB3并清理
        r3 = connect_redis_with_retry(config3)
        logger.info(f"\n清理目标数据库 DB{args.db3}")
        r3.flushdb()
        
        # 执行合并
        logger.info(f"\n=== 执行合并操作 ===")
        start_time = time.time()
        merge_results = merge_to_new_db(config1, config2, config3, batch_size=args.batch_size, default_preference=args.preference)
        end_time = time.time()
        
        # 打印合并结果
        logger.info(f"\n=== 合并完成 ===")
        logger.info(f"总耗时: {end_time - start_time:.2f} 秒")
        logger.info(f"总处理键数: {merge_results.get('total_keys', 0)}")
        logger.info(f"成功合并键数: {merge_results.get('successfully_merged', 0)}")
        logger.info(f"合并失败键数: {merge_results.get('merge_failed', 0)}")
        
        # 显示合并失败的键（如果有）
        if merge_results.get('merge_failed', 0) > 0 and 'failed_keys' in merge_results:
            logger.error(f"\n合并失败的键 ({len(merge_results['failed_keys'])}个):")
            for key in merge_results['failed_keys']:
                try:
                    # 尝试解码键以便更好地显示
                    if isinstance(key, bytes):
                        decoded_key = key.decode('utf-8', errors='replace')
                    else:
                        decoded_key = str(key)
                    logger.error(f"  {decoded_key}")
                except Exception as e:
                    logger.error(f"  键显示错误: {e}")
        
        logger.info("\n🎉 Redis数据库合并完成！")
        return 0
        
    except Exception as e:
        logger.error(f"合并过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())