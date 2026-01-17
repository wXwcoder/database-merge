// setup_table_shard.js
/**
 * MongoDB 分片集群初始化脚本（简化版）
 * 功能：
 * 1. 为指定表创建 {id: "hashed"} 索引（分片必需）
 * 2. 启用哈希分片
 * 3. 验证分片状态
 * 
 * 注意：此简化版仅创建分片必需的哈希索引，业务索引可在分片后单独创建
 * 使用：mongosh --file setup_table_shard.js
 * 使用：mongosh 交互界面执行 load("/root/setup_table_shard.js")
 *  db.ug_device1.getIndexes()
 * db.ug_device.getIndexes()
 * sh.status()
 * db.ug_user.getShardDistribution()
 * db.ug_user.stats()
 * 
 */

// ==================== 配置区 ====================
const CONFIG = {
  // MongoDB 连接信息（如果在命令行中提供，这里可以不填）
  connectionString: "", // 留空则需要在命令行中提供
  
  // 目标数据库
  database: "xsdk_v2_test",
  
  // 需要分片的表配置（仅保留分片必需的索引）
  collections: [
     {
       name: "ug_user",
       description: "用户表",
       shardKey: {name: "hashed"}, //ug_user 使用id作为分片键，保持良好的数据分布
       indexes: [
         // 普通索引：name (注意：在分片集群中，由于分片键是id，name字段的唯一性需由应用层保证)
         {
           key: {name: "hashed"},
           options: {
             unique: true,
             name: "idx_name"
           }
         },
         // 单字段索引：id (原分片键)
         {
           key: {id: 1},
           options: {
             name: "idx_id"
           }
         },
         // 单字段索引：appID
         {
           key: {appID: 1},
           options: {
             name: "idx_appID"
           }
         },
         // 单字段索引：phoneNum
         {
           key: {phoneNum: 1},
           options: {
             name: "idx_phoneNum"
           }
         },
         // 单字段索引：lastLoginTime
         {
           key: {lastLoginTime: 1},
           options: {
             name: "idx_lastLoginTime"
           }
         },
         // 单字段索引：createTime
         {
           key: {createTime: 1},
           options: {
             name: "idx_createTime"
           }
         },
         // 单字段索引：accountType
         {
           key: {accountType: 1},
           options: {
             name: "idx_accountType"
           }
         },
         // 单字段索引：idCard
         {
           key: {idCard: 1},
           options: {
             name: "idx_idCard"
           }
         }
       ] // 分片时仅需要哈希索引，业务索引可在分片后单独创建
     },
    {
        name: "ug_id_card",
        description: "身份证表",
        shardKey: {idCard: 1}, //ug_id_card 使用idCard作为分片键，确保唯一索引有效
        indexes: [
            // 唯一索引：idCard（分片键必须包含唯一索引字段）
            {
                key: {idCard: 1},
                options: {
                    unique: true,
                    name: "idx_idCard"
                }
            }
        ] // 分片时仅需要哈希索引，业务索引可在分片后单独创建
    },
    {
      name: "ug_device",
      description: "设备表",
      shardKey: {appID: 1, deviceID: 1},  //ug_device 改为使用简单的 _id 哈希分片
      indexes: [
        // 唯一复合索引：appID + deviceID
        {
          key: {appID: 1, deviceID: 1},
          options: {
            unique: true,
            name: "idx_deviceID"
          }
        },
        // 单字段索引：oaid
        {
          key: {oaid: 1},
          options: {
            name: "idx_oaid"
          }
        },
        // 单字段索引：imei
        {
          key: {imei: 1},
          options: {
            name: "idx_imei"
          }
        },
        // 单字段索引：idfa
        {
          key: {idfa: 1},
          options: {
            name: "idx_idfa"
          }
        },
        // 时间字段索引：createTime
        {
          key: {createTime: 1},
          options: {
            name: "idx_createTime"
          }
        }
      ]
    },
    {
        name: "ug_order_notify_log",
        description: "订单通知日志表",
        shardKey: {orderID: 1}, //ug_order_notify_log 使用idCard作为分片键，确保唯一索引有效
        indexes: [
            {
                key: {orderID: 1},
                options: {
                    name: "idx_orderID"
                }
            }
        ] // 分片时仅需要哈希
    },
    {
        name: "ug_order_platform_log",
        description: "订单平台日志表",
        shardKey: {orderID: 1}, //ug_order_platform_log 使用idCard作为分片键，确保唯一索引有效
        indexes: [
            {
                key: {orderID: 1},
                options: {
                    name: "idx_orderID"
                }
            }
        ] // 分片时仅需要哈希
    },
    {
        name: "ug_game_user",
        description: "游戏用户表",
        shardKey: {uid: 1}, //ug_game_user 使用uid作为分片键，确保唯一索引有效
        indexes: [
            // 单字段索引：uid
            {
                key: {uid: 1},
                options: {
                    name: "idx_uid"
                }
            },
            // 单字段索引：appID
            {
                key: {appID: 1},
                options: {
                    name: "idx_appID"
                }
            },
            // 单字段索引：name
            {
                key: {name: 1},
                options: {
                    name: "idx_name"
                }
            },
            // 单字段索引：lastLoginTime
            {
                key: {lastLoginTime: 1},
                options: {
                    name: "idx_lastLoginTime"
                }
            },
            // 单字段索引：accountType
            {
                key: {accountType: 1},
                options: {
                    name: "idx_accountType"
                }
            }
        ] // 分片时仅需要哈希
    },
    {
        name: "ug_order",
        description: "订单表",
        shardKey: {uid: 1}, //ug_order 使用uid作为分片键，确保良好的数据分布
        indexes: [
            // 分片必需索引：uid (哈希索引)
            {
                key: {uid: 1},
                options: {
                    name: "idx_uid"
                }
            },
            // 业务索引：id (主键)
            {
                key: {id: 1},
                options: {
                    name: "idx_id"
                }
            },
            // 业务索引：platformOrderID (平台订单ID)
            {
                key: {platformOrderID: 1},
                options: {
                    name: "idx_platformOrderID"
                }
            },
            // 业务索引：appID (应用ID)
            {
                key: {appID: 1},
                options: {
                    name: "idx_appID"
                }
            },
            // 业务索引：payType (支付类型)
            {
                key: {payType: 1},
                options: {
                    name: "idx_payType"
                }
            },
            // 业务索引：roleID (角色ID)
            {
                key: {roleID: 1},
                options: {
                    name: "idx_roleID"
                }
            },
            // 业务索引：serverID (服务器ID)
            {
                key: {serverID: 1},
                options: {
                    name: "idx_serverID"
                }
            },
            // 业务索引：createTime (创建时间)
            {
                key: {createTime: 1},
                options: {
                    name: "idx_createTime"
                }
            },
            // 复合索引：uid + createTime (用户订单时间查询)
            {
                key: {uid: 1, createTime: -1},
                options: {
                    name: "idx_uid_createTime"
                }
            },
            // 复合索引：appID + createTime (应用订单时间查询)
            {
                key: {appID: 1, createTime: -1},
                options: {
                    name: "idx_appID_createTime"
                }
            }
        ]
    },
    {
        name: "ug_login_log",
        description: "登录日志表",
        shardKey: {userID: 1}, //ug_login_log 使用userID作为分片键，确保良好的数据分布
        indexes: [
            // userID字段索引
            {
                key: {userID: 1},
                options: {
                    name: "idx_userID"
                }
            },
            // appID字段索引
            {
                key: {appID: 1},
                options: {
                    name: "idx_appID"
                }
            },
            
            // loginTime字段索引（支持时间范围查询）
            {
                key: {loginTime: 1},
                options: {
                    name: "idx_loginTime"
                }
            }
        ]
    }

  ],
  
  // 分片设置
  sharding: {
    initialChunks: 8,        // 初始分块数量
    chunkSizeMB: 64,         // 分块大小（MB）
    enableBalancer: true     // 是否启用平衡器
  },
  
  // 执行选项
  options: {
    skipExistingSharded: true,  // 跳过已分片的表
    backgroundIndex: true,      // 后台创建索引
    validateResults: true,      // 验证结果
    dryRun: false              // 试运行，不实际执行
  }
};

// ==================== 日志和工具函数 ====================
class Logger {
  constructor(verbose = true) {
    this.verbose = verbose;
    this.startTime = Date.now();
  }
  
  info(message) {
    const timestamp = new Date().toISOString().substring(11, 19);
    print(`[${timestamp}] ℹ️  ${message}`);
  }
  
  success(message) {
    const timestamp = new Date().toISOString().substring(11, 19);
    print(`[${timestamp}] ✅ ${message}`);
  }
  
  warning(message) {
    const timestamp = new Date().toISOString().substring(11, 19);
    print(`[${timestamp}] ⚠️  ${message}`);
  }
  
  error(message) {
    const timestamp = new Date().toISOString().substring(11, 19);
    print(`[${timestamp}] ❌ ${message}`);
  }
  
  section(title) {
    print(`\n${'='.repeat(60)}`);
    print(`📋 ${title}`);
    print(`${'='.repeat(60)}`);
  }
  
  divider() {
    print(`${'-'.repeat(60)}`);
  }
  
  getElapsedTime() {
    const elapsed = ((Date.now() - this.startTime) / 1000).toFixed(2);
    return `${elapsed}秒`;
  }
}

// 工具函数
function sleep(ms) {
  const start = Date.now();
  while (Date.now() - start < ms) {
    // 空循环等待
  }
}

function formatBytes(bytes, decimals = 2) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(decimals)) + ' ' + sizes[i];
}

// ==================== 核心功能函数 ====================
class ShardingManager {
  constructor(config, logger) {
    this.config = config;
    this.logger = logger;
    this.results = {
      success: [],
      skipped: [],
      failed: []
    };
  }
  
  // 主执行函数
  async execute() {
    this.logger.section(`开始 MongoDB 分片设置`);
    this.logger.info(`数据库: ${this.config.database}`);
    this.logger.info(`目标表: ${this.config.collections.map(c => c.name).join(', ')}`);
    this.logger.info(`模式: ${this.config.options.dryRun ? '试运行' : '实际执行'}`);
    
    try {
      // 步骤1: 检查连接和权限
      await this.validateConnection();
      
      // 步骤2: 启用数据库分片
      await this.enableDatabaseSharding();
      
      // 步骤3: 处理每个表
      for (const collConfig of this.config.collections) {
        await this.processCollection(collConfig);
      }
      
      // 步骤4: 验证结果
      await this.validateResults();
      
      // 步骤5: 显示摘要
      this.showSummary();
      
    } catch (error) {
      this.logger.error(`执行失败: ${error.message}`);
      throw error;
    }
  }
  
  // 步骤1: 验证连接和权限
  async validateConnection() {
    this.logger.info("验证 MongoDB 连接和权限...");
    
    try {
      // 检查是否连接到 mongos
      const hello = db.runCommand({hello: 1});
      if (hello.msg !== "isdbgrid") {
        this.logger.warning("可能未连接到 mongos，当前连接类型: " + hello.msg);
      }
      
      // 检查权限
      const connectionStatus = db.runCommand({connectionStatus: 1});
      const user = connectionStatus.authInfo.authenticatedUsers[0];
      
      if (user) {
        this.logger.success(`已连接用户: ${user.user}@${user.db}`);
        
        // 检查是否有关键权限
        const userInfo = db.adminCommand({
          usersInfo: {user: user.user, db: user.db},
          showPrivileges: true
        });
        
        const hasShardPerm = userInfo.users[0].roles.some(role => 
          role.role === "clusterAdmin" || 
          role.role === "clusterManager"
        );
        
        if (!hasShardPerm) {
          this.logger.warning("用户可能缺少分片管理权限");
        }
      }
      
      this.logger.success("连接验证通过");
    } catch (error) {
      throw new Error(`连接验证失败: ${error.message}`);
    }
  }
  
  // 步骤2: 启用数据库分片
  async enableDatabaseSharding() {
    this.logger.info(`启用数据库分片: ${this.config.database}`);
    
    if (this.config.options.dryRun) {
      this.logger.info("[试运行] 跳过实际执行");
      return;
    }
    
    try {
      const result = sh.enableSharding(this.config.database);
      
      if (result.ok === 1) {
        this.logger.success("数据库分片启用成功");
      } else {
        throw new Error(`启用失败: ${JSON.stringify(result)}`);
      }
    } catch (error) {
      if (error.message.includes('already enabled')) {
        this.logger.info("数据库分片已启用");
      } else {
        throw error;
      }
    }
  }
  
  // 步骤3: 处理单个表
  async processCollection(collConfig) {
    this.logger.divider();
    this.logger.info(`处理表: ${collConfig.name} (${collConfig.description})`);
    
    const fullName = `${this.config.database}.${collConfig.name}`;
    
    try {
      // 切换到目标数据库
      use(this.config.database);
      
      // 检查表是否存在
      const collectionExists = await this.checkCollectionExists(collConfig.name);
      if (!collectionExists) {
        this.logger.warning(`表 ${collConfig.name} 不存在，将创建`);
        if (!this.config.options.dryRun) {
          db.createCollection(collConfig.name);
          this.logger.success("表创建成功");
        }
      }
      
      // 检查是否已分片
      const isAlreadySharded = await this.checkIfSharded(fullName);
      if (isAlreadySharded && this.config.options.skipExistingSharded) {
        this.logger.info(`表已分片，跳过`);
        this.results.skipped.push(collConfig.name);
        return;
      }
      
      // 创建索引
      await this.createIndexes(collConfig);
      
      // 创建分片键索引（分片必需）
      await this.createShardKeyIndex(collConfig);
      
      // 启用分片
      await this.enableCollectionSharding(collConfig);
      
      // 预先分割分块
      await this.preSplitChunks(collConfig);
      
      this.results.success.push(collConfig.name);
      this.logger.success(`表 ${collConfig.name} 处理完成`);
      
    } catch (error) {
      this.logger.error(`处理表 ${collConfig.name} 失败: ${error.message}`);
      this.results.failed.push({
        collection: collConfig.name,
        error: error.message
      });
    }
  }
  
  // 检查表是否存在
  async checkCollectionExists(collectionName) {
    const collectionNames = db.getCollectionNames();
    return collectionNames.includes(collectionName);
  }
  
  // 检查是否已分片
  async checkIfSharded(fullCollectionName) {
    const configDB = db.getSiblingDB('config');
    const collConfig = configDB.collections.findOne({_id: fullCollectionName});
    return collConfig && collConfig.key;
  }
  
  // 创建索引
  async createIndexes(collConfig) {
    if (!collConfig.indexes || collConfig.indexes.length === 0) {
      return;
    }
    
    this.logger.info(`创建业务索引 (${collConfig.indexes.length} 个)`);
    
    const coll = db[collConfig.name];
    const existingIndexes = coll.getIndexes();
    
    for (const [index, idxConfig] of collConfig.indexes.entries()) {
      // 检查索引是否已存在
      const exists = existingIndexes.some(existing => 
        JSON.stringify(existing.key) === JSON.stringify(idxConfig.key)
      );
      
      if (exists) {
        this.logger.info(`  索引 ${index + 1}. ${JSON.stringify(idxConfig.key)} (已存在)`);
        continue;
      }
      
      if (this.config.options.dryRun) {
        this.logger.info(`  [试运行] 创建索引: ${JSON.stringify(idxConfig.key)}`);
        continue;
      }
      
      try {
        const options = {
          ...idxConfig.options,
          background: this.config.options.backgroundIndex
        };
        
        coll.createIndex(idxConfig.key, options);
        this.logger.success(`  索引 ${index + 1}. ${JSON.stringify(idxConfig.key)} 创建成功`);
        
        // 如果是后台创建，等待一下
        if (this.config.options.backgroundIndex) {
          sleep(100);
        }
      } catch (error) {
        this.logger.warning(`  索引 ${index + 1}. ${JSON.stringify(idxConfig.key)} 创建失败: ${error.message}`);
      }
    }
  }
  
  // 创建分片键索引（_id 哈希索引）
  async createShardKeyIndex(collConfig) {
    this.logger.info(`创建分片键索引（_id 哈希）: ${JSON.stringify(collConfig.shardKey)}`);
    
    const coll = db[collConfig.name];
    const existingIndexes = coll.getIndexes();
    
    // 检查是否已有分片键索引
    const hasShardKeyIndex = existingIndexes.some(idx => 
      JSON.stringify(idx.key) === JSON.stringify(collConfig.shardKey)
    );
    
    if (hasShardKeyIndex) {
      this.logger.info(`  分片键索引已存在`);
      return;
    }
    
    if (this.config.options.dryRun) {
      this.logger.info(`  [试运行] 创建 _id 哈希索引`);
      return;
    }
    
    try {
      const options = {
        background: this.config.options.backgroundIndex
      };
      
      coll.createIndex(collConfig.shardKey, options);
      this.logger.success(`  _id 哈希索引创建成功`);
      
      // 等待索引创建完成
      if (this.config.options.backgroundIndex) {
        this.logger.info(`  等待 _id 哈希索引创建完成...`);
        sleep(3000); // 等待3秒
      }
    } catch (error) {
      throw new Error(`创建 _id 哈希索引失败: ${error.message}`);
    }
  }
  
  // 启用表分片
  async enableCollectionSharding(collConfig) {
    const fullName = `${this.config.database}.${collConfig.name}`;
    
    this.logger.info(`启用表分片`);
    this.logger.info(`  分片键: ${JSON.stringify(collConfig.shardKey)}`);
    
    if (this.config.options.dryRun) {
      this.logger.info(`  [试运行] 启用分片: ${fullName}`);
      return;
    }
    
    try {
      const result = sh.shardCollection(fullName, collConfig.shardKey);
      
      if (result.ok === 1) {
        this.logger.success(`  分片启用成功`);
      } else {
        throw new Error(`分片失败: ${JSON.stringify(result)}`);
      }
    } catch (error) {
      if (error.message.includes('already sharded')) {
        this.logger.info(`  表已分片`);
      } else if (error.message.includes('create an index')) {
        this.logger.warning(`  分片失败: 需要先创建索引`);
        throw error;
      } else {
        throw error;
      }
    }
  }
  
  // 预先分割分块
  async preSplitChunks(collConfig) {
    const fullName = `${this.config.database}.${collConfig.name}`;
    
    this.logger.info(`预先分割分块 (目标: ${this.config.sharding.initialChunks} 个)`);
    
    if (this.config.options.dryRun) {
      this.logger.info(`  [试运行] 分割分块`);
      return;
    }
    
    const configDB = db.getSiblingDB('config');
    const currentChunks = configDB.chunks.countDocuments({ns: fullName});
    
    if (currentChunks >= this.config.sharding.initialChunks) {
      this.logger.info(`  当前已有 ${currentChunks} 个分块，满足要求`);
      return;
    }
    
    const chunksNeeded = this.config.sharding.initialChunks - currentChunks;
    this.logger.info(`  需要创建 ${chunksNeeded} 个新分块`);
    
    let created = 0;
    for (let i = 0; i < chunksNeeded; i++) {
      try {
        // 使用复合哈希分片键的分割点
        // 对于复合哈希索引，使用不同的 appID 值来分割
        sh.splitFind(fullName, {appID: i, deviceID: "split_point_" + i});
        created++;
        this.logger.info(`    分割 ${created}/${chunksNeeded} 完成`);
        sleep(50); // 短暂延迟
      } catch (error) {
        if (error.message.includes('chunk is too small')) {
          this.logger.info(`    分块太小，停止分割`);
          break;
        }
        this.logger.warning(`    分割 ${i + 1} 失败: ${error.message}`);
      }
    }
    
    if (created > 0) {
      this.logger.success(`  成功分割 ${created} 个分块`);
    }
  }
  
  // 步骤4: 验证结果
  async validateResults() {
    if (!this.config.options.validateResults) {
      return;
    }
    
    this.logger.section("验证分片设置结果");
    
    const configDB = db.getSiblingDB('config');
    
    for (const collConfig of this.config.collections) {
      const fullName = `${this.config.database}.${collConfig.name}`;
      
      this.logger.divider();
      this.logger.info(`验证表: ${collConfig.name}`);
      
      try {
        // 检查索引
        await this.validateIndexes(collConfig);
        
        // 检查分片配置
        await this.validateSharding(fullName, configDB);
        
        // 检查数据分布
        await this.validateDataDistribution(collConfig);
        
      } catch (error) {
        this.logger.warning(`验证失败: ${error.message}`);
      }
    }
  }
  
  // 验证索引
  async validateIndexes(collConfig) {
    use(this.config.database);
    const coll = db[collConfig.name];
    const indexes = coll.getIndexes();
    
    this.logger.info(`索引总数: ${indexes.length}`);
    
    // 检查是否有分片键索引
    const shardKeyIndex = indexes.find(idx => 
      JSON.stringify(idx.key) === JSON.stringify(collConfig.shardKey)
    );
    if (shardKeyIndex) {
      this.logger.success(`  分片键索引: 存在 ✅`);
    } else {
      this.logger.error(`  分片键索引: 缺失 ❌`);
    }
    
    // 显示所有索引
    if (this.logger.verbose) {
      indexes.forEach((idx, i) => {
        this.logger.info(`  ${i + 1}. ${idx.name}: ${JSON.stringify(idx.key)}`);
      });
    }
  }
  
  // 验证分片配置
  async validateSharding(fullName, configDB) {
    const collConfig = configDB.collections.findOne({_id: fullName});
    
    if (collConfig && collConfig.key) {
      this.logger.success(`  分片配置: 已启用 ✅`);
      this.logger.info(`  分片键: ${JSON.stringify(collConfig.key)}`);
    } else {
      this.logger.error(`  分片配置: 未启用 ❌`);
    }
    
    // 检查分块
    const chunkCount = configDB.chunks.countDocuments({ns: fullName});
    this.logger.info(`  分块数量: ${chunkCount}`);
    
    // 检查分块分布
    const chunksByShard = {};
    configDB.chunks.find({ns: fullName}).forEach(chunk => {
      chunksByShard[chunk.shard] = (chunksByShard[chunk.shard] || 0) + 1;
    });
    
    this.logger.info(`  分块分布: ${JSON.stringify(chunksByShard)}`);
  }
  
  // 验证数据分布
  async validateDataDistribution(collConfig) {
    use(this.config.database);
    const coll = db[collConfig.name];
    
    try {
      const stats = coll.stats();
      
      if (stats.sharded) {
        this.logger.success(`  数据状态: 已分片 ✅`);
        this.logger.info(`  文档数: ${stats.count.toLocaleString()}`);
        this.logger.info(`  数据大小: ${formatBytes(stats.size)}`);
        
        if (stats.shards) {
          for (const shardName in stats.shards) {
            const shardStats = stats.shards[shardName];
            const percentage = ((shardStats.size / stats.size) * 100).toFixed(1);
            this.logger.info(`    ${shardName}: ${formatBytes(shardStats.size)} (${percentage}%)`);
          }
        }
      } else {
        this.logger.info(`  数据状态: 未分片`);
      }
    } catch (error) {
      this.logger.warning(`  无法获取数据统计: ${error.message}`);
    }
  }
  
  // 步骤5: 显示摘要
  showSummary() {
    this.logger.section("执行摘要");
    
    const total = this.config.collections.length;
    const success = this.results.success.length;
    const skipped = this.results.skipped.length;
    const failed = this.results.failed.length;
    
    this.logger.info(`总表数: ${total}`);
    this.logger.success(`成功: ${success}`);
    this.logger.info(`跳过: ${skipped}`);
    
    if (failed > 0) {
      this.logger.error(`失败: ${failed}`);
      this.logger.divider();
      this.logger.info("失败详情:");
      this.results.failed.forEach(fail => {
        this.logger.error(`  ${fail.collection}: ${fail.error}`);
      });
    }
    
    this.logger.divider();
    this.logger.info(`总耗时: ${this.logger.getElapsedTime()}`);
    
    if (failed === 0) {
      this.logger.success("🎉 所有任务执行完成！");
    } else {
      this.logger.warning("⚠️  部分任务执行失败，请检查错误信息");
    }
    
    // 显示后续建议
    this.showRecommendations();
  }
  
  // 显示建议
  showRecommendations() {
    if (this.config.options.dryRun) {
      return;
    }
    
    this.logger.section("后续建议");
    
    this.logger.info("1. 监控分片状态:");
    this.logger.info("   命令: sh.status()");
    
    this.logger.info("\n2. 检查平衡器状态:");
    this.logger.info("   命令: db.adminCommand({balancerStatus: 1})");
    
    if (this.config.sharding.enableBalancer) {
      this.logger.info("\n3. 启动平衡器（如果未运行）:");
      this.logger.info("   命令: db.adminCommand({balancerStart: 1})");
    }
    
    this.logger.info("\n4. 查看表的分片分布:");
    this.config.collections.forEach(coll => {
      this.logger.info(`   命令: db.${coll.name}.getShardDistribution()`);
    });
    
    this.logger.info("\n5. 测试查询性能:");
    this.config.collections.forEach(coll => {
      this.logger.info(`   命令: db.${coll.name}.find({id: "test_id"}).explain('executionStats')`);
    });
  }
}

// ==================== 执行入口 ====================
async function main() {
  const logger = new Logger(true);
  
  try {
    logger.section("MongoDB 分片设置脚本");
    logger.info("版本: 1.0.0");
    logger.info("作者: MongoDB 管理工具");
    logger.info("开始时间: " + new Date().toISOString());
    
    // 创建分片管理器
    const manager = new ShardingManager(CONFIG, logger);
    
    // 执行分片设置
    await manager.execute();
    
  } catch (error) {
    logger.error("脚本执行失败: " + error.message);
    process.exit(1);
  }
}

// 自动执行
if (typeof process !== 'undefined') {
  // 解析命令行参数
  const args = process.argv.slice(2);
  const params = {};
  
  for (let i = 0; i < args.length; i += 2) {
    if (args[i].startsWith('--')) {
      const key = args[i].slice(2);
      const value = args[i + 1];
      
      switch (key) {
        case 'dry-run':
          CONFIG.options.dryRun = value === 'true';
          break;
        case 'database':
          CONFIG.database = value;
          break;
        case 'chunks':
          CONFIG.sharding.initialChunks = parseInt(value);
          break;
        // 可以添加更多参数解析
      }
    }
  }
  
  // 执行主函数
  main();
} else {
  // 在 mongosh 交互环境中
  print("请在命令行中使用: mongosh --file setup_sharding_v2.js");
}