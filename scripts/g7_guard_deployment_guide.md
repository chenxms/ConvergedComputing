# G7-2025 触发器守卫白名单部署指南

## 概述

本指南提供G7-2025批次触发器守卫的完整部署和管理方案，支持从全面拦截模式到白名单模式的平滑切换。

## 当前状态分析

### 现有触发器
- `g7_guard_insert` / `g7_guard_update` - 全面拦截模式
- `g7_guard_insert_copy1` / `g7_guard_insert_copy2` - 重复触发器（需清理）
- `g7_guard_update_copy1` / `g7_guard_update_copy2` - 重复触发器（需清理）

### 关键特性
- ✅ 批次代码归一化（支持各种破折号格式）
- ✅ 完整的日志记录（`g7_guard_log`表）
- ✅ 用户和连接信息跟踪
- ❌ 缺少白名单机制
- ❌ 缺少动态配置能力

## 部署方案

### 阶段一：清理环境并验证现状

```bash
# 1. 检查当前触发器状态
python scripts/check_g7_triggers.py

# 2. 查看守卫日志
python -c "
from app.database.connection import get_db
from sqlalchemy import text
with next(get_db()) as db:
    logs = db.execute(text('SELECT * FROM g7_guard_log ORDER BY created_at DESC LIMIT 10')).fetchall()
    for log in logs:
        print(f'{log.created_at}: {log.event} - {log.message}')
"

# 3. 清理重复触发器
python -c "
from app.database.connection import get_db
from sqlalchemy import text
with next(get_db()) as db:
    triggers = ['g7_guard_insert_copy1', 'g7_guard_insert_copy2', 'g7_guard_update_copy1', 'g7_guard_update_copy2']
    for trigger in triggers:
        db.execute(text(f'DROP TRIGGER IF EXISTS {trigger}'))
    db.commit()
    print('✅ 重复触发器已清理')
"
```

### 阶段二：安装白名单守卫系统

```bash
# 1. 安装白名单守卫（会自动清理旧触发器）
python scripts/install_g7_guard_with_whitelist.py

# 2. 验证安装结果
python scripts/g7_guard_switch.py status

# 3. 配置初始白名单
python scripts/manage_g7_whitelist.py list
python scripts/manage_g7_whitelist.py add user g7_pipeline_user "G7流水线专用用户"
python scripts/manage_g7_whitelist.py add application materialize_g7 "G7物化流水线"
```

### 阶段三：测试验证

```bash
# 1. 运行完整测试套件
python scripts/test_g7_guard_whitelist.py

# 2. 手动验证关键功能
# 测试G7写入被拦截
python -c "
from app.database.connection import get_db
from sqlalchemy import text
try:
    with next(get_db()) as db:
        db.execute(text(\"INSERT INTO statistical_aggregations (batch_code, aggregation_level, school_id, region_id, calculated_data) VALUES ('G7-2025', 'school', 'TEST_BLOCK', 'TEST', '{}')\"))
        db.commit()
        print('❌ 写入未被拦截')
except Exception as e:
    if 'blocked by guard' in str(e):
        print('✅ 写入正确被拦截')
    else:
        print(f'❓ 异常: {e}')
"

# 3. 检查日志记录
python scripts/manage_g7_whitelist.py logs 5
```

## 运维管理

### 白名单管理

```bash
# 查看当前白名单
python scripts/manage_g7_whitelist.py list

# 添加新的白名单条目
python scripts/manage_g7_whitelist.py add user new_pipeline_user "新流水线用户"
python scripts/manage_g7_whitelist.py add application pipeline_v13 "V1.3流水线"

# 临时禁用白名单条目
python scripts/manage_g7_whitelist.py disable user old_user

# 删除白名单条目
python scripts/manage_g7_whitelist.py remove user obsolete_user

# 查看守卫统计
python scripts/manage_g7_whitelist.py stats
```

### 守卫模式切换

```bash
# 查看当前状态
python scripts/g7_guard_switch.py status

# 切换到全面拦截模式（紧急情况）
python scripts/g7_guard_switch.py block-all

# 切换到白名单模式（正常运行）
python scripts/g7_guard_switch.py whitelist

# 临时禁用所有守卫（维护窗口）
python scripts/g7_guard_switch.py disable

# 重新启用守卫
python scripts/g7_guard_switch.py enable

# 维护模式管理
python scripts/g7_guard_switch.py maintenance on "计划维护：升级流水线"
python scripts/g7_guard_switch.py maintenance off
```

### 日志监控

```bash
# 查看最近的守卫活动
python scripts/manage_g7_whitelist.py logs 20

# 查看统计信息
python scripts/manage_g7_whitelist.py stats

# 查看特定时间段的活动
python -c "
from app.database.connection import get_db
from sqlalchemy import text
with next(get_db()) as db:
    result = db.execute(text(\"
        SELECT DATE(created_at) as date, action, COUNT(*) as count
        FROM g7_guard_log
        WHERE created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        GROUP BY DATE(created_at), action
        ORDER BY date DESC, action
    \")).fetchall()
    print('近7日守卫活动统计:')
    for row in result:
        print(f'  {row.date}: {row.action} - {row.count}次')
"
```

## 故障排除

### 常见问题

1. **触发器未生效**
   ```bash
   # 检查触发器是否存在
   python scripts/check_g7_triggers.py

   # 重新安装触发器
   python scripts/install_g7_guard_with_whitelist.py
   ```

2. **白名单用户仍被拦截**
   ```bash
   # 检查白名单配置
   python scripts/manage_g7_whitelist.py list

   # 检查用户匹配逻辑
   python -c "
   from app.database.connection import get_db
   from sqlalchemy import text
   with next(get_db()) as db:
       result = db.execute(text('SELECT CURRENT_USER() as user')).fetchone()
       print(f'当前用户: {result.user}')

       matches = db.execute(text(\"
           SELECT value FROM g7_guard_whitelist
           WHERE type = 'user' AND enabled = TRUE
             AND (CURRENT_USER() LIKE CONCAT('%', value, '%') OR
                  SUBSTRING_INDEX(CURRENT_USER(), '@', 1) = value)
       \")).fetchall()
       print(f'匹配的白名单条目: {[m.value for m in matches]}')
   "
   ```

3. **性能问题**
   ```bash
   # 测试触发器性能
   python scripts/test_g7_guard_whitelist.py

   # 查看数据库锁等待
   python -c "
   from app.database.connection import get_db
   from sqlalchemy import text
   with next(get_db()) as db:
       locks = db.execute(text('SHOW PROCESSLIST')).fetchall()
       for lock in locks:
           if 'statistical_aggregations' in str(lock):
               print(f'发现相关进程: {lock}')
   "
   ```

### 紧急操作

1. **完全禁用守卫**
   ```bash
   python scripts/g7_guard_switch.py disable
   ```

2. **快速恢复到全面拦截**
   ```bash
   python scripts/g7_guard_switch.py block-all
   ```

3. **紧急删除所有触发器**
   ```sql
   -- 直接SQL操作
   DROP TRIGGER IF EXISTS g7_guard_insert;
   DROP TRIGGER IF EXISTS g7_guard_update;
   DROP TRIGGER IF EXISTS g7_guard_whitelist_insert;
   DROP TRIGGER IF EXISTS g7_guard_whitelist_update;
   ```

## 最佳实践

### 1. 白名单管理
- 定期审查白名单条目，删除不再需要的条目
- 使用描述字段记录添加原因和有效期
- 为不同的应用使用不同的标识符

### 2. 监控和告警
- 每日检查守卫日志，关注异常活动
- 设置告警监控拦截频率异常
- 定期验证白名单用户的访问情况

### 3. 变更管理
- 在维护窗口进行守卫模式切换
- 切换前后都要运行验证测试
- 保留详细的操作日志

### 4. 安全考虑
- 白名单条目应遵循最小权限原则
- 定期轮换应用标识符
- 监控白名单条目的使用情况

## 部署检查清单

### 安装前检查
- [ ] 确认数据库连接正常
- [ ] 备份现有触发器定义
- [ ] 检查当前守卫日志状态
- [ ] 确认维护窗口时间

### 安装步骤
- [ ] 清理重复触发器
- [ ] 安装白名单守卫系统
- [ ] 配置初始白名单条目
- [ ] 运行验证测试

### 安装后验证
- [ ] 确认触发器正确安装
- [ ] 验证白名单功能正常
- [ ] 检查日志记录功能
- [ ] 确认性能符合预期

### 运维准备
- [ ] 配置监控告警
- [ ] 文档化白名单管理流程
- [ ] 培训运维人员使用工具
- [ ] 建立应急响应流程

## 附录

### 相关文件列表
- `scripts/install_g7_guard_with_whitelist.py` - 白名单守卫安装
- `scripts/manage_g7_whitelist.py` - 白名单管理工具
- `scripts/g7_guard_switch.py` - 守卫模式切换工具
- `scripts/test_g7_guard_whitelist.py` - 验证测试脚本
- `scripts/check_g7_triggers.py` - 触发器状态检查
- `scripts/uninstall_g7_guard.py` - 卸载守卫（仅触发器）
- `quick_remove_g7_guard.sql` - 快速删除触发器SQL

### 数据库表结构
- `g7_guard_whitelist` - 白名单配置表
- `g7_guard_log` - 守卫日志表
- `g7_guard_config` - 守卫配置表（可选）
- `statistical_aggregations` - 受保护的主数据表