# G7-2025 增强守卫系统部署和管理指南

## 概述

G7-2025增强守卫系统是一个数据库级别的保护机制，用于防止意外修改G7-2025批次的统计汇聚数据。系统提供了以下功能：

- **智能阻断**: 自动识别并阻断G7-2025相关的INSERT/UPDATE操作
- **白名单机制**: 允许特定用户绕过保护
- **维护模式**: 在维护窗口期间临时解除保护
- **详细日志**: 记录所有守卫操作和决策过程
- **批次代码标准化**: 自动处理各种破折号变体

## 快速开始

### 1. 安装增强守卫系统

```bash
cd /path/to/ConvergedComputing
python install_g7_enhanced_guard.py
```

### 2. 验证安装

```bash
python test_g7_guard_system.py
```

### 3. 测试维护模式和白名单

```bash
python test_maintenance_mode.py
```

## 管理操作

### 使用增强守卫管理器（推荐）

```bash
# 查看状态
python scripts/enhanced_g7_guard.py status

# 安装守卫
python scripts/enhanced_g7_guard.py install

# 卸载守卫
python scripts/enhanced_g7_guard.py uninstall

# 添加白名单用户
python scripts/enhanced_g7_guard.py add-whitelist "admin%"

# 移除白名单用户
python scripts/enhanced_g7_guard.py remove-whitelist "admin%"

# 启用维护模式
python scripts/enhanced_g7_guard.py enable-maintenance

# 禁用维护模式
python scripts/enhanced_g7_guard.py disable-maintenance

# 验证触发器完整性
python scripts/enhanced_g7_guard.py validate
```

### 使用维护窗口管理器

```bash
# 开始计划维护窗口（60分钟）
python scripts/maintenance_window_manager.py start --duration 60

# 开始紧急维护窗口
python scripts/maintenance_window_manager.py start --emergency

# 延长维护窗口30分钟
python scripts/maintenance_window_manager.py extend --minutes 30

# 结束维护窗口
python scripts/maintenance_window_manager.py stop

# 查看维护状态
python scripts/maintenance_window_manager.py status

# 查看维护历史
python scripts/maintenance_window_manager.py schedule
```

### 使用回滚管理器

```bash
# 备份当前配置
python scripts/g7_guard_rollback.py backup

# 回滚到安装前状态
python scripts/g7_guard_rollback.py rollback

# 紧急解锁（移除所有限制）
python scripts/g7_guard_rollback.py emergency

# 从备份恢复配置
python scripts/g7_guard_rollback.py restore

# 查看状态
python scripts/g7_guard_rollback.py status

# 完全清理所有守卫数据
python scripts/g7_guard_rollback.py clean
```

## 系统架构

### 数据库表结构

#### g7_enhanced_guard_log
记录所有守卫操作的详细日志：
- 事件类型和操作动作
- 决策结果（ALLOWED/BLOCKED）
- 用户信息和连接详情
- 维护模式和白名单状态
- 执行时间统计

#### g7_guard_whitelist
白名单用户管理：
- 用户模式匹配（支持通配符）
- 激活状态控制
- 添加者和备注信息

#### g7_guard_config
系统配置管理：
- maintenance_mode: 维护模式开关
- guard_enabled: 守卫启用状态
- log_retention_days: 日志保留天数

### 触发器设计

#### g7_enhanced_guard_insert / g7_enhanced_guard_update
- **批次代码标准化**: 自动处理各种破折号变体（–、−、—）
- **维护模式检查**: 从配置表读取维护模式状态
- **白名单验证**: 检查当前用户是否在白名单中
- **决策记录**: 详细记录决策过程和执行时间
- **智能阻断**: 只在必要时抛出异常

## 操作场景

### 场景1：日常G7数据保护

正常情况下，守卫系统会自动阻断任何对G7-2025数据的修改：

```sql
-- 这个操作会被阻断
INSERT INTO statistical_aggregations (batch_code, aggregation_level, school_id, ...)
VALUES ('G7-2025', 'SCHOOL', 'test_school', ...);
-- Error: G7-2025 writes blocked by enhanced guard

-- 这个操作会被允许
INSERT INTO statistical_aggregations (batch_code, aggregation_level, school_id, ...)
VALUES ('G8-2025', 'SCHOOL', 'test_school', ...);
-- Success
```

### 场景2：紧急维护操作

当需要紧急修复G7数据时：

```bash
# 启动紧急维护
python scripts/maintenance_window_manager.py start --emergency

# 执行数据修复操作
python fix_g7_data.py

# 结束维护窗口
python scripts/maintenance_window_manager.py stop
```

### 场景3：特定用户授权

为特定用户（如数据管理员）添加永久白名单：

```bash
# 添加管理员到白名单
python scripts/enhanced_g7_guard.py add-whitelist "data_admin%"

# 该用户现在可以直接操作G7数据
# 其他用户仍然被保护
```

### 场景4：计划维护窗口

进行定期数据维护：

```bash
# 备份当前状态
python scripts/g7_guard_rollback.py backup

# 启动2小时维护窗口
python scripts/maintenance_window_manager.py start --duration 120

# 执行维护操作
python run_maintenance_tasks.py

# 如需延长时间
python scripts/maintenance_window_manager.py extend --minutes 30

# 完成后结束维护
python scripts/maintenance_window_manager.py stop
```

### 场景5：问题排查和回滚

当发现问题需要快速回滚：

```bash
# 紧急解锁（移除所有保护）
python scripts/g7_guard_rollback.py emergency

# 执行问题修复
python emergency_fix.py

# 从备份恢复正常状态
python scripts/g7_guard_rollback.py restore
```

## 监控和告警

### 日志查询

```sql
-- 查看最近的阻断事件
SELECT event, action, decision, user_host, message, created_at
FROM g7_enhanced_guard_log
WHERE decision = 'BLOCKED'
AND created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
ORDER BY created_at DESC;

-- 查看维护模式使用情况
SELECT action, message, created_at
FROM g7_enhanced_guard_log
WHERE event = 'MAINTENANCE'
ORDER BY created_at DESC
LIMIT 10;

-- 查看白名单使用统计
SELECT is_whitelisted, COUNT(*) as count
FROM g7_enhanced_guard_log
WHERE batch_code = 'G7-2025'
AND created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
GROUP BY is_whitelisted;
```

### 性能监控

```sql
-- 查看触发器执行时间
SELECT AVG(execution_time_ms) as avg_time,
       MAX(execution_time_ms) as max_time,
       COUNT(*) as total_triggers
FROM g7_enhanced_guard_log
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR);
```

## 安全最佳实践

### 1. 权限控制
- 限制对守卫管理脚本的访问权限
- 使用专门的数据库用户执行守卫操作
- 定期审查白名单用户

### 2. 操作审计
- 所有守卫操作都会记录详细日志
- 维护窗口需要有明确的开始和结束记录
- 白名单变更需要记录操作人员和原因

### 3. 备份策略
- 执行重要操作前必须备份当前配置
- 定期备份守卫日志数据
- 保留多个版本的配置备份

### 4. 应急预案
- 准备紧急解锁程序
- 建立快速回滚机制
- 制定问题升级流程

## 故障排除

### 常见问题

#### 1. 触发器未正确安装
```bash
# 检查触发器状态
python scripts/enhanced_g7_guard.py validate

# 重新安装
python scripts/enhanced_g7_guard.py uninstall
python scripts/enhanced_g7_guard.py install
```

#### 2. 维护模式无法关闭
```bash
# 强制关闭维护模式
python -c "
from sqlalchemy import text
from app.database.connection import get_db
with next(get_db()) as db:
    db.execute(text(\"UPDATE g7_guard_config SET config_value='false' WHERE config_key='maintenance_mode'\"))
    db.commit()
    print('Maintenance mode force disabled')
"
```

#### 3. 紧急情况下无法操作
```bash
# 紧急解锁所有限制
python scripts/g7_guard_rollback.py emergency
```

#### 4. 白名单用户仍被阻断
```bash
# 检查白名单配置
python -c "
from sqlalchemy import text
from app.database.connection import get_db
with next(get_db()) as db:
    result = db.execute(text('SELECT * FROM g7_guard_whitelist WHERE is_active=TRUE'))
    for row in result:
        print(f'Pattern: {row[1]}, Added by: {row[2]}')
"
```

### 日志分析

查看详细的决策过程：

```sql
SELECT
    created_at,
    action,
    user_host,
    is_whitelisted,
    maintenance_mode,
    decision,
    message
FROM g7_enhanced_guard_log
WHERE batch_code = 'G7-2025'
ORDER BY created_at DESC
LIMIT 20;
```

## 版本升级

### 从基础守卫升级到增强守卫

```bash
# 1. 备份当前状态
python scripts/g7_guard_rollback.py backup

# 2. 卸载旧守卫
python scripts/uninstall_g7_guard.py

# 3. 安装增强守卫
python install_g7_enhanced_guard.py

# 4. 验证安装
python test_g7_guard_system.py
```

## 联系信息

如遇到问题，请联系：
- 系统管理员
- 数据库管理员
- 开发团队

---

**注意**: 本文档描述的所有操作都会影响生产数据的访问控制，请务必在执行前充分了解操作的影响范围。