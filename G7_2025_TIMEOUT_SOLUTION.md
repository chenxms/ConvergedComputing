# G7-2025批次超时问题解决方案

## 问题描述
- **现象**：访问 `/api/v12/batch/G7-2025/regional` 超时
- **原因**：G7-2025数据量大，实时计算耗时过长
- **表现**：前端报404（实际是超时）

## 立即解决方案

### 1. 前端增加超时时间
```javascript
// subjectsV12.js
const getRegionalSubjects = async (batchCode) => {
  try {
    const response = await axios.get(
      `${API_BASE}/api/v12/batch/${batchCode}/regional`,
      {
        timeout: 60000,  // 增加到60秒
        headers: {
          'Content-Type': 'application/json'
        }
      }
    );
    return response.data;
  } catch (error) {
    if (error.code === 'ECONNABORTED') {
      console.error('请求超时，数据量较大，请稍后重试');
      // 可以提示用户数据正在准备中
    }
    throw error;
  }
};
```

### 2. 先物化数据（推荐）

#### 步骤1：调用物化接口
```bash
# 手动触发物化
curl -X POST http://117.72.14.166:8000/api/v12/batch/G7-2025/materialize

# 或前端调用
await axios.post(`${API_BASE}/api/v12/batch/G7-2025/materialize`);
```

#### 步骤2：等待物化完成后查询
物化会将计算结果缓存到数据库，后续查询会很快

### 3. 使用异步模式

前端可以实现异步加载：
```javascript
// 1. 先尝试获取
// 2. 如果超时，显示"数据准备中"
// 3. 触发物化
// 4. 轮询检查状态

async function loadG7Data() {
  // 显示加载中
  showLoading('正在加载数据，首次加载可能需要较长时间...');

  try {
    // 尝试获取（短超时）
    const data = await getRegionalSubjects('G7-2025');
    return data;
  } catch (error) {
    if (isTimeout(error)) {
      // 触发物化
      showLoading('数据准备中，请稍候...');
      await materializeData('G7-2025');

      // 重试获取
      return await getRegionalSubjects('G7-2025');
    }
    throw error;
  }
}
```

## 后端优化建议

### 1. 添加缓存机制
```python
# 在subjects_v12_api.py中添加缓存
from functools import lru_cache
import hashlib

@lru_cache(maxsize=128)
def get_cached_regional_data(batch_code: str):
    # 缓存计算结果
    return calculate_regional_data(batch_code)
```

### 2. 实现进度查询
```python
@router.get("/batch/{batch_code}/status")
async def get_batch_status(batch_code: str):
    """查询批次数据准备状态"""
    return {
        "batch_code": batch_code,
        "status": "ready" | "processing" | "not_started",
        "progress": 75,  # 百分比
        "estimated_time": 30  # 预计剩余秒数
    }
```

### 3. 数据库索引优化
```sql
-- 添加复合索引加速查询
CREATE INDEX idx_batch_school_subject
ON student_score_detail(batch_code, school_id, subject_name);

CREATE INDEX idx_statistical_aggregation_batch
ON statistical_aggregation(batch_code, aggregation_level);
```

## 验证步骤

### 1. 测试其他批次
```bash
# 测试数据量较小的批次
curl http://117.72.14.166:8000/api/v12/batch/G4-2024/regional
curl http://117.72.14.166:8000/api/v12/batch/G8-2025/regional
```

### 2. 监控性能
```bash
# 查看服务器资源使用
docker stats converged-computing-app

# 查看日志
docker logs converged-computing-app --tail 100 | grep G7-2025
```

### 3. 数据库查询分析
```sql
-- 检查G7-2025数据量
SELECT COUNT(*) FROM student_score_detail WHERE batch_code = 'G7-2025';

-- 检查是否有缓存数据
SELECT * FROM statistical_aggregation
WHERE batch_code = 'G7-2025'
AND aggregation_level = 'REGIONAL';
```

## 总结

1. **问题本质**：不是404，是超时
2. **根本原因**：G7-2025数据量大
3. **快速解决**：增加超时时间 + 物化数据
4. **长期方案**：优化查询 + 缓存机制

## 建议操作顺序

1. **立即**：前端增加超时到60秒
2. **然后**：手动物化G7-2025数据
3. **后续**：实现自动物化和缓存机制