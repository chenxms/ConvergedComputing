#!/usr/bin/env python3
"""
数据库连接状态检查和清理脚本
检查当前连接状态，清理僵死连接，优化性能设置
"""

import time
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from app.database.connection import get_database_url

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_database_status():
    """检查数据库连接状态和性能指标"""
    try:
        # 创建引擎，不使用连接池来避免额外连接
        engine = create_engine(
            get_database_url(),
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True
        )

        with engine.connect() as conn:
            logger.info("=== 数据库连接状态检查 ===")

            # 1. 检查当前进程列表
            logger.info("1. 检查当前进程列表")
            result = conn.execute(text("SHOW PROCESSLIST"))
            processes = result.fetchall()

            logger.info(f"当前活跃连接数: {len(processes)}")

            # 统计不同状态的连接
            status_count = {}
            long_running = []

            for proc in processes:
                proc_id, user, host, db, command, time_val, state, info = proc

                # 统计状态
                status_count[state] = status_count.get(state, 0) + 1

                # 找出长时间运行的查询（超过60秒）
                if time_val and time_val > 60:
                    long_running.append({
                        'id': proc_id,
                        'user': user,
                        'time': time_val,
                        'state': state,
                        'info': info[:100] if info else None
                    })

            logger.info("连接状态统计:")
            for state, count in status_count.items():
                logger.info(f"  {state or 'NULL'}: {count}")

            if long_running:
                logger.warning(f"发现 {len(long_running)} 个长时间运行的查询:")
                for query in long_running:
                    logger.warning(f"  ID {query['id']}: {query['user']} - {query['time']}s - {query['state']} - {query['info']}")

            # 2. 检查连接统计
            logger.info("\n2. 检查连接统计")
            connection_stats = conn.execute(text("SHOW STATUS LIKE '%connection%'")).fetchall()
            for stat in connection_stats:
                logger.info(f"  {stat[0]}: {stat[1]}")

            # 3. 检查关键性能指标
            logger.info("\n3. 检查关键性能指标")
            perf_queries = [
                "SHOW STATUS LIKE 'Threads_%'",
                "SHOW STATUS LIKE 'Max_used_connections'",
                "SHOW STATUS LIKE 'Aborted_%'",
                "SHOW VARIABLES LIKE 'max_connections'",
                "SHOW VARIABLES LIKE 'wait_timeout'",
                "SHOW VARIABLES LIKE 'interactive_timeout'"
            ]

            for query in perf_queries:
                try:
                    result = conn.execute(text(query)).fetchall()
                    for row in result:
                        logger.info(f"  {row[0]}: {row[1]}")
                except Exception as e:
                    logger.error(f"执行查询失败 {query}: {e}")

            # 4. 检查InnoDB状态
            logger.info("\n4. 检查InnoDB状态")
            try:
                innodb_status = conn.execute(text("SHOW ENGINE INNODB STATUS")).fetchone()
                if innodb_status:
                    status_text = innodb_status[2]

                    # 提取关键信息
                    lines = status_text.split('\n')
                    for line in lines:
                        if any(keyword in line.lower() for keyword in ['pending', 'thread', 'transaction', 'lock']):
                            if line.strip():
                                logger.info(f"  {line.strip()}")

            except Exception as e:
                logger.error(f"获取InnoDB状态失败: {e}")

        engine.dispose()
        return True

    except SQLAlchemyError as e:
        logger.error(f"数据库连接失败: {e}")
        return False
    except Exception as e:
        logger.error(f"检查过程发生错误: {e}")
        return False

def kill_long_running_queries(max_time_seconds=300):
    """杀死长时间运行的查询"""
    try:
        engine = create_engine(
            get_database_url(),
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True
        )

        with engine.connect() as conn:
            logger.info(f"\n=== 清理超过 {max_time_seconds} 秒的长时间查询 ===")

            # 获取长时间运行的查询
            result = conn.execute(text("SHOW PROCESSLIST"))
            processes = result.fetchall()

            killed_count = 0
            for proc in processes:
                proc_id, user, host, db, command, time_val, state, info = proc

                # 跳过系统用户和当前连接
                if user in ['system user', 'event_scheduler'] or command == 'Binlog Dump':
                    continue

                # 杀死长时间运行的查询（但不杀死Sleep状态的连接）
                if time_val and time_val > max_time_seconds and state and state.lower() != 'sleep':
                    try:
                        conn.execute(text(f"KILL {proc_id}"))
                        logger.info(f"已杀死进程 {proc_id}: {user} - {time_val}s - {state}")
                        killed_count += 1
                    except Exception as e:
                        logger.error(f"杀死进程 {proc_id} 失败: {e}")

            logger.info(f"共杀死 {killed_count} 个长时间运行的查询")

        engine.dispose()
        return killed_count

    except Exception as e:
        logger.error(f"清理长时间查询失败: {e}")
        return 0

def optimize_database_settings():
    """优化数据库设置（只读检查，不修改）"""
    try:
        engine = create_engine(
            get_database_url(),
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True
        )

        with engine.connect() as conn:
            logger.info("\n=== 数据库设置优化建议 ===")

            # 检查关键配置
            config_checks = [
                ('max_connections', 500, 'ge'),  # 应该 >= 500
                ('wait_timeout', 3600, 'le'),    # 应该 <= 3600
                ('interactive_timeout', 3600, 'le'),
                ('innodb_buffer_pool_size', 1073741824, 'ge'),  # 应该 >= 1GB
            ]

            for var_name, recommended, comparison in config_checks:
                try:
                    result = conn.execute(text(f"SHOW VARIABLES LIKE '{var_name}'")).fetchone()
                    if result:
                        current_value = result[1]
                        try:
                            current_num = int(current_value)
                            if comparison == 'ge' and current_num < recommended:
                                logger.warning(f"  {var_name}: 当前 {current_value}, 建议 >= {recommended}")
                            elif comparison == 'le' and current_num > recommended:
                                logger.warning(f"  {var_name}: 当前 {current_value}, 建议 <= {recommended}")
                            else:
                                logger.info(f"  {var_name}: {current_value} ✓")
                        except ValueError:
                            logger.info(f"  {var_name}: {current_value}")
                except Exception as e:
                    logger.error(f"检查 {var_name} 失败: {e}")

        engine.dispose()

    except Exception as e:
        logger.error(f"优化检查失败: {e}")

def test_connection_performance():
    """测试连接性能"""
    logger.info("\n=== 连接性能测试 ===")

    try:
        # 测试单个连接的延迟
        start_time = time.time()
        engine = create_engine(
            get_database_url(),
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True
        )

        with engine.connect() as conn:
            # 简单查询测试
            conn.execute(text("SELECT 1"))
            connection_time = time.time() - start_time
            logger.info(f"单次连接+查询耗时: {connection_time:.3f}s")

            # 多次查询测试
            start_time = time.time()
            for i in range(10):
                conn.execute(text("SELECT 1"))
            query_time = (time.time() - start_time) / 10
            logger.info(f"平均查询耗时: {query_time:.3f}s")

        engine.dispose()

    except Exception as e:
        logger.error(f"性能测试失败: {e}")

if __name__ == "__main__":
    logger.info("开始数据库连接清理和优化...")

    # 1. 检查当前状态
    if not check_database_status():
        logger.error("数据库连接失败，退出")
        exit(1)

    # 2. 清理长时间运行的查询
    killed = kill_long_running_queries(300)  # 5分钟

    # 3. 测试连接性能
    test_connection_performance()

    # 4. 提供优化建议
    optimize_database_settings()

    logger.info("\n数据库连接清理完成!")

    if killed > 0:
        logger.info("建议等待几秒钟让系统稳定，然后重新运行您的数据处理任务")