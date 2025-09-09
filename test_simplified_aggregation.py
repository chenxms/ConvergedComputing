#!/usr/bin/env python3
"""
简化汇聚服务测试脚本
测试问卷处理器、汇聚服务和数据仓库的集成功能
"""

import asyncio
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 使用项目配置的数据库连接
import os
DATABASE_HOST = os.getenv("DATABASE_HOST", "117.72.14.166")
DATABASE_PORT = os.getenv("DATABASE_PORT", "23506")
DATABASE_USER = os.getenv("DATABASE_USER", "root")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD", "mysql_Lujing2022")
DATABASE_NAME = os.getenv("DATABASE_NAME", "appraisal_test")

DATABASE_URL = (
    f"mysql+pymysql://{DATABASE_USER}:{DATABASE_PASSWORD}"
    f"@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"
    "?charset=utf8mb4"
)
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


def test_questionnaire_processor():
    """测试问卷处理器"""
    from app.services.questionnaire_processor import (
        QuestionnaireProcessor, QuestionnaireConfig, ScaleType
    )
    
    logger.info("=== 测试问卷处理器 ===")
    
    processor = QuestionnaireProcessor()
    
    # 创建测试数据
    test_data = pd.DataFrame({
        'student_id': ['S001', 'S002', 'S003', 'S001', 'S002', 'S003'],
        'question_id': ['Q1', 'Q1', 'Q1', 'Q2', 'Q2', 'Q2'],
        'raw_score': [4, 3, 5, 2, 1, 3],
        'dimension_code': ['DIM1', 'DIM1', 'DIM1', 'DIM1', 'DIM1', 'DIM1'],
        'dimension_name': ['学习兴趣', '学习兴趣', '学习兴趣', '学习兴趣', '学习兴趣', '学习兴趣']
    })
    
    # 创建配置
    configs = [
        QuestionnaireConfig(
            scale_type=ScaleType.SCALE_5_LIKERT,
            question_id='Q1',
            question_name='您对学习是否感兴趣？',
            dimension_code='DIM1',
            dimension_name='学习兴趣'
        ),
        QuestionnaireConfig(
            scale_type=ScaleType.SCALE_5_LIKERT,
            question_id='Q2',
            question_name='您认为学习重要吗？',
            dimension_code='DIM1',
            dimension_name='学习兴趣'
        )
    ]
    
    # 处理问卷数据
    result = processor.process_questionnaire_data(test_data, configs, "TEST-BATCH")
    
    logger.info(f"处理结果: {len(result)} 个维度")
    for dim_stat in result:
        logger.info(f"维度: {dim_stat.dimension_name}, 平均分: {dim_stat.avg_score}")
        logger.info(f"  选项分布: {len(dim_stat.dimension_option_distributions)} 个选项")
        logger.info(f"  题目数量: {len(dim_stat.questions)} 个题目")
    
    return True


def test_aggregation_service_with_real_data():
    """使用真实数据测试汇聚服务"""
    from app.services.simplified_aggregation_service import SimplifiedAggregationService
    from app.repositories.simplified_aggregation_repository import SimplifiedAggregationRepository
    from app.database.models import AggregationLevel
    
    logger.info("=== 测试汇聚服务（真实数据）===")
    
    db_session = SessionLocal()
    
    try:
        # 查找可用的批次
        query = text("""
            SELECT batch_code, COUNT(DISTINCT student_id) as student_count,
                   COUNT(DISTINCT school_id) as school_count,
                   COUNT(DISTINCT subject_id) as subject_count
            FROM student_score_detail 
            WHERE total_score IS NOT NULL
            GROUP BY batch_code
            HAVING student_count >= 10
            ORDER BY student_count DESC
            LIMIT 3
        """)
        
        result = db_session.execute(query)
        available_batches = result.fetchall()
        
        if not available_batches:
            logger.warning("没有找到合适的测试批次")
            return False
        
        logger.info("可用的测试批次:")
        for batch in available_batches:
            logger.info(f"  {batch.batch_code}: {batch.student_count}学生, {batch.school_count}学校, {batch.subject_count}科目")
        
        # 选择第一个批次进行测试
        test_batch = available_batches[0].batch_code
        logger.info(f"使用批次进行测试: {test_batch}")
        
        # 初始化服务和仓库
        aggregation_service = SimplifiedAggregationService(db_session)
        repository = SimplifiedAggregationRepository(db_session)
        
        def progress_callback(progress, message):
            logger.info(f"进度 {progress}%: {message}")
        
        # 测试区域级汇聚
        logger.info("开始区域级汇聚测试...")
        regional_result = aggregation_service.aggregate_batch_regional(
            test_batch, progress_callback
        )
        
        if regional_result['success']:
            logger.info(f"区域级汇聚成功!")
            logger.info(f"  科目数量: {regional_result['subjects_count']}")
            logger.info(f"  学校总数: {regional_result['total_schools']}")
            logger.info(f"  学生总数: {regional_result['total_students']}")
            logger.info(f"  耗时: {regional_result['duration']:.2f}秒")
            
            # 保存区域级数据
            save_result = repository.save_aggregation_data(
                batch_code=test_batch,
                aggregation_level=AggregationLevel.REGIONAL,
                data=regional_result['data'],
                calculation_duration=regional_result['duration']
            )
            logger.info(f"保存区域级数据: {save_result}")
        else:
            logger.error(f"区域级汇聚失败: {regional_result['error']}")
            return False
        
        # 测试学校级汇聚
        school_query = text("""
            SELECT DISTINCT school_id 
            FROM student_score_detail 
            WHERE batch_code = :batch_code 
            LIMIT 2
        """)
        school_result = db_session.execute(school_query, {'batch_code': test_batch})
        schools = [row.school_id for row in school_result.fetchall()]
        
        if schools:
            test_school = schools[0]
            logger.info(f"测试学校级汇聚，学校: {test_school}")
            
            school_result = aggregation_service.aggregate_batch_school(
                test_batch, test_school, f"学校_{test_school}", progress_callback
            )
            
            if school_result['success']:
                logger.info(f"学校级汇聚成功!")
                logger.info(f"  科目数量: {school_result['subjects_count']}")
                logger.info(f"  学生总数: {school_result['total_students']}")
                logger.info(f"  耗时: {school_result['duration']:.2f}秒")
                
                # 保存学校级数据
                save_result = repository.save_aggregation_data(
                    batch_code=test_batch,
                    aggregation_level=AggregationLevel.SCHOOL,
                    data=school_result['data'],
                    school_id=test_school,
                    school_name=f"学校_{test_school}",
                    calculation_duration=school_result['duration']
                )
                logger.info(f"保存学校级数据: {save_result}")
            else:
                logger.error(f"学校级汇聚失败: {school_result['error']}")
        
        # 测试数据读取
        logger.info("测试数据读取...")
        read_result = repository.get_aggregation_data(
            test_batch, AggregationLevel.REGIONAL
        )
        if read_result:
            logger.info(f"读取区域级数据成功，记录ID: {read_result['id']}")
            logger.info(f"  数据版本: {read_result['data_version']}")
            logger.info(f"  学生总数: {read_result['total_students']}")
            logger.info(f"  创建时间: {read_result['created_at']}")
        
        # 测试批次状态
        status_info = repository.get_batch_aggregation_status(test_batch)
        logger.info(f"批次状态信息: {status_info}")
        
        return True
        
    except Exception as e:
        logger.error(f"测试汇聚服务失败: {str(e)}")
        return False
    
    finally:
        db_session.close()


def test_repository_operations():
    """测试仓库操作"""
    from app.repositories.simplified_aggregation_repository import SimplifiedAggregationRepository
    from app.database.models import AggregationLevel, CalculationStatus
    from app.schemas.simplified_aggregation_schema import RegionalAggregationData, SubjectStatistics
    
    logger.info("=== 测试仓库操作 ===")
    
    db_session = SessionLocal()
    
    try:
        repository = SimplifiedAggregationRepository(db_session)
        
        # 创建测试数据
        test_data = {
            'batch_code': 'TEST-REPO',
            'aggregation_level': 'REGIONAL',
            'total_schools': 5,
            'total_students': 100,
            'subjects': {},
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),
            'data_version': '2.0'
        }
        
        # 测试保存
        save_result = repository.save_aggregation_data(
            batch_code='TEST-REPO',
            aggregation_level=AggregationLevel.REGIONAL,
            data=test_data,
            calculation_duration=5.5
        )
        logger.info(f"保存测试: {save_result}")
        
        # 测试读取
        read_result = repository.get_aggregation_data(
            'TEST-REPO', AggregationLevel.REGIONAL
        )
        logger.info(f"读取测试: 找到记录 ID {read_result['id'] if read_result else 'None'}")
        
        # 测试状态更新
        update_result = repository.update_aggregation_status(
            'TEST-REPO', AggregationLevel.REGIONAL, CalculationStatus.COMPLETED
        )
        logger.info(f"状态更新测试: {update_result}")
        
        # 测试最近记录查询
        recent_records = repository.get_recent_aggregations(limit=5)
        logger.info(f"最近记录查询: 找到 {len(recent_records)} 条记录")
        
        # 清理测试数据
        delete_result = repository.delete_batch_aggregations('TEST-REPO')
        logger.info(f"清理测试数据: {delete_result}")
        
        return True
        
    except Exception as e:
        logger.error(f"测试仓库操作失败: {str(e)}")
        return False
    
    finally:
        db_session.close()


def main():
    """主测试函数"""
    logger.info("开始简化汇聚服务测试")
    
    test_results = {
        '问卷处理器': test_questionnaire_processor(),
        '汇聚服务（真实数据）': test_aggregation_service_with_real_data(),
        '仓库操作': test_repository_operations(),
    }
    
    logger.info("=== 测试结果汇总 ===")
    success_count = 0
    for test_name, result in test_results.items():
        status = "✅ 成功" if result else "❌ 失败"
        logger.info(f"{test_name}: {status}")
        if result:
            success_count += 1
    
    logger.info(f"总体结果: {success_count}/{len(test_results)} 个测试通过")
    
    if success_count == len(test_results):
        logger.info("🎉 所有测试通过！简化汇聚服务已就绪")
    else:
        logger.warning("⚠️  部分测试失败，请检查相关组件")


if __name__ == "__main__":
    main()