#!/usr/bin/env python3
"""
汇聚计算API接口
提供RESTful API访问多层级汇聚功能
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from flask import Flask, request, jsonify
from flask_cors import CORS
from typing import Dict, Any
import json
import traceback
from multi_layer_aggregator import MultiLayerAggregator
from app.database.connection import get_db
from sqlalchemy import text
from app.database.repositories import StatisticalAggregationRepository
from app.database.enums import AggregationLevel as DBAggregationLevel, CalculationStatus
from app.utils.precision import round2_json
from app.services.subjects_builder import SubjectsBuilder

app = Flask(__name__)
CORS(app)  # 允许跨域访问

# 全局汇聚器实例
aggregator = MultiLayerAggregator()

# v1.2 subjects builder
subjects_builder = SubjectsBuilder()

def success_response(data: Any, message: str = "操作成功") -> Dict[str, Any]:
    """成功响应格式"""
    return {
        'success': True,
        'message': message,
        'data': data,
        'code': 200
    }

def error_response(message: str, code: int = 500, details: str = None) -> Dict[str, Any]:
    """错误响应格式"""
    response = {
        'success': False,
        'message': message,
        'code': code
    }
    if details:
        response['details'] = details
    return response

@app.route('/api/health', methods=['GET'])
def health_check():
    """健康检查"""
    return success_response({'status': 'healthy', 'version': '1.0'})

@app.route('/api/batch/<batch_code>/overview', methods=['GET'])
def get_batch_overview(batch_code: str):
    """
    获取批次概览
    
    GET /api/batch/G4-2025/overview
    """
    try:
        result = aggregator.get_batch_overview(batch_code)
        
        if 'error' in result:
            return jsonify(error_response(result['error'], 404)), 404
        
        return jsonify(success_response(result, f"成功获取批次 {batch_code} 概览"))
        
    except Exception as e:
        return jsonify(error_response(f"获取批次概览失败: {str(e)}", 500, traceback.format_exc())), 500


# ========== v1.2 unified subjects endpoints ==========

def _fetch_v12_regional(batch_code: str):
    db = next(get_db())
    try:
        repo = StatisticalAggregationRepository(db)
        regional = repo.get_regional_statistics(batch_code)
        if regional and regional.statistics_data and isinstance(regional.statistics_data, dict):
            data = regional.statistics_data
            # 若已包含 subjects 结构且 schema_version>=v1.2 则直接返回
            if data.get('subjects'):
                return data
        # 构建并保存
        subjects = subjects_builder.build_regional_subjects(batch_code)
        result = {
            'schema_version': 'v1.2',
            'batch_code': batch_code,
            'aggregation_level': 'REGIONAL',
            'subjects': subjects,
        }
        processed = round2_json(result)
        repo.upsert_statistics({
            'batch_code': batch_code,
            'aggregation_level': DBAggregationLevel.REGIONAL,
            'school_id': None,
            'school_name': None,
            'statistics_data': processed,
            'calculation_status': CalculationStatus.COMPLETED,
        })
        return processed
    finally:
        db.close()


def _fetch_v12_school(batch_code: str, school_code: str):
    db = next(get_db())
    try:
        repo = StatisticalAggregationRepository(db)
        # 直接查学校级记录
        school_rows = repo.get_all_school_statistics(batch_code)
        for row in school_rows:
            if (row.school_id or '') == school_code:
                data = row.statistics_data if isinstance(row.statistics_data, dict) else {}
                if data.get('subjects'):
                    return data
        # 构建并保存
        subjects = subjects_builder.build_school_subjects(batch_code, school_code)
        result = {
            'schema_version': 'v1.2',
            'batch_code': batch_code,
            'aggregation_level': 'SCHOOL',
            'school_code': school_code,
            'subjects': subjects,
        }
        processed = round2_json(result)
        repo.upsert_statistics({
            'batch_code': batch_code,
            'aggregation_level': DBAggregationLevel.SCHOOL,
            'school_id': school_code,
            'school_name': None,
            'statistics_data': processed,
            'calculation_status': CalculationStatus.COMPLETED,
        })
        return processed
    finally:
        db.close()


@app.route('/api/v12/batch/<batch_code>/regional', methods=['GET'])
def get_v12_regional(batch_code: str):
    try:
        data = _fetch_v12_regional(batch_code)
        return jsonify(success_response(data, f"v1.2 区域级 subjects 已生成 {batch_code}"))
    except Exception as e:
        return jsonify(error_response(f"生成 v1.2 区域级失败: {str(e)}", 500, traceback.format_exc())), 500


@app.route('/api/v12/batch/<batch_code>/school/<school_code>', methods=['GET'])
def get_v12_school(batch_code: str, school_code: str):
    try:
        data = _fetch_v12_school(batch_code, school_code)
        return jsonify(success_response(data, f"v1.2 学校级 subjects 已生成 {batch_code}/{school_code}"))
    except Exception as e:
        return jsonify(error_response(f"生成 v1.2 学校级失败: {str(e)}", 500, traceback.format_exc())), 500


@app.route('/api/v12/batch/<batch_code>/materialize', methods=['POST'])
def materialize_v12(batch_code: str):
    try:
        # 触发区域
        _fetch_v12_regional(batch_code)
        # 触发学校级（批量）
        db = next(get_db())
        try:
            rows = db.execute(text("SELECT DISTINCT school_code FROM student_cleaned_scores WHERE batch_code=:b"), {"b": batch_code}).fetchall()
        finally:
            db.close()
        count = 0
        for (school_code,) in rows:
            _fetch_v12_school(batch_code, school_code)
            count += 1
        return jsonify(success_response({"batch_code": batch_code, "schools_materialized": count}, "v1.2 subjects 全量生成完成"))
    except Exception as e:
        return jsonify(error_response(f"v1.2 全量生成失败: {str(e)}", 500, traceback.format_exc())), 500

@app.route('/api/batch/<batch_code>/subjects', methods=['GET'])
def get_subjects_analysis(batch_code: str):
    """
    获取学科汇聚分析
    
    GET /api/batch/G4-2025/subjects
    GET /api/batch/G4-2025/subjects?school_code=SCH001
    """
    try:
        school_code = request.args.get('school_code')
        
        result = aggregator.aggregate_all_subjects(batch_code, school_code)
        
        if 'error' in result:
            return jsonify(error_response(result['error'], 404)), 404
        
        level = "学校" if school_code else "区域"
        message = f"成功获取批次 {batch_code} {level}层级学科分析"
        
        return jsonify(success_response(result, message))
        
    except Exception as e:
        return jsonify(error_response(f"获取学科分析失败: {str(e)}", 500, traceback.format_exc())), 500

@app.route('/api/batch/<batch_code>/dimensions', methods=['GET'])
def get_dimensions_analysis(batch_code: str):
    """
    获取维度汇聚分析
    
    GET /api/batch/G4-2025/dimensions
    GET /api/batch/G4-2025/dimensions?school_code=SCH001
    GET /api/batch/G4-2025/dimensions?school_code=SCH001&subject_name=数学
    """
    try:
        school_code = request.args.get('school_code')
        subject_name = request.args.get('subject_name')
        
        result = aggregator.aggregate_all_dimensions(batch_code, school_code, subject_name)
        
        if 'error' in result:
            return jsonify(error_response(result['error'], 404)), 404
        
        level = "学校" if school_code else "区域"
        subject_filter = f" {subject_name}学科" if subject_name else ""
        message = f"成功获取批次 {batch_code} {level}层级{subject_filter}维度分析"
        
        return jsonify(success_response(result, message))
        
    except Exception as e:
        return jsonify(error_response(f"获取维度分析失败: {str(e)}", 500, traceback.format_exc())), 500

@app.route('/api/batch/<batch_code>/complete', methods=['GET'])
def get_complete_analysis(batch_code: str):
    """
    获取完整分析报告
    
    GET /api/batch/G4-2025/complete
    GET /api/batch/G4-2025/complete?school_code=SCH001
    """
    try:
        school_code = request.args.get('school_code')
        
        result = aggregator.get_complete_analysis(batch_code, school_code)
        
        if 'error' in result:
            return jsonify(error_response(result['error'], 404)), 404
        
        level = "学校" if school_code else "区域"
        message = f"成功生成批次 {batch_code} {level}层级完整分析报告"
        
        return jsonify(success_response(result, message))
        
    except Exception as e:
        return jsonify(error_response(f"生成完整分析报告失败: {str(e)}", 500, traceback.format_exc())), 500

@app.route('/api/batch/<batch_code>/rankings', methods=['GET'])
def get_school_rankings(batch_code: str):
    """
    获取学校排名分析（仅区域层级）
    
    GET /api/batch/G4-2025/rankings
    GET /api/batch/G4-2025/rankings?subject_name=数学
    """
    try:
        subject_name = request.args.get('subject_name')
        
        result = aggregator.get_school_ranking(batch_code, subject_name)
        
        if 'error' in result:
            return jsonify(error_response(result['error'], 404)), 404
        
        subject_filter = f" {subject_name}学科" if subject_name else ""
        message = f"成功获取批次 {batch_code}{subject_filter}学校排名分析"
        
        return jsonify(success_response(result, message))
        
    except Exception as e:
        return jsonify(error_response(f"获取学校排名失败: {str(e)}", 500, traceback.format_exc())), 500

@app.route('/api/batch/<batch_code>/questionnaire/options', methods=['GET'])
def get_questionnaire_options(batch_code: str):
    """
    获取问卷选项分布分析
    
    GET /api/batch/G4-2025/questionnaire/options
    GET /api/batch/G4-2025/questionnaire/options?school_code=SCH001
    GET /api/batch/G4-2025/questionnaire/options?school_code=SCH001&dimension_code=CZL-hqx
    """
    try:
        school_code = request.args.get('school_code')
        dimension_code = request.args.get('dimension_code')
        
        result = aggregator.questionnaire_aggregator.get_option_distribution(
            batch_code, school_code, dimension_code
        )
        
        if 'error' in result:
            return jsonify(error_response(result['error'], 404)), 404
        
        level = "学校" if school_code else "区域"
        dimension_filter = f" {dimension_code}维度" if dimension_code else ""
        message = f"成功获取批次 {batch_code} {level}层级{dimension_filter}问卷选项分布"
        
        return jsonify(success_response(result, message))
        
    except Exception as e:
        return jsonify(error_response(f"获取问卷选项分布失败: {str(e)}", 500, traceback.format_exc())), 500

@app.route('/api/batch/<batch_code>/questionnaire/summary', methods=['GET'])
def get_questionnaire_summary(batch_code: str):
    """
    获取问卷汇总分析
    
    GET /api/batch/G4-2025/questionnaire/summary
    GET /api/batch/G4-2025/questionnaire/summary?school_code=SCH001
    """
    try:
        school_code = request.args.get('school_code')
        
        result = aggregator.questionnaire_aggregator.get_questionnaire_summary(
            batch_code, school_code
        )
        
        if 'error' in result:
            return jsonify(error_response(result['error'], 404)), 404
        
        level = "学校" if school_code else "区域"
        message = f"成功获取批次 {batch_code} {level}层级问卷汇总分析"
        
        return jsonify(success_response(result, message))
        
    except Exception as e:
        return jsonify(error_response(f"获取问卷汇总分析失败: {str(e)}", 500, traceback.format_exc())), 500

@app.route('/api/batch/<batch_code>/subjects/comparison', methods=['GET'])
def get_subjects_comparison(batch_code: str):
    """
    获取学科对比分析
    
    GET /api/batch/G4-2025/subjects/comparison
    GET /api/batch/G4-2025/subjects/comparison?school_codes=SCH001,SCH002,SCH003
    """
    try:
        school_codes_param = request.args.get('school_codes')
        school_codes = school_codes_param.split(',') if school_codes_param else None
        
        result = aggregator.exam_aggregator.get_subject_comparison(batch_code, school_codes)
        
        if 'error' in result:
            return jsonify(error_response(result['error'], 404)), 404
        
        scope = f"{len(school_codes)}所学校" if school_codes else "全区域"
        message = f"成功获取批次 {batch_code} {scope}学科对比分析"
        
        return jsonify(success_response(result, message))
        
    except Exception as e:
        return jsonify(error_response(f"获取学科对比分析失败: {str(e)}", 500, traceback.format_exc())), 500

@app.route('/api/batches', methods=['GET'])
def get_available_batches():
    """
    获取可用批次列表
    
    GET /api/batches
    """
    try:
        # 通过查询数据库获取可用批次
        session = aggregator.exam_aggregator.get_session()
        
        try:
            from sqlalchemy import text
            
            query = text("""
                SELECT DISTINCT 
                    batch_code,
                    COUNT(DISTINCT student_id) as student_count,
                    COUNT(DISTINCT school_code) as school_count,
                    COUNT(DISTINCT subject_name) as subject_count
                FROM student_cleaned_scores
                GROUP BY batch_code
                ORDER BY batch_code
            """)
            
            result = session.execute(query)
            batches = []
            
            for row in result.fetchall():
                batches.append({
                    'batch_code': row[0],
                    'student_count': row[1],
                    'school_count': row[2], 
                    'subject_count': row[3]
                })
            
            return jsonify(success_response({
                'batches': batches,
                'total_count': len(batches)
            }, "成功获取可用批次列表"))
            
        finally:
            aggregator.exam_aggregator.close_session(session)
        
    except Exception as e:
        return jsonify(error_response(f"获取批次列表失败: {str(e)}", 500, traceback.format_exc())), 500

@app.route('/api/batch/<batch_code>/schools', methods=['GET'])
def get_batch_schools(batch_code: str):
    """
    获取批次学校列表
    
    GET /api/batch/G4-2025/schools
    """
    try:
        schools = aggregator.exam_aggregator.get_school_info(batch_code)
        
        school_list = [
            {
                'school_code': school[0],
                'school_name': school[1],
                'student_count': school[2]
            }
            for school in schools
        ]
        
        return jsonify(success_response({
            'batch_code': batch_code,
            'schools': school_list,
            'total_schools': len(school_list)
        }, f"成功获取批次 {batch_code} 学校列表"))
        
    except Exception as e:
        return jsonify(error_response(f"获取学校列表失败: {str(e)}", 500, traceback.format_exc())), 500

@app.route('/api/docs', methods=['GET'])
def get_api_docs():
    """
    API文档
    
    GET /api/docs
    """
    docs = {
        "title": "教育统计汇聚计算API",
        "version": "1.0",
        "description": "提供多层级、多学科类型的教育统计汇聚计算接口",
        "endpoints": {
            "批次管理": {
                "GET /api/batches": "获取可用批次列表",
                "GET /api/batch/{batch_code}/overview": "获取批次概览",
                "GET /api/batch/{batch_code}/schools": "获取批次学校列表"
            },
            "学科分析": {
                "GET /api/batch/{batch_code}/subjects": "获取学科汇聚分析",
                "GET /api/batch/{batch_code}/subjects/comparison": "获取学科对比分析"
            },
            "维度分析": {
                "GET /api/batch/{batch_code}/dimensions": "获取维度汇聚分析"
            },
            "问卷分析": {
                "GET /api/batch/{batch_code}/questionnaire/options": "获取问卷选项分布",
                "GET /api/batch/{batch_code}/questionnaire/summary": "获取问卷汇总分析"
            },
            "综合分析": {
                "GET /api/batch/{batch_code}/complete": "获取完整分析报告",
                "GET /api/batch/{batch_code}/rankings": "获取学校排名分析"
            },
            "系统": {
                "GET /api/health": "健康检查",
                "GET /api/docs": "API文档"
            }
        },
        "parameters": {
            "school_code": "学校代码（可选，用于学校层级分析）",
            "subject_name": "学科名称（可选，用于筛选特定学科）",
            "dimension_code": "维度代码（可选，用于筛选特定维度）",
            "school_codes": "学校代码列表（逗号分隔，用于对比分析）"
        },
        "response_format": {
            "success": {
                "success": True,
                "message": "操作成功",
                "data": "实际数据",
                "code": 200
            },
            "error": {
                "success": False,
                "message": "错误信息",
                "code": "错误代码",
                "details": "详细错误信息（可选）"
            }
        }
    }
    
    return jsonify(success_response(docs, "API文档获取成功"))

@app.errorhandler(404)
def not_found(error):
    """404错误处理"""
    return jsonify(error_response("接口未找到", 404)), 404

@app.errorhandler(405)
def method_not_allowed(error):
    """405错误处理"""
    return jsonify(error_response("请求方法不允许", 405)), 405

@app.errorhandler(500)
def internal_error(error):
    """500错误处理"""
    return jsonify(error_response("内部服务器错误", 500)), 500

if __name__ == '__main__':
    print("=== 汇聚计算API服务启动 ===")
    print("访问 http://localhost:5000/api/docs 查看API文档")
    print("访问 http://localhost:5000/api/health 进行健康检查")
    print("=====================================")
    
    app.run(host='0.0.0.0', port=5000, debug=True)
