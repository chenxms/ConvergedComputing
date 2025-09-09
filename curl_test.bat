@echo off
echo === 快速批次API测试 ===

echo.
echo [TEST] 测试批次创建...

curl -X POST "http://localhost:8000/api/v1/management/batches" ^
     -H "Content-Type: application/json" ^
     -d "{\"batch_code\":\"CURL_TEST_001\",\"aggregation_level\":\"regional\",\"statistics_data\":{\"batch_info\":{\"batch_code\":\"CURL_TEST_001\",\"total_students\":1000,\"total_schools\":50},\"academic_subjects\":[{\"subject_id\":1,\"subject_name\":\"语文\",\"statistics\":{\"average_score\":85.5,\"difficulty_coefficient\":0.71,\"discrimination_coefficient\":0.45}}]},\"data_version\":\"1.0\",\"total_students\":1000,\"total_schools\":50,\"triggered_by\":\"curl_test\"}" ^
     --max-time 10

echo.
echo.
echo [TEST] 测试批次查询...
curl -X GET "http://localhost:8000/api/v1/management/batches" ^
     --max-time 10

echo.
echo === 测试完成 ===