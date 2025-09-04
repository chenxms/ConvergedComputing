import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch
from datetime import datetime
import json

from app.main import app
from app.database.enums import AggregationLevel, CalculationStatus
from app.schemas.request_schemas import StatisticalAggregationCreateRequest


@pytest.fixture
def client():
    """Test client fixture"""
    return TestClient(app)


@pytest.fixture
def sample_batch_data():
    """Sample batch data for testing"""
    return {
        "batch_code": "TEST_BATCH_001",
        "aggregation_level": "regional",
        "statistics_data": {
            "batch_info": {
                "batch_code": "TEST_BATCH_001",
                "total_students": 1000,
                "total_schools": 10
            },
            "academic_subjects": [
                {
                    "subject_id": 1,
                    "subject_name": "数学",
                    "statistics": {
                        "average_score": 85.5,
                        "difficulty_coefficient": 0.71,
                        "discrimination_coefficient": 0.45
                    }
                }
            ]
        },
        "total_students": 1000,
        "total_schools": 10
    }


@pytest.fixture
def mock_db_session():
    """Mock database session"""
    with patch('app.database.connection.get_db_session') as mock_get_db:
        mock_session = Mock()
        mock_get_db.return_value = mock_session
        yield mock_session


class TestBatchManagement:
    """Test batch management endpoints"""
    
    def test_create_batch_success(self, client, sample_batch_data, mock_db_session):
        """Test successful batch creation"""
        with patch('app.services.batch_service.BatchService') as mock_service:
            # Mock the service response
            mock_batch = Mock()
            mock_batch.id = 1
            mock_batch.batch_code = "TEST_BATCH_001"
            mock_batch.aggregation_level = AggregationLevel.REGIONAL
            mock_batch.created_at = datetime.now()
            
            mock_service_instance = mock_service.return_value
            mock_service_instance.create_batch.return_value = mock_batch
            
            response = client.post("/api/v1/statistics/batches", json=sample_batch_data)
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "TEST_BATCH_001" in data["message"]
            assert data["data"]["batch_code"] == "TEST_BATCH_001"
    
    def test_create_batch_validation_error(self, client, mock_db_session):
        """Test batch creation with validation error"""
        invalid_data = {
            "batch_code": "",  # Empty batch code should fail validation
            "aggregation_level": "regional"
        }
        
        response = client.post("/api/v1/statistics/batches", json=invalid_data)
        assert response.status_code == 422  # Validation error
    
    def test_get_batch_success(self, client, mock_db_session):
        """Test successful batch retrieval"""
        with patch('app.services.batch_service.BatchService') as mock_service:
            # Mock the service response
            mock_batch = Mock()
            mock_batch.id = 1
            mock_batch.batch_code = "TEST_BATCH_001"
            mock_batch.aggregation_level = AggregationLevel.REGIONAL
            mock_batch.school_id = None
            mock_batch.statistics_data = {"test": "data"}
            mock_batch.calculation_status = CalculationStatus.COMPLETED
            mock_batch.total_students = 1000
            mock_batch.total_schools = 10
            mock_batch.created_at = datetime.now()
            mock_batch.updated_at = datetime.now()
            
            mock_service_instance = mock_service.return_value
            mock_service_instance.get_batch.return_value = mock_batch
            
            response = client.get("/api/v1/statistics/batches/TEST_BATCH_001")
            
            assert response.status_code == 200
            data = response.json()
            assert data["batch_code"] == "TEST_BATCH_001"
    
    def test_get_batch_not_found(self, client, mock_db_session):
        """Test batch retrieval when batch doesn't exist"""
        with patch('app.services.batch_service.BatchService') as mock_service:
            mock_service_instance = mock_service.return_value
            mock_service_instance.get_batch.return_value = None
            
            response = client.get("/api/v1/statistics/batches/NONEXISTENT")
            
            assert response.status_code == 404
    
    def test_update_batch_success(self, client, mock_db_session):
        """Test successful batch update"""
        with patch('app.services.batch_service.BatchService') as mock_service:
            # Mock the service response
            mock_batch = Mock()
            mock_batch.id = 1
            mock_batch.batch_code = "TEST_BATCH_001"
            mock_batch.updated_at = datetime.now()
            
            mock_service_instance = mock_service.return_value
            mock_service_instance.update_batch.return_value = mock_batch
            
            update_data = {
                "calculation_status": "completed",
                "total_students": 1200
            }
            
            response = client.put("/api/v1/statistics/batches/TEST_BATCH_001", json=update_data)
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "TEST_BATCH_001" in data["message"]
    
    def test_delete_batch_success(self, client, mock_db_session):
        """Test successful batch deletion"""
        with patch('app.services.batch_service.BatchService') as mock_service:
            mock_service_instance = mock_service.return_value
            mock_service_instance.delete_batch.return_value = True
            
            response = client.delete("/api/v1/statistics/batches/TEST_BATCH_001")
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "TEST_BATCH_001" in data["message"]
    
    def test_list_batches_success(self, client, mock_db_session):
        """Test successful batch listing"""
        with patch('app.services.batch_service.BatchService') as mock_service:
            # Mock the service response
            mock_batches = [Mock() for _ in range(3)]
            for i, batch in enumerate(mock_batches):
                batch.id = i + 1
                batch.batch_code = f"TEST_BATCH_{i:03d}"
                batch.aggregation_level = AggregationLevel.REGIONAL
                batch.school_id = None
                batch.statistics_data = {"test": "data"}
                batch.calculation_status = CalculationStatus.COMPLETED
                batch.total_students = 1000
                batch.total_schools = 10
                batch.created_at = datetime.now()
                batch.updated_at = datetime.now()
            
            mock_service_instance = mock_service.return_value
            mock_service_instance.list_batches.return_value = mock_batches
            
            response = client.get("/api/v1/statistics/batches")
            
            assert response.status_code == 200
            data = response.json()
            assert len(data) == 3
            assert data[0]["batch_code"] == "TEST_BATCH_000"


class TestTaskManagement:
    """Test task management endpoints"""
    
    def test_start_calculation_task_success(self, client, mock_db_session):
        """Test successful task start"""
        with patch('app.services.task_manager.TaskManager') as mock_manager:
            # Mock the task manager response
            mock_task = Mock()
            mock_task.id = "task-123"
            mock_task.batch_id = 1
            mock_task.status = "running"
            mock_task.progress = 0.0
            mock_task.started_at = datetime.now()
            mock_task.completed_at = None
            mock_task.error_message = None
            
            mock_manager_instance = mock_manager.return_value
            mock_manager_instance.start_calculation_task.return_value = mock_task
            
            response = client.post("/api/v1/statistics/tasks/TEST_BATCH_001/start")
            
            assert response.status_code == 200
            data = response.json()
            assert data["id"] == "task-123"
            assert data["status"] == "running"
    
    def test_get_task_status_success(self, client, mock_db_session):
        """Test successful task status retrieval"""
        with patch('app.services.task_manager.TaskManager') as mock_manager:
            mock_task = Mock()
            mock_task.id = "task-123"
            mock_task.batch_id = 1
            mock_task.status = "completed"
            mock_task.progress = 100.0
            mock_task.started_at = datetime.now()
            mock_task.completed_at = datetime.now()
            mock_task.error_message = None
            
            mock_manager_instance = mock_manager.return_value
            mock_manager_instance.get_task_status.return_value = mock_task
            
            response = client.get("/api/v1/statistics/tasks/task-123/status")
            
            assert response.status_code == 200
            data = response.json()
            assert data["id"] == "task-123"
            assert data["status"] == "completed"
            assert data["progress"] == 100.0
    
    def test_get_task_status_not_found(self, client, mock_db_session):
        """Test task status retrieval when task doesn't exist"""
        with patch('app.services.task_manager.TaskManager') as mock_manager:
            mock_manager_instance = mock_manager.return_value
            mock_manager_instance.get_task_status.return_value = None
            
            response = client.get("/api/v1/statistics/tasks/nonexistent/status")
            
            assert response.status_code == 404
    
    def test_get_task_progress_success(self, client, mock_db_session):
        """Test successful task progress retrieval"""
        with patch('app.services.task_manager.TaskManager') as mock_manager:
            mock_progress = {
                "task_id": "task-123",
                "overall_progress": 65.5,
                "stage_details": [
                    {"stage": "data_loading", "status": "completed", "progress": 100.0},
                    {"stage": "statistical_calculation", "status": "processing", "progress": 45.2},
                    {"stage": "result_aggregation", "status": "pending", "progress": 0.0}
                ],
                "last_updated": datetime.now().isoformat()
            }
            
            mock_manager_instance = mock_manager.return_value
            mock_manager_instance.get_task_progress.return_value = mock_progress
            
            response = client.get("/api/v1/statistics/tasks/task-123/progress")
            
            assert response.status_code == 200
            data = response.json()
            assert data["task_id"] == "task-123"
            assert data["overall_progress"] == 65.5
            assert len(data["stage_details"]) == 3
    
    def test_cancel_task_success(self, client, mock_db_session):
        """Test successful task cancellation"""
        with patch('app.services.task_manager.TaskManager') as mock_manager:
            mock_manager_instance = mock_manager.return_value
            mock_manager_instance.cancel_task.return_value = True
            
            response = client.post("/api/v1/statistics/tasks/task-123/cancel")
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "task-123" in data["message"]
    
    def test_batch_start_tasks_success(self, client, mock_db_session):
        """Test successful batch task start"""
        with patch('app.services.task_manager.TaskManager') as mock_manager:
            mock_tasks = [Mock() for _ in range(3)]
            for i, task in enumerate(mock_tasks):
                task.id = f"task-{i}"
                task.batch_id = i + 1
                task.status = "pending"
                task.progress = 0.0
                task.started_at = datetime.now()
                task.completed_at = None
                task.error_message = None
            
            mock_manager_instance = mock_manager.return_value
            mock_manager_instance.batch_start_tasks.return_value = mock_tasks
            
            batch_codes = ["BATCH_001", "BATCH_002", "BATCH_003"]
            response = client.post(
                "/api/v1/statistics/tasks/batch-start",
                json=batch_codes
            )
            
            assert response.status_code == 200
            data = response.json()
            assert len(data) == 3
            assert data[0]["id"] == "task-0"
    
    def test_batch_cancel_tasks_success(self, client, mock_db_session):
        """Test successful batch task cancellation"""
        with patch('app.services.task_manager.TaskManager') as mock_manager:
            mock_manager_instance = mock_manager.return_value
            mock_manager_instance.batch_cancel_tasks.return_value = 2
            
            task_ids = ["task-1", "task-2", "task-3"]
            response = client.post(
                "/api/v1/statistics/tasks/batch-cancel",
                json=task_ids
            )
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert data["data"]["cancelled_count"] == 2
    
    def test_list_tasks_success(self, client, mock_db_session):
        """Test successful task listing"""
        with patch('app.services.task_manager.TaskManager') as mock_manager:
            mock_tasks = [Mock() for _ in range(2)]
            for i, task in enumerate(mock_tasks):
                task.id = f"task-{i}"
                task.batch_id = i + 1
                task.status = "completed"
                task.progress = 100.0
                task.started_at = datetime.now()
                task.completed_at = datetime.now()
                task.error_message = None
            
            mock_manager_instance = mock_manager.return_value
            mock_manager_instance.list_tasks.return_value = mock_tasks
            
            response = client.get("/api/v1/statistics/tasks?status=completed")
            
            assert response.status_code == 200
            data = response.json()
            assert len(data) == 2
            assert data[0]["status"] == "completed"


class TestSystemStatus:
    """Test system status endpoints"""
    
    def test_get_system_status_success(self, client, mock_db_session):
        """Test successful system status retrieval"""
        with patch('app.services.task_manager.TaskManager') as mock_manager:
            mock_status = {
                "system_status": "healthy",
                "uptime_seconds": 3600.0,
                "memory_tasks": 5,
                "cached_progress": 3,
                "statistics": {
                    "total_tasks": 100,
                    "running_tasks": 2,
                    "completed_tasks": 95,
                    "failed_tasks": 3,
                    "cancelled_tasks": 0
                },
                "last_updated": datetime.now().isoformat()
            }
            
            mock_manager_instance = mock_manager.return_value
            mock_manager_instance.get_system_status.return_value = mock_status
            
            response = client.get("/api/v1/statistics/system/status")
            
            assert response.status_code == 200
            data = response.json()
            assert data["system_status"] == "healthy"
            assert "statistics" in data
            assert data["statistics"]["total_tasks"] == 100


class TestErrorHandling:
    """Test error handling scenarios"""
    
    def test_create_batch_service_error(self, client, sample_batch_data, mock_db_session):
        """Test batch creation service error"""
        with patch('app.services.batch_service.BatchService') as mock_service:
            mock_service_instance = mock_service.return_value
            mock_service_instance.create_batch.side_effect = Exception("Database error")
            
            response = client.post("/api/v1/statistics/batches", json=sample_batch_data)
            
            assert response.status_code == 500
    
    def test_start_task_validation_error(self, client, mock_db_session):
        """Test task start validation error"""
        with patch('app.services.task_manager.TaskManager') as mock_manager:
            mock_manager_instance = mock_manager.return_value
            mock_manager_instance.start_calculation_task.side_effect = ValueError("Batch not found")
            
            response = client.post("/api/v1/statistics/tasks/NONEXISTENT/start")
            
            assert response.status_code == 400
    
    def test_batch_operation_limit_exceeded(self, client, mock_db_session):
        """Test batch operation with too many items"""
        # Test batch start with too many items
        batch_codes = [f"BATCH_{i:03d}" for i in range(60)]  # More than 50 limit
        response = client.post(
            "/api/v1/statistics/tasks/batch-start",
            json=batch_codes
        )
        
        assert response.status_code == 400
        assert "最多启动50个" in response.json()["detail"]
        
        # Test batch cancel with too many items
        task_ids = [f"task-{i}" for i in range(150)]  # More than 100 limit
        response = client.post(
            "/api/v1/statistics/tasks/batch-cancel",
            json=task_ids
        )
        
        assert response.status_code == 400
        assert "最多取消100个" in response.json()["detail"]