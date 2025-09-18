from typing import Any, List, Optional

from app.services.question_option_distribution_service import (
    QuestionOptionDistributionService,
)


class DummyResult:
    def __init__(self, rows: Optional[List[Any]] = None, row: Any = None) -> None:
        self._rows = rows or []
        self._row = row

    def fetchall(self) -> List[Any]:
        return list(self._rows)

    def fetchone(self) -> Any:
        if self._row is not None:
            return self._row
        return self._rows[0] if self._rows else None


class QuestionLabelStubDB:
    def execute(self, sql: Any, params: Optional[dict] = None) -> DummyResult:
        query = str(sql)
        if "GROUP BY question_id, instrument_type" in query:
            return DummyResult(rows=[("Q1", "TYPE", "3", 0, 5)])
        if "is_reverse = :rev" in query:
            return DummyResult(rows=[(1, None), (2, "  "), (3, "satisfied")])
        if "question_id=:qid" in query:
            return DummyResult(rows=[(1, "poor", 10), (2, "average", 5), (3, "satisfied", 3)])
        raise AssertionError(f"Unexpected SQL executed: {query}")


class SubjectLabelStubDB:
    def execute(self, sql: Any, params: Optional[dict] = None) -> DummyResult:
        query = str(sql)
        if "GROUP BY instrument_type, scale_level" in query and "LIMIT 1" in query:
            return DummyResult(row=("TYPE", "5", 8))
        if "FROM questionnaire_scale_options" in query:
            return DummyResult(rows=[(1, "very satisfied"), (2, " "), (3, " satisfied ")])
        if "GROUP BY option_level, option_label" in query:
            return DummyResult(rows=[(2, "fair", 6), (3, "satisfied", 4)])
        if "COUNT(DISTINCT option_level)" in query:
            return DummyResult(row=(3,))
        raise AssertionError(f"Unexpected SQL executed: {query}")


def test_question_label_map_fills_missing_values_with_scores():
    service = QuestionOptionDistributionService()
    label_maps = service._get_question_scale_label_maps(
        QuestionLabelStubDB(), batch_code="B001", subject_name="survey-subject"
    )

    assert label_maps == {"Q1": {1: "poor", 2: "average", 3: "satisfied"}}


def test_subject_label_map_filters_blank_labels():
    service = QuestionOptionDistributionService()
    label_map = service._get_scale_label_map(
        batch_code="B001", subject_name="survey-subject", db=SubjectLabelStubDB()
    )

    assert label_map == {1: "very satisfied", 2: "fair", 3: "satisfied"}
