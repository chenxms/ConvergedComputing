import pandas as pd
import pytest

from app.calculation.calculators.grade_calculator import (
    GradeLevelConfig,
    GradeLevelDistributionCalculator,
    batch_calculate_grades,
    calculate_individual_grade,
)


def test_elementary_thresholds():
    thresholds = GradeLevelConfig.get_thresholds("4th_grade")
    assert thresholds["excellent"] == pytest.approx(0.85)
    assert thresholds["good"] == pytest.approx(0.70)
    assert thresholds["pass"] == pytest.approx(0.60)
    assert thresholds["fail"] == pytest.approx(0.0)


def test_middle_school_thresholds():
    thresholds = GradeLevelConfig.get_thresholds("7th_grade")
    assert thresholds["A"] == pytest.approx(0.80)
    assert thresholds["B"] == pytest.approx(0.70)
    assert thresholds["C"] == pytest.approx(0.60)
    assert thresholds["D"] == pytest.approx(0.0)


def test_elementary_individual_grade_boundaries():
    excellent = calculate_individual_grade(85, "3rd_grade", 100)
    good = calculate_individual_grade(70, "3rd_grade", 100)
    passing = calculate_individual_grade(60, "3rd_grade", 100)
    failing = calculate_individual_grade(59, "3rd_grade", 100)

    assert excellent["grade"] == "excellent"
    assert good["grade"] == "good"
    assert passing["grade"] == "pass"
    assert failing["grade"] == "fail"
    assert passing["threshold_met"] is True
    assert failing["threshold_met"] is False


def test_middle_school_individual_grade_boundaries():
    level_a = calculate_individual_grade(80, "8th_grade", 100)
    level_b = calculate_individual_grade(75, "8th_grade", 100)
    level_c = calculate_individual_grade(60, "8th_grade", 100)
    level_d = calculate_individual_grade(59, "8th_grade", 100)

    assert level_a["grade"] == "A"
    assert level_b["grade"] == "B"
    assert level_c["grade"] == "C"
    assert level_d["grade"] == "D"
    assert level_c["threshold_met"] is True
    assert level_d["threshold_met"] is False


def test_batch_grade_distribution_elementary():
    df = pd.DataFrame({
        "grade_level": ["5th_grade"] * 4,
        "score": [90, 82, 68, 40],
    })
    result = batch_calculate_grades(df, grade_level_col="grade_level", score_col="score", max_score=100)

    assert list(result["calculated_grade"]) == ["excellent", "good", "pass", "fail"]


def test_distribution_calculator_statistics():
    calculator = GradeLevelDistributionCalculator()
    df = pd.DataFrame({"score": [95, 88, 74, 65, 52]})
    config = {"grade_level": "6th_grade", "max_score": 100}

    result = calculator.calculate(df, config)

    assert result["total_count"] == 5
    assert pytest.approx(result["distribution"]["counts"]["excellent"], rel=0) == 2
    assert pytest.approx(result["distribution"]["counts"]["good"], rel=0) == 1
    assert pytest.approx(result["distribution"]["counts"]["pass"], rel=0) == 1
    assert pytest.approx(result["distribution"]["counts"]["fail"], rel=0) == 1
    assert result["statistics"]["pass_rate"] >= 0.6
