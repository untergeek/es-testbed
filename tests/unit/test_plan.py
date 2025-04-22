"""Tests for es_testbed._plan module."""

# pylint: disable=W0212
from unittest.mock import patch
import pytest
from dotmap import DotMap
from es_testbed._plan import PlanBuilder


def test_plan_builder_init(plan_builder, settings):
    """Test PlanBuilder initialization."""
    assert plan_builder.settings == settings
    assert plan_builder._plan.cleanup == "UNSET"


def test_plan_builder_init_no_settings():
    """Test PlanBuilder initialization without settings."""
    with pytest.raises(ValueError, match="Must provide a settings dictionary"):
        PlanBuilder(settings=None)


def test_plan_property(plan_builder):
    """Test PlanBuilder plan property."""
    assert plan_builder.plan == plan_builder._plan


def test_plan_builder_autoset(settings):
    """Test PlanBuilder initialization with autobuild."""
    plan_builder = PlanBuilder(settings=settings, autobuild=True)
    assert plan_builder.settings == settings
    assert plan_builder._plan.cleanup == "UNSET"


def test_create_lists(plan_builder):
    """Test PlanBuilder _create_lists method."""
    plan_builder._create_lists()
    for name in [
        "indices",
        "data_stream",
        "snapshots",
        "ilm_policies",
        "index_templates",
        "component_templates",
    ]:
        assert name in plan_builder._plan
        assert plan_builder._plan[name] == []


def test_setup(plan_builder):
    """Test PlanBuilder setup method."""
    with patch("es_testbed._plan.randomstr", return_value="randomstr"):
        with patch.object(plan_builder, "update") as mock_update:
            with patch.object(
                plan_builder, "update_rollover_alias"
            ) as mock_update_rollover_alias:
                with patch.object(plan_builder, "update_ilm") as mock_update_ilm:
                    plan_builder.setup()
                    assert plan_builder._plan.uniq == "randomstr"
                    mock_update.assert_called_once_with(plan_builder.settings)
                    mock_update_rollover_alias.assert_called_once()
                    mock_update_ilm.assert_called_once()


def test_update(plan_builder):
    """Test PlanBuilder update method."""
    new_settings = {"new_key": "new_value"}
    plan_builder.update(new_settings)
    assert plan_builder._plan.new_key == "new_value"


def test_update_ilm(plan_builder):
    """Test PlanBuilder update_ilm method."""
    plan_builder._plan.ilm = DotMap({"enabled": True, "phases": ["hot"]})
    plan_builder._plan.index_buildlist = [{"searchable": "warm"}]
    with patch("es_testbed._plan.build_ilm_policy", return_value="ilm_policy"):
        plan_builder.update_ilm()
        assert plan_builder._plan.ilm.policy == "ilm_policy"
        assert "warm" in plan_builder._plan.ilm.phases


def test_update_ilm_not_in_plan(plan_builder):
    """Test PlanBuilder update_ilm method when 'ilm' is not in the plan."""
    plan_builder._plan.pop("ilm")
    plan_builder.update_ilm()
    assert plan_builder._plan.ilm.enabled is False
    assert plan_builder._plan.ilm.phases == ["hot", "delete"]


def test_update_ilm_ilm_is_bool(plan_builder):
    """Test PlanBuilder update_ilm method when 'ilm' is True"""
    plan_builder._plan.pop("ilm")
    plan_builder._plan.ilm = True
    plan_builder.update_ilm()
    assert plan_builder._plan.ilm.enabled is False
    assert plan_builder._plan.ilm.phases == ["hot", "delete"]


def test_update_ilm_disabled(plan_builder):
    """Test PlanBuilder update_ilm method when ILM is disabled."""
    plan_builder._plan.ilm = DotMap({"enabled": False})
    with patch("es_testbed._plan.build_ilm_policy") as mock_build_ilm_policy:
        plan_builder.update_ilm()
        mock_build_ilm_policy.assert_not_called()


def test_update_ilm_no_phases(plan_builder):
    """Test PlanBuilder update_ilm method with no phases."""
    plan_builder._plan.ilm.enabled = True
    assert plan_builder.plan.ilm.enabled
    plan_builder.plan.ilm.phases = None  # Override the defaults to be empty
    plan_builder.update_ilm()
    assert len(plan_builder._plan.ilm.policy["phases"].keys()) == 2
    assert "hot" in plan_builder._plan.ilm.policy["phases"]
    assert "delete" in plan_builder._plan.ilm.policy["phases"]


def test_update_ilm_existing_phases(plan_builder):
    """Test PlanBuilder update_ilm method with existing phases."""
    plan_builder._plan.ilm.enabled = True
    plan_builder.update_ilm()
    assert len(plan_builder._plan.ilm.policy["phases"].keys()) == 2
    assert "warm" not in plan_builder._plan.ilm.policy["phases"]
    plan_builder._plan.ilm.phases.append("warm")
    plan_builder.update_ilm()
    assert len(plan_builder._plan.ilm.policy["phases"].keys()) == 3
    assert "warm" in plan_builder._plan.ilm.policy["phases"]


def test_update_rollover_alias(plan_builder):
    """Test PlanBuilder update_rollover_alias method."""
    plan_builder._plan.prefix = "test-prefix"
    plan_builder._plan.uniq = "test-uniq"
    plan_builder._plan.rollover_alias = True
    plan_builder.update_rollover_alias()
    assert plan_builder._plan.rollover_alias == "test-prefix-idx-test-uniq"

    plan_builder._plan.rollover_alias = False
    plan_builder.update_rollover_alias()
    assert plan_builder._plan.rollover_alias is None
