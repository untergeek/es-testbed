"""Tests for the es_testbed.es_api module"""

# pylint: disable=C0115,C0116,R0903,R0913,R0917,W0212
from unittest.mock import MagicMock, patch, call
import re
import pytest
from elasticsearch8.exceptions import TransportError
from es_wait.exceptions import EsWaitFatal, EsWaitTimeout
from es_testbed.es_api import (
    change_ds,
    delete,
    exists,
    get,
    get_aliases,
    get_backing_indices,
    get_ilm,
    get_ilm_phases,
    get_write_index,
    ilm_explain,
    ilm_move,
    put_comp_tmpl,
    put_idx_tmpl,
    put_ilm,
    resolver,
    rollover,
    snapshot_name,
    wait_wrapper,
)
from es_testbed.exceptions import (
    NameChanged,
    ResultNotExpected,
    TestbedFailure,
    TestbedMisconfig,
)


def test_change_ds_success(client):
    change_ds(client, actions="test-actions")
    client.indices.modify_data_stream.assert_called_once_with(
        actions="test-actions", body=None
    )


def test_change_ds_exception(client):
    client.indices.modify_data_stream.side_effect = Exception("error")
    with pytest.raises(ResultNotExpected):
        change_ds(client, actions="test-actions")


def test_get_write_index(client):
    client.indices.get_alias.return_value = {
        "index1": {"aliases": {"test-alias": {"is_write_index": True}}},
        "index2": {"aliases": {"test-alias": {"is_write_index": False}}},
    }
    result = get_write_index(client, "test-alias")
    assert result == "index1"


def test_get_write_index_no_write_index(client):
    client.indices.get_alias.return_value = {
        "index1": {"aliases": {"test-alias": {"is_write_index": False}}},
        "index2": {"aliases": {"test-alias": {"is_write_index": False}}},
    }
    result = get_write_index(client, "test-alias")
    assert result is None


def test_resolver(client):
    client.indices.resolve_index.return_value = {
        "indices": ["index1"],
        "aliases": ["alias1"],
        "data_streams": ["data_stream1"],
    }
    result = resolver(client, "test-name")
    assert result == {
        "indices": ["index1"],
        "aliases": ["alias1"],
        "data_streams": ["data_stream1"],
    }


def test_rollover(client):
    rollover(client, "test-alias")
    client.indices.rollover.assert_called_once_with(
        alias="test-alias", wait_for_active_shards="all"
    )


def test_ilm_move(client):
    ilm_move(client, "test-index", {"phase": "hot"}, {"phase": "cold"})
    client.ilm.move_to_step.assert_called_once_with(
        index="test-index", current_step={"phase": "hot"}, next_step={"phase": "cold"}
    )


def test_ilm_move_exception(client, caplog):
    caplog.set_level(50)
    client.ilm.move_to_step.side_effect = Exception("error")
    with pytest.raises(ResultNotExpected):
        ilm_move(client, "test-index", {"phase": "hot"}, {"phase": "cold"})
        assert (
            "Unable to move index test-index to ILM next step: "
            "{'phase': 'cold'}" in caplog.text
        )


def test_put_comp_tmpl(client):
    with patch("es_testbed.es_api.wait_wrapper") as mock_wait_wrapper:
        put_comp_tmpl(client, "test-template", {"template": "data"})
        mock_wait_wrapper.assert_called_once()


def test_put_idx_tmpl(client):
    with patch("es_testbed.es_api.wait_wrapper") as mock_wait_wrapper:
        put_idx_tmpl(client, "test-template", ["pattern"], ["component"])
        mock_wait_wrapper.assert_called_once()


def test_put_ilm(client):
    put_ilm(client, "test-policy", {"policy": "data"})
    client.ilm.put_lifecycle.assert_called_once_with(
        name="test-policy", policy={"policy": "data"}
    )


def test_put_ilm_exception(client):
    client.ilm.put_lifecycle.side_effect = Exception("error")
    with pytest.raises(TestbedFailure):
        put_ilm(client, "test-policy", {"policy": "data"})


def test_snapshot_name(client):
    client.indices.get.return_value = {
        "test-index": {
            "settings": {
                "index": {"store": {"snapshot": {"snapshot_name": "snapshot1"}}}
            }
        }
    }
    with patch("es_testbed.es_api.exists", return_value=True):
        result = snapshot_name(client, "test-index")
        assert result == "snapshot1"


def test_snapshot_name_no_snapshot(client):
    client.indices.get.return_value = {"test-index": {"settings": {"index": {}}}}
    with patch("es_testbed.es_api.exists", return_value=True):
        with patch("es_testbed.es_api.logger.error") as mock_error:
            result = snapshot_name(client, "test-index")
            assert result is None
            mock_error.assert_called_once_with(
                "test-index is not a searchable snapshot"
            )


def test_get_aliases_keyerror(client):
    name = "test-index"
    client.indices.get.return_value = {name: {}}
    assert get_aliases(client, name) is None


def test_get_backing_indices_raises(client):
    name = "a"
    with patch(
        "es_testbed.es_api.resolver", return_value={"data_streams": ["a", "aa"]}
    ):
        with pytest.raises(ResultNotExpected) as err:
            get_backing_indices(client, name)
            assert (
                f"Expected only a single data_stream matching {name}"
                in err.value.args[0]
            )


def test_get_ilm_raises(client, caplog):
    caplog.set_level(50)
    client.ilm.get_lifecycle.side_effect = Exception("error")
    pattern = "test-index"
    with pytest.raises(ResultNotExpected):
        get_ilm(client, pattern)
        assert f"Unable to get ILM lifecycle matching {pattern}" in caplog.text


def test_get_ilm_phases_raises(client, caplog):
    caplog.set_level(50)
    name = "test-index"
    with patch("es_testbed.es_api.get_ilm", return_value={"not": "found"}):
        with pytest.raises(ResultNotExpected):
            get_ilm_phases(client, name)
            assert f"Unable to get ILM lifecycle named {name}." in caplog.text


class TestWaitWrapper:

    def test_wait_wrapper_success(self, client):
        wait_cls = MagicMock()
        wait_kwargs = {"name": "test-name", "kind": "index", "pause": 1}
        func = MagicMock()
        f_kwargs = {"name": "test-name"}
        wait_wrapper(client, wait_cls, wait_kwargs, func, f_kwargs)
        func.assert_called_once_with(**f_kwargs)
        wait_cls.assert_called_once_with(client, **wait_kwargs)
        wait_cls.return_value.wait.assert_called_once()

    def test_wait_wrapper_es_wait_fatal(self, client):
        wait_cls = MagicMock()
        wait_kwargs = {"name": "test-name", "kind": "index", "pause": 1}
        func = MagicMock()
        f_kwargs = {"name": "test-name"}
        wait_cls.return_value.wait.side_effect = EsWaitFatal(
            "fatal error", elapsed=10, errors=["error1"]
        )
        with pytest.raises(
            TestbedFailure, match="fatal error. Elapsed time: 10. Errors:"
        ):
            wait_wrapper(client, wait_cls, wait_kwargs, func, f_kwargs)

    def test_wait_wrapper_es_wait_timeout(self, client):
        wait_cls = MagicMock()
        wait_kwargs = {"name": "test-name", "kind": "index", "pause": 1}
        func = MagicMock()
        f_kwargs = {"name": "test-name"}
        wait_cls.return_value.wait.side_effect = EsWaitTimeout(
            "timeout error", 10.0, 10.0
        )
        with pytest.raises(
            TestbedFailure, match="timeout error. Elapsed time: 10.0. Timeout: 10.0"
        ):
            wait_wrapper(client, wait_cls, wait_kwargs, func, f_kwargs)

    def test_wait_wrapper_transport_error(self, client):
        wait_cls = MagicMock()
        wait_kwargs = {"name": "test-name", "kind": "index", "pause": 1}
        func = MagicMock()
        f_kwargs = {"name": "test-name"}
        func.side_effect = TransportError("error")
        with pytest.raises(
            TestbedFailure,
            match="Elasticsearch TransportError class exception encountered:",
        ):
            wait_wrapper(client, wait_cls, wait_kwargs, func, f_kwargs)

    def test_wait_wrapper_general_exception(self, client):
        wait_cls = MagicMock()
        wait_kwargs = {"name": "test-name", "kind": "index", "pause": 1}
        func = MagicMock()
        f_kwargs = {"name": "test-name"}
        err = "general error"
        msg = f"General Exception caught: \nException('{err}')"
        func.side_effect = Exception(err)
        with pytest.raises(TestbedFailure) as exc:
            wait_wrapper(client, wait_cls, wait_kwargs, func, f_kwargs)
            assert re.match(exc.value.args[0], msg, re.MULTILINE)


class TestIlmExplain:

    def test_ilm_explain(self, client):
        client.ilm.explain_lifecycle.return_value = {
            "indices": {"test-index": {"phase": "hot"}}
        }
        result = ilm_explain(client, "test-index")
        assert result == {"phase": "hot"}

    def test_ilm_explain_key_error(self, client, caplog):
        caplog.set_level(10)
        client.ilm.explain_lifecycle.return_value = {
            "indices": {"new-index": {"phase": "hot"}}
        }
        client.ilm.explain_lifecycle.side_effect = KeyError
        with pytest.raises(KeyError):
            assert ilm_explain(client, "test-index") == {"phase": "hot"}
            assert "Index name changed" in caplog.text

    def test_ilm_explain_not_found_error(self, client, caplog, notfound):
        caplog.set_level(30)  # warning
        client.ilm.explain_lifecycle.side_effect = notfound
        with pytest.raises(NameChanged):
            ilm_explain(client, "test-index")
            assert (
                "Datastream/Index Name changed. test-index was not found" in caplog.text
            )

    def test_ilm_explain_general_exception(self, client):
        client.ilm.explain_lifecycle.side_effect = Exception("error")
        with patch("es_testbed.es_api.logger.critical") as mock_critical:
            with pytest.raises(ResultNotExpected):
                ilm_explain(client, "test-index")
                mock_critical.assert_called_once_with(
                    "Unable to get ILM information for index test-index"
                )


class TestExists:

    def test_exists_snapshot(self, client):
        client.snapshot.get.return_value = {
            "snapshots": [{"snapshot": "test-snapshot"}]
        }
        assert exists(client, "snapshot", "test-snapshot", repository="test-repo")
        client.snapshot.get.assert_called_once_with(
            snapshot="test-snapshot", repository="test-repo"
        )

    def test_exists_ilm(self, client):
        client.ilm.get_lifecycle.return_value = {"test-ilm": {}}
        assert exists(client, "ilm", "test-ilm")
        client.ilm.get_lifecycle.assert_called_once_with(name="test-ilm")

    def test_exists_index(self, client):
        client.indices.exists.return_value = True
        assert exists(client, "index", "test-index")
        client.indices.exists.assert_called_once_with(index="test-index")

    def test_exists_data_stream(self, client):
        client.indices.exists.return_value = True
        assert exists(client, "data_stream", "test-data-stream")
        client.indices.exists.assert_called_once_with(index="test-data-stream")

    def test_exists_template(self, client):
        client.indices.exists_index_template.return_value = True
        assert exists(client, "template", "test-template")
        client.indices.exists_index_template.assert_called_once_with(
            name="test-template"
        )

    def test_exists_component(self, client):
        client.cluster.exists_component_template.return_value = True
        assert exists(client, "component", "test-component")
        client.cluster.exists_component_template.assert_called_once_with(
            name="test-component"
        )

    def test_exists_not_found_error(self, client, notfound):
        client.indices.exists.side_effect = notfound
        assert not exists(client, "index", "test-index")

    def test_exists_general_exception(self, client):
        client.indices.exists.side_effect = Exception("error")
        with pytest.raises(ResultNotExpected):
            exists(client, "index", "test-index")

    def test_exists_no_name(self, client):
        assert not exists(client, "index", None)


class TestDelete:

    def test_delete_snapshot_success(self, client):
        client.snapshot.delete.return_value = {"acknowledged": True}
        assert delete(client, "snapshot", "test-snapshot", repository="test-repo")

    def test_delete_index_success(self, client):
        client.indices.delete.return_value = {"acknowledged": True}
        assert delete(client, "index", "test-index")

    def test_delete_template_success(self, client):
        client.indices.delete_index_template.return_value = {"acknowledged": True}
        assert delete(client, "template", "test-template")

    def test_delete_component_success(self, client):
        client.cluster.delete_component_template.return_value = {"acknowledged": True}
        assert delete(client, "component", "test-component")

    def test_delete_ilm_success(self, client):
        client.ilm.delete_lifecycle.return_value = {"acknowledged": True}
        assert delete(client, "ilm", "test-ilm")

    def test_delete_not_found_error(self, client, notfound):
        client.indices.delete.side_effect = notfound
        with patch("es_testbed.es_api.logger.warning"):
            assert delete(client, "index", "test-index")

    def test_delete_general_exception(self, client):
        client.indices.delete.side_effect = Exception("error")
        with pytest.raises(ResultNotExpected):
            delete(client, "index", "test-index")

    def test_delete_verify_failure(self, client):
        client.indices.delete.return_value = {"acknowledged": False}
        with patch("es_testbed.es_api.verify", return_value=False) as mock_verify:
            assert not delete(client, "index", "test-index")
            mock_verify.assert_called_once_with(
                client, "index", "test-index", repository=None
            )

    def test_delete_none_name(self, client):
        with patch("es_testbed.es_api.debug.lv3") as mock_debug:
            assert not delete(client, "index", None)
            mock_debug.assert_has_calls([call('"index" has a None value for name')])


class TestGet:

    def test_get_snapshot_success(self, client):
        client.snapshot.get.return_value = {
            "snapshots": [{"snapshot": "test-snapshot"}]
        }
        result = get(client, "snapshot", "test-snapshot", repository="test-repo")
        assert result == ["test-snapshot"]
        client.snapshot.get.assert_called_once_with(
            snapshot="test-snapshot", repository="test-repo"
        )

    def test_get_index_success(self, client):
        client.indices.get.return_value = {"test-index": {}}
        result = get(client, "index", "test-index")
        assert result == ["test-index"]

    def test_get_template_success(self, client):
        client.indices.get_index_template.return_value = {
            "index_templates": [{"name": "test-template"}]
        }
        result = get(client, "template", "test-template")
        assert result == ["test-template"]
        client.indices.get_index_template.assert_called_once_with(name="test-template")

    def test_get_component_success(self, client):
        client.cluster.get_component_template.return_value = {
            "component_templates": [{"name": "test-component"}]
        }
        result = get(client, "component", "test-component")
        assert result == ["test-component"]
        client.cluster.get_component_template.assert_called_once_with(
            name="test-component"
        )

    def test_get_ilm_success(self, client):
        client.ilm.get_lifecycle.return_value = {"test-ilm": {}}
        result = get(client, "ilm", "test-ilm")
        assert result == ["test-ilm"]
        client.ilm.get_lifecycle.assert_called_once_with(name="test-ilm")

    def test_get_data_stream_success(self, client):
        client.indices.get_data_stream.return_value = {
            "data_streams": [{"name": "test-data-stream"}]
        }
        result = get(client, "data_stream", "test-data-stream")
        assert result == ["test-data-stream"]
        client.indices.get_data_stream.assert_called_once_with(
            name="test-data-stream", expand_wildcards=["open", "closed"]
        )

    def test_get_not_found_error(self, client, notfound):
        client.indices.get.side_effect = notfound
        assert get(client, "index", "test-index") == []

    def test_get_general_exception(self, client):
        client.indices.get.side_effect = Exception("error")
        with pytest.raises(ResultNotExpected):
            get(client, "index", "test-index")

    def test_get_pattern_is_none(self, client, caplog):
        caplog.set_level(40)
        kind = "template"
        with pytest.raises(TestbedMisconfig):
            get(client, kind, None)
            assert f'"{kind}" has a None value for pattern' in caplog.text
