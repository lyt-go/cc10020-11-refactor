"""字段解析明细与转换凭证复验测试。

覆盖重构要求：
1. 转换与质检共用字段解析层，单独执行时各自取值/空值行为不变；
2. 字面键 user.name 与 user 对象下的 name 同时存在时两处取值不同；
3. 串联复验持久化实际读取位置与输入版本，区分键不存在与值为 null；
4. 同一凭证重复复验只记一份结果（幂等）；
5. 转换后来源行被更正：原凭证复验仍依据当时原输入与规则版本，
   更正后的输入另做转换，其质检结论单独保留，两次复验均可追溯规则与字段。
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.persistence import JsonStore
from app.services.field_resolution import (
    SOURCE_ROW_MODULE,
    VOUCHER_MODULE,
    parse_field_path,
    read_at_location,
    resolve_literal,
    resolve_nested_first,
)
from app.services.data_transform import DataTransformService
from app.services.data_quality_rule import DataQualityRuleService


@pytest.fixture(autouse=True)
def _isolated_store(monkeypatch, tmp_path):
    """每个测试使用独立的临时存储，避免互相污染。"""
    store = JsonStore(base_dir=str(tmp_path))
    import app.services.data_transform as dt_mod
    import app.services.data_quality_rule as dq_mod
    monkeypatch.setattr(dt_mod, "json_store", store)
    monkeypatch.setattr(dq_mod, "json_store", store)
    yield store


def _make_transform_rule(store, mapping='{"target_name": "user.name"}', rule_version="v1"):
    return store.insert("data_transform", {
        "规则编号": "TRANS-REVAL-001",
        "源Schema": "json",
        "目标Schema": "json",
        "字段映射": mapping,
        "转换函数": "passthrough",
        "默认值填充": "使用默认值",
        "异常策略": "跳过",
        "规则版本": rule_version,
        "status": "已生效",
        "pending": False,
        "abnormal": False,
    })


def _make_quality_rule(store, field="user.name", upper="100", expression="is_numeric",
                       rule_version="v1", block="警告"):
    return store.insert("data_quality_rule", {
        "规则编号": "QUAL-REVAL-001",
        "数据来源": "ALL",
        "校验字段": field,
        "校验表达式": expression,
        "阈值下界": "0",
        "阈值上界": upper,
        "阻断策略": block,
        "规则版本": rule_version,
        "status": "已生效",
        "pending": False,
        "abnormal": False,
    })


# ──────────────────────────────────────────────────────────
# 共用字段解析层
# ──────────────────────────────────────────────────────────
def test_parse_field_path_shared():
    """路径解析只有一份实现，转换与质检共用。"""
    assert parse_field_path("name") == ["name"]
    assert parse_field_path("user.profile.email") == ["user", "profile", "email"]
    assert parse_field_path("items[0].id") == ["items", "[0]", "id"]


def test_literal_key_vs_nested_value_differ():
    """同一输入行中字面键 user.name 与嵌套 user.name 的值可不同。"""
    row = {"user.name": "literal_value", "user": {"name": "nested_value"}}

    nested = resolve_nested_first(row, "user.name")
    assert nested.value == "nested_value"
    assert nested.read_location == "nested:user.name"
    assert nested.found and not nested.is_null

    literal = resolve_literal(row, "user.name")
    assert literal.value == "literal_value"
    assert literal.read_location == "literal:user.name"

    # 两处值确实不同
    assert nested.value != literal.value


def test_nested_first_falls_back_to_literal_key():
    """转换策略：嵌套不存在时回退到整串字面键（原 _extract_value 行为）。"""
    row = {"user.name": "only_literal"}
    resolution = resolve_nested_first(row, "user.name")
    # 第一段 user 缺失 → 回退到字面键 user.name；继续解析 name 段时
    # 当前值不是 dict，与原实现一样抛出 KeyError
    assert resolution.error is not None
    assert isinstance(resolution.error, KeyError)

    # 单段路径直接命中字面键
    single = resolve_nested_first({"name": "x"}, "name")
    assert single.value == "x"
    assert single.found


def test_missing_vs_null_distinguished():
    """字段解析明细区分键不存在与值为 null。"""
    missing = resolve_nested_first({"a": 1}, "b")
    assert not missing.found
    assert not missing.is_null
    assert missing.value is None
    assert missing.read_location == "missing:b"

    null_value = resolve_nested_first({"a": None}, "a")
    assert null_value.found
    assert null_value.is_null
    assert null_value.value is None

    lit_missing = resolve_literal({}, "k")
    assert not lit_missing.found and not lit_missing.is_null

    lit_null = resolve_literal({"k": None}, "k")
    assert lit_null.found and lit_null.is_null


def test_read_at_location_strict():
    """按持久化位置重读：nested 只走嵌套，literal 只查字面键。"""
    row = {"user.name": 5, "user": {"name": 150}}
    assert read_at_location(row, "nested:user.name").value == 150
    assert read_at_location(row, "literal:user.name").value == 5
    missing = read_at_location(row, "missing:user.name")
    assert not missing.found


# ──────────────────────────────────────────────────────────
# 单独执行转换/质检：输入输出与取值行为不变
# ──────────────────────────────────────────────────────────
def test_standalone_transform_behavior_unchanged():
    svc = DataTransformService()
    rule = _make_transform_rule(
        _store(), '{"target_id": "id", "target_name": "name", "target_value": "value"}')

    source = {"id": 1001, "name": "Test Record", "value": 42.5, "extra": "ignored"}
    result = svc.apply_transform(rule_id=rule["id"], source_data=source)

    assert result["ok"]
    assert set(result.keys()) == {
        "ok", "data", "errors", "fields_transformed", "total_fields", "quality_check",
    }
    assert result["data"]["target_name"] == "Test Record"
    assert result["fields_transformed"] == 3
    assert "quality_check" in result


def test_standalone_transform_reads_nested_first():
    svc = DataTransformService()
    rule = _make_transform_rule(_store())
    row = {"user.name": "literal_value", "user": {"name": "nested_value"}}
    result = svc.apply_transform(rule_id=rule["id"], source_data=row)
    # 转换沿用原有嵌套优先取值
    assert result["data"]["target_name"] == "nested_value"


def test_standalone_quality_reads_literal_key():
    svc = DataQualityRuleService()
    _make_quality_rule(_store(), field="user.name", upper="100", expression="is_numeric")
    row = {"user.name": 50, "user": {"name": 150}}
    result = svc.validate_record(row, source_module="test")
    # 质检沿用原有字面键取值：50 在限值内；若误读嵌套值 150 会超限
    assert result["passed"]
    assert set(result.keys()) == {
        "passed", "checks", "failed", "passed_checks", "details", "score",
    }


def test_standalone_quality_empty_field():
    svc = DataQualityRuleService()
    store = _store()
    store.insert("data_quality_rule", {
        "规则编号": "QUAL-EMPTY",
        "数据来源": "ALL",
        "校验字段": "name",
        "校验表达式": "not_empty",
        "阈值下界": "0",
        "阈值上界": "0",
        "阻断策略": "警告",
        "规则版本": "v1",
        "status": "已生效",
        "pending": False,
        "abnormal": False,
    })
    assert not svc.validate_record({"name": ""}, source_module="test")["passed"]
    assert svc.validate_record({"name": "ok"}, source_module="test")["passed"]


def _store():
    """取当前测试隔离后的存储实例。"""
    import app.services.data_transform as dt_mod
    return dt_mod.json_store


# ──────────────────────────────────────────────────────────
# 转换凭证：字段解析明细与输入版本持久化
# ──────────────────────────────────────────────────────────
def test_tracked_transform_persists_voucher_with_resolutions():
    svc = DataTransformService()
    store = _store()
    rule = _make_transform_rule(store)
    _make_quality_rule(store)

    row = {"user.name": 50, "user": {"name": 150}, "extra": None}
    result = svc.apply_transform_tracked(rule_id=rule["id"], source_data=row, source_row_id="ROW-1")

    assert result["ok"]
    assert result["voucher_id"]
    assert result["input_version"] == 1
    # 转换结果仍按嵌套优先取值
    assert result["data"]["target_name"] == 150

    voucher = svc.get_voucher(result["voucher_id"])
    assert voucher["source_row_id"] == "ROW-1"
    assert voucher["input_version"] == 1
    assert voucher["original_input"] == row
    assert voucher["transform_rule_version"] == "v1"
    assert voucher["revalidation"] is None

    # 字段解析明细：记录实际读取位置，区分 found / is_null
    details = voucher["field_resolutions"]
    assert len(details) == 1
    detail = details[0]
    assert detail["field_path"] == "user.name"
    assert detail["target_field"] == "target_name"
    assert detail["read_location"] == "nested:user.name"
    assert detail["found"] and not detail["is_null"]
    assert detail["value"] == 150

    # 质检规则快照随凭证持久化
    assert len(voucher["quality_rules_snapshot"]) == 1
    assert voucher["quality_rules_snapshot"][0]["规则版本"] == "v1"


def test_tracked_transform_records_missing_and_null():
    svc = DataTransformService()
    store = _store()
    rule = _make_transform_rule(store, '{"has_value": "present", "null_value": "nullable", "gap": "absent"}')

    result = svc.apply_transform_tracked(
        rule_id=rule["id"],
        source_data={"present": 1, "nullable": None},
        source_row_id="ROW-NULL",
    )
    voucher = svc.get_voucher(result["voucher_id"])
    by_path = {d["field_path"]: d for d in voucher["field_resolutions"]}

    assert by_path["present"]["found"] and not by_path["present"]["is_null"]
    # 键存在但值为 null
    assert by_path["nullable"]["found"] and by_path["nullable"]["is_null"]
    # 键不存在
    assert not by_path["absent"]["found"]
    assert not by_path["absent"]["is_null"]
    assert by_path["absent"]["read_location"] == "missing:absent"


# ──────────────────────────────────────────────────────────
# 复验：按明细读取原输入与规则版本，幂等
# ──────────────────────────────────────────────────────────
def test_revalidate_reads_original_input_by_resolution():
    svc_t = DataTransformService()
    svc_q = DataQualityRuleService()
    store = _store()
    rule = _make_transform_rule(store)
    _make_quality_rule(store, upper="100")

    # 字面值 50 在限值内，嵌套值 150 超限；转换实际读的是嵌套值
    row = {"user.name": 50, "user": {"name": 150}}
    result = svc_t.apply_transform_tracked(rule_id=rule["id"], source_data=row, source_row_id="ROW-2")

    reval = svc_q.revalidate_voucher(result["voucher_id"])
    assert reval["ok"] and not reval["already_validated"]
    conclusion = reval["revalidation"]
    # 复验按明细的 nested 位置读取 → 150 超限
    assert not conclusion["passed"]
    detail = conclusion["details"][0]
    assert detail["read_location"] == "nested:user.name"
    assert detail["value"] == 150
    assert detail["found"] and not detail["is_null"]
    # 结论已写回凭证
    assert svc_t.get_voucher(result["voucher_id"])["revalidation"]["passed"] is False


def test_revalidate_idempotent_single_record():
    svc_t = DataTransformService()
    svc_q = DataQualityRuleService()
    store = _store()
    rule = _make_transform_rule(store)
    _make_quality_rule(store)

    result = svc_t.apply_transform_tracked(
        rule_id=rule["id"], source_data={"user": {"name": 10}}, source_row_id="ROW-3")
    voucher_id = result["voucher_id"]

    first = svc_q.revalidate_voucher(voucher_id)
    second = svc_q.revalidate_voucher(voucher_id)
    third = svc_q.revalidate_voucher(voucher_id)

    assert not first["already_validated"]
    assert second["already_validated"] and third["already_validated"]
    # 只记一份结果：内容完全一致，凭证上只有一条结论
    assert first["revalidation"] == second["revalidation"] == third["revalidation"]
    voucher = svc_t.get_voucher(voucher_id)
    assert voucher["revalidation"] == first["revalidation"]


def test_revalidate_voucher_not_found():
    svc_q = DataQualityRuleService()
    result = svc_q.revalidate_voucher(99999)
    assert not result["ok"]


# ──────────────────────────────────────────────────────────
# 核心场景：转换后来源行被更正
# ──────────────────────────────────────────────────────────
def test_correction_between_transform_and_revalidate():
    svc_t = DataTransformService()
    svc_q = DataQualityRuleService()
    store = _store()
    rule = _make_transform_rule(store)
    _make_quality_rule(store, upper="100")

    # 1. 原输入：字面值 50（限值内）与嵌套值 150（超限）分处两侧
    original = {"user.name": 50, "user": {"name": 150}}
    first = svc_t.apply_transform_tracked(rule_id=rule["id"], source_data=original, source_row_id="ROW-4")
    voucher_v1 = first["voucher_id"]
    assert first["input_version"] == 1

    # 2. 转换结束到复验开始之间，同一来源行收到更正（两侧值互换）
    corrected = {"user.name": 150, "user": {"name": 50}}
    corr = svc_t.register_source_correction("ROW-4", corrected)
    assert corr["input_version"] == 2

    # 3. 用原转换结果（凭证）发起复验：仍依据当时的原输入与规则版本
    reval_v1 = svc_q.revalidate_voucher(voucher_v1)
    conclusion_v1 = reval_v1["revalidation"]
    assert conclusion_v1["input_version"] == 1
    # 读的是 v1 的嵌套值 150 → 超限；不是更正后的 50
    assert conclusion_v1["details"][0]["value"] == 150
    assert not conclusion_v1["passed"]

    # 4. 更正没有写进原结果：凭证原输入与转换结果保持 v1
    voucher_after = svc_t.get_voucher(voucher_v1)
    assert voucher_after["original_input"] == original
    assert voucher_after["transform_result"]["target_name"] == 150

    # 5. 用更正后的输入另做一次转换，其质检结论单独保留
    second = svc_t.apply_transform_tracked(rule_id=rule["id"], source_data=corrected, source_row_id="ROW-4")
    voucher_v2 = second["voucher_id"]
    assert voucher_v2 != voucher_v1
    reval_v2 = svc_q.revalidate_voucher(voucher_v2)
    conclusion_v2 = reval_v2["revalidation"]
    # v2 的嵌套值是 50 → 通过
    assert conclusion_v2["details"][0]["value"] == 50
    assert conclusion_v2["passed"]

    # 6. 两次复验结论各自独立、互不覆盖
    assert svc_t.get_voucher(voucher_v1)["revalidation"]["passed"] is False
    assert svc_t.get_voucher(voucher_v2)["revalidation"]["passed"] is True

    # 7. 两次复验各能找回所用规则和字段
    for conclusion in (conclusion_v1, conclusion_v2):
        assert conclusion["rules_used"][0]["rule_no"] == "QUAL-REVAL-001"
        assert conclusion["rules_used"][0]["rule_version"] == "v1"
        fields = conclusion["fields_used"]
        assert fields[0]["field_path"] == "user.name"
        assert fields[0]["read_location"] == "nested:user.name"

    # 8. 输入版本历史可追溯
    assert svc_t.get_source_row_version("ROW-4", 1)["data"] == original
    assert svc_t.get_source_row_version("ROW-4", 2)["data"] == corrected


def test_revalidate_uses_rule_version_snapshot():
    """复验依据转换时的规则版本，而非复验时的当前规则。"""
    svc_t = DataTransformService()
    svc_q = DataQualityRuleService()
    store = _store()
    rule = _make_transform_rule(store)
    qr = _make_quality_rule(store, upper="100", rule_version="v1")

    row = {"user": {"name": 150}}
    result = svc_t.apply_transform_tracked(rule_id=rule["id"], source_data=row, source_row_id="ROW-5")

    # 转换后规则被修改：版本升级、阈值放宽到 1000
    store.update("data_quality_rule", qr["id"], {"阈值上界": "1000", "规则版本": "v2"})

    reval = svc_q.revalidate_voucher(result["voucher_id"])
    conclusion = reval["revalidation"]
    # 仍按 v1 规则（上界 100）判定 → 150 超限
    assert not conclusion["passed"]
    assert conclusion["rules_used"][0]["rule_version"] == "v1"
    assert "超过阈值 100.0" in conclusion["details"][0]["range_result"]["message"]


def test_revalidate_distinguishes_missing_and_null():
    """复验结论区分键不存在与值为 null。"""
    svc_t = DataTransformService()
    svc_q = DataQualityRuleService()
    store = _store()
    rule = _make_transform_rule(store, '{"got_null": "nullable", "got_gap": "absent"}')
    store.insert("data_quality_rule", {
        "规则编号": "QUAL-NULL",
        "数据来源": "ALL",
        "校验字段": "nullable",
        "校验表达式": "not_empty",
        "阈值下界": "0",
        "阈值上界": "0",
        "阻断策略": "警告",
        "规则版本": "v1",
        "status": "已生效",
        "pending": False,
        "abnormal": False,
    })
    store.insert("data_quality_rule", {
        "规则编号": "QUAL-GAP",
        "数据来源": "ALL",
        "校验字段": "absent",
        "校验表达式": "not_empty",
        "阈值下界": "0",
        "阈值上界": "0",
        "阻断策略": "警告",
        "规则版本": "v1",
        "status": "已生效",
        "pending": False,
        "abnormal": False,
    })

    result = svc_t.apply_transform_tracked(
        rule_id=rule["id"], source_data={"nullable": None}, source_row_id="ROW-6")
    conclusion = svc_q.revalidate_voucher(result["voucher_id"])["revalidation"]

    by_field = {d["field"]: d for d in conclusion["details"]}
    # 值为 null：found=True, is_null=True
    assert by_field["nullable"]["found"] and by_field["nullable"]["is_null"]
    assert not by_field["nullable"]["passed"]
    # 键不存在：found=False, is_null=False
    assert not by_field["absent"]["found"]
    assert not by_field["absent"]["is_null"]
    assert not by_field["absent"]["passed"]
