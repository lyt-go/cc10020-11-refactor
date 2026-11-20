"""接入质量校验领域服务：字段级校验、完整性检查、阈值告警与阻断策略。

提供转换后数据的质量校验能力，包括必填字段检查、类型校验、
数值范围校验和完整性评分。
字段取值逻辑已收拢至 field_resolution（与数据转换共用字段解析明细）；
复验场景按转换凭证持久化的字段解析明细读取原输入与字段规则，
结论写回凭证且幂等（同一凭证重复复验只记一份结果）。
跨模块关联：data_transform（转换后校验）、batch_ingest（批量质量检查）。
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService
from app.services.field_resolution import (
    VOUCHER_MODULE,
    FieldResolution,
    read_at_location,
    resolve_literal,
)


class DataQualityRuleService(BaseModuleService):
    MODULE = "data_quality_rule"
    ENTITY = "质量规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则编号", "数据来源", "校验字段", "校验表达式", "阈值下界", "阈值上界", "阻断策略", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "数据来源", "校验字段"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def __init__(self) -> None:
        super().__init__()
        self._validation_history: list[dict[str, Any]] = []

    def validate_record(self, record: dict[str, Any], source_module: str = "") -> dict[str, Any]:
        """对单条记录执行质量校验。

        校验流程：
        1. 加载与该来源相关的已生效规则
        2. 逐条规则执行校验表达式
        3. 检查数值范围（阈值上下界）
        4. 根据阻断策略决定是否拒绝

        Returns:
            { passed: bool, checks: int, failed: int, details: list, score: int }
        """
        rules = json_store.get_all(self.MODULE)
        matching = [
            r for r in rules
            if r.get("status") == "已生效"
            and (str(r.get("数据来源", "")) in (source_module, "", "ALL", "*")
                 or source_module == ""
                 or not source_module)
        ]

        if not matching:
            return {"passed": True, "checks": 0, "failed": 0, "details": [], "score": 100}

        results = []
        total_passed = 0
        total_failed = 0

        for rule in matching:
            field = str(rule.get("校验字段", ""))
            expression = str(rule.get("校验表达式", ""))
            block_policy = str(rule.get("阻断策略", "警告"))
            try:
                lower = float(rule.get("阈值下界", "0") or "0")
                upper = float(rule.get("阈值上界", "0") or "0")
            except ValueError:
                lower = 0
                upper = 0

            check_result = self._execute_check(record, field, expression)
            range_check = self._check_range(record, field, lower, upper)

            passed = check_result.get("passed", True) and range_check.get("passed", True)
            detail = {
                "rule_no": rule.get("规则编号"),
                "field": field,
                "expression": expression,
                "passed": passed,
                "expr_result": check_result,
                "range_result": range_check,
                "block_policy": block_policy,
            }
            results.append(detail)

            if passed:
                total_passed += 1
            else:
                total_failed += 1
                if block_policy == "阻断":
                    self._validation_history.append({
                        "rule": rule.get("规则编号"),
                        "field": field,
                        "outcome": "blocked",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })
                    return {
                        "passed": False,
                        "checks": len(matching),
                        "failed": total_failed,
                        "passed_checks": total_passed,
                        "details": results,
                        "score": max(0, 100 - total_failed * 10),
                        "blocked": True,
                    }

        self._validation_history.append({
            "record_preview": str(record)[:100],
            "checks": len(matching),
            "passed": total_passed,
            "failed": total_failed,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        return {
            "passed": total_failed == 0,
            "checks": len(matching),
            "failed": total_failed,
            "passed_checks": total_passed,
            "details": results,
            "score": max(0, 100 - total_failed * 10),
        }

    def _execute_check(self, record: dict[str, Any], field: str, expression: str) -> dict[str, Any]:
        """执行校验表达式（按字面键取值后求值）。"""
        resolution = resolve_literal(record, field)
        return self._evaluate_expression(resolution, expression)

    def _evaluate_expression(self, resolution: FieldResolution, expression: str) -> dict[str, Any]:
        """对已解析的字段值执行校验表达式。

        支持的表达式操作：
        - "not_empty": 非空检查
        - "is_numeric": 是否为数字
        - "pattern:xxx": 正则匹配
        - "in:val1,val2": 值域检查
        - "len_gt:N": 长度大于N
        """
        value = resolution.value
        field_present = resolution.found
        field = resolution.field_path

        if expression == "not_empty":
            if not field_present or value is None or str(value).strip() == "":
                return {"passed": False, "message": f"字段 {field} 为空"}
            return {"passed": True, "message": "通过"}

        if expression == "is_numeric":
            try:
                float(str(value))
                return {"passed": True, "message": "通过"}
            except (ValueError, TypeError):
                return {"passed": False, "message": f"字段 {field} 不是数字"}

        if expression.startswith("pattern:"):
            pattern = expression.split(":", 1)[1]
            if not re.match(pattern, str(value)):
                return {"passed": False, "message": f"字段 {field} 不匹配模式 {pattern}"}
            return {"passed": True, "message": "通过"}

        if expression.startswith("in:"):
            allowed = expression.split(":", 1)[1].split(",")
            if str(value) not in allowed:
                return {"passed": False, "message": f"字段 {field} 不在允许值 {allowed} 中"}
            return {"passed": True, "message": "通过"}

        if expression.startswith("len_gt:"):
            min_len = int(expression.split(":", 1)[1])
            if len(str(value or "")) < min_len:
                return {"passed": False, "message": f"字段 {field} 长度小于 {min_len}"}
            return {"passed": True, "message": "通过"}

        return {"passed": True, "message": "未识别表达式，默认通过"}

    def _check_range(self, record: dict[str, Any], field: str, lower: float, upper: float) -> dict[str, Any]:
        """检查字段值是否在阈值范围内（按字面键取值后求值）。"""
        resolution = resolve_literal(record, field)
        return self._evaluate_range(resolution.value, lower, upper)

    def _evaluate_range(self, value: Any, lower: float, upper: float) -> dict[str, Any]:
        """对已解析的字段值检查是否在阈值范围内。"""
        if lower == 0 and upper == 0:
            return {"passed": True, "message": "无范围限制"}

        try:
            num_value = float(str(value))
        except (ValueError, TypeError):
            return {"passed": True, "message": "非数值字段跳过范围检查"}

        if lower > 0 and num_value < lower:
            return {"passed": False, "message": f"值 {num_value} 低于阈值 {lower}"}
        if upper > 0 and num_value > upper:
            return {"passed": False, "message": f"值 {num_value} 超过阈值 {upper}"}

        return {"passed": True, "message": f"值 {num_value} 在范围 [{lower}, {upper}] 内"}

    def revalidate_voucher(self, voucher_id: int) -> dict[str, Any]:
        """按转换凭证复验：依据凭证关联的字段解析明细读取原输入与字段规则。

        复验口径：
        1. 原输入使用凭证持久化的快照与输入版本，不读取来源行后续更正；
        2. 字段值按明细记录的实际读取位置（nested/literal）从原输入快照重读；
        3. 质检规则使用凭证快照的规则与版本，而非当前规则；
        4. 结论写回凭证，同一凭证重复复验只记一份结果（幂等）。
        """
        voucher = json_store.get_by_id(VOUCHER_MODULE, voucher_id)
        if voucher is None:
            return {"ok": False, "message": f"转换凭证 {voucher_id} 不存在"}

        existing = voucher.get("revalidation")
        if existing:
            return {
                "ok": True,
                "already_validated": True,
                "voucher_id": voucher_id,
                "voucher_no": voucher.get("voucher_no"),
                "revalidation": existing,
                "message": "该凭证已复验，返回既有结论，不重复记录",
            }

        original_input = voucher.get("original_input") or {}
        rules_snapshot = voucher.get("quality_rules_snapshot") or []
        resolutions = [
            FieldResolution.from_dict(r) for r in voucher.get("field_resolutions", [])
        ]
        res_by_path = {r.field_path: r for r in resolutions}
        res_by_target = {r.target_field: r for r in resolutions if r.target_field}

        details = []
        total_passed = 0
        total_failed = 0
        blocked = False

        for rule in rules_snapshot:
            field = str(rule.get("校验字段", ""))
            expression = str(rule.get("校验表达式", ""))
            block_policy = str(rule.get("阻断策略", "警告"))
            try:
                lower = float(rule.get("阈值下界", "0") or "0")
                upper = float(rule.get("阈值上界", "0") or "0")
            except ValueError:
                lower = 0
                upper = 0

            # 按凭证明细记录的实际读取位置从原输入快照读值；
            # 明细未覆盖的字段回退为质检原有的字面键取值。
            stored = res_by_path.get(field) or res_by_target.get(field)
            if stored is not None and stored.read_location:
                resolution = read_at_location(original_input, stored.read_location)
                resolution.field_path = field
            else:
                resolution = resolve_literal(original_input, field)

            check_result = self._evaluate_expression(resolution, expression)
            range_check = self._evaluate_range(resolution.value, lower, upper)
            passed = check_result.get("passed", True) and range_check.get("passed", True)

            details.append({
                "rule_no": rule.get("规则编号"),
                "rule_version": rule.get("规则版本"),
                "field": field,
                "read_location": resolution.read_location,
                "found": resolution.found,
                "is_null": resolution.is_null,
                "value": resolution.value,
                "expression": expression,
                "passed": passed,
                "expr_result": check_result,
                "range_result": range_check,
                "block_policy": block_policy,
            })

            if passed:
                total_passed += 1
            else:
                total_failed += 1
                if block_policy == "阻断":
                    blocked = True
                    break

        conclusion = {
            "voucher_id": voucher_id,
            "voucher_no": voucher.get("voucher_no"),
            "source_row_id": voucher.get("source_row_id"),
            "input_version": voucher.get("input_version"),
            "passed": total_failed == 0 and not blocked,
            "blocked": blocked,
            "checks": len(rules_snapshot),
            "failed": total_failed,
            "passed_checks": total_passed,
            "score": max(0, 100 - total_failed * 10),
            "details": details,
            "rules_used": [
                {
                    "rule_no": r.get("规则编号"),
                    "rule_version": r.get("规则版本"),
                    "field": r.get("校验字段"),
                }
                for r in rules_snapshot
            ],
            "fields_used": [r.to_dict() for r in resolutions],
            "validated_at": datetime.now(timezone.utc).isoformat(),
        }

        json_store.update(VOUCHER_MODULE, voucher_id, {"revalidation": conclusion})
        self._validation_history.append({
            "voucher_id": voucher_id,
            "voucher_no": voucher.get("voucher_no"),
            "input_version": voucher.get("input_version"),
            "checks": len(rules_snapshot),
            "passed": total_passed,
            "failed": total_failed,
            "blocked": blocked,
            "revalidation": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        return {
            "ok": True,
            "already_validated": False,
            "voucher_id": voucher_id,
            "voucher_no": voucher.get("voucher_no"),
            "revalidation": conclusion,
        }

    def validate_batch(self, records: list[dict[str, Any]], source_module: str = "") -> dict[str, Any]:
        """批量质量校验。"""
        results = []
        passed_count = 0
        failed_count = 0
        blocked_count = 0

        for record in records:
            r = self.validate_record(record, source_module)
            if r.get("blocked"):
                blocked_count += 1
                failed_count += 1
            elif r["passed"]:
                passed_count += 1
            else:
                failed_count += 1
            results.append(r)

        avg_score = sum(r.get("score", 0) for r in results) / max(len(results), 1)

        return {
            "ok": blocked_count == 0,
            "total": len(records),
            "passed": passed_count,
            "failed": failed_count,
            "blocked": blocked_count,
            "average_score": round(avg_score, 2),
            "results": results,
        }

    def get_quality_report(self) -> dict[str, Any]:
        """获取质量校验报告。"""
        history = self._validation_history[-100:]
        rules = json_store.get_all(self.MODULE)
        active_rules = sum(1 for r in rules if r.get("status") == "已生效")

        return {
            "active_rules": active_rules,
            "total_checks": len(history),
            "recent_failures": sum(1 for h in history if h.get("failed", 0) > 0),
            "recent_blocks": sum(1 for h in history if h.get("blocked")),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出接入质量校验的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "接入质量校验数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_validations": len(self._validation_history),
            "active_rules": sum(1 for r in rows if r.get("status") == "已生效"),
            "operation": self.OPERATION_TERM,
        }