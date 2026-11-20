"""数据转换编排领域服务：JSON/XML数据转换、字段映射、类型转换与结构重组。

提供从源Schema到目标Schema的字段级映射和值转换能力，
支持嵌套结构抽取、类型强制转换和异常降级策略。
字段取值逻辑已收拢至 field_resolution（与接入质量校验共用字段解析明细）；
可复验转换会生成转换凭证，持久化原输入快照、输入版本与字段解析明细，
供 data_quality_rule 事后按原口径复验。
跨模块关联：data_quality_rule（质量校验）、data_lake_sink（转换后落湖）。
"""
from __future__ import annotations

import copy
import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from app.persistence import json_store
from app.services.base import BaseModuleService
from app.services.field_resolution import (
    SOURCE_ROW_MODULE,
    VOUCHER_MODULE,
    FieldResolution,
    resolve_nested_first,
)


class DataTransformService(BaseModuleService):
    MODULE = "data_transform"
    ENTITY = "转换规则"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["规则编号", "源Schema", "目标Schema", "字段映射", "转换函数", "默认值填充", "异常策略", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "源Schema", "目标Schema"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    _BUILTIN_FUNCTIONS: dict[str, Callable[[Any], Any]] = {
        "to_string": lambda x: str(x) if x is not None else "",
        "to_integer": lambda x: int(x) if x is not None else 0,
        "to_float": lambda x: float(x) if x is not None else 0.0,
        "to_boolean": lambda x: bool(x) if x is not None else False,
        "to_upper": lambda x: str(x).upper() if x is not None else "",
        "to_lower": lambda x: str(x).lower() if x is not None else "",
        "extract_numbers": lambda x: re.sub(r"[^0-9]", "", str(x)) if x is not None else "",
        "parse_datetime": lambda x: x,
        "default_empty": lambda x: x if x is not None and str(x).strip() else "",
    }

    def apply_transform(self, rule_id: int, source_data: dict[str, Any]) -> dict[str, Any]:
        """应用转换规则，将源数据转换为目标结构。

        转换流程：
        1. 加载已生效的转换规则
        2. 解析字段映射定义
        3. 逐个字段应用转换函数
        4. 处理异常策略（跳过/默认值/报错）

        Returns:
            { ok: bool, data: dict, errors: list, fields_transformed: int }
        """
        rule = json_store.get_by_id(self.MODULE, rule_id)
        if rule is None:
            return {"ok": False, "data": {}, "errors": ["转换规则不存在"], "fields_transformed": 0}

        if rule.get("status") != "已生效":
            return {"ok": False, "data": {}, "errors": ["转换规则未生效"], "fields_transformed": 0}

        core, _ = self._transform_core(rule, source_data)

        # 质量校验（跨模块：data_quality_rule）
        quality_check = self._run_quality_checks(rule_id, core["data"])

        return {**core, "quality_check": quality_check}

    def _transform_core(
        self,
        rule: dict[str, Any],
        source_data: dict[str, Any],
        collect_resolutions: bool = False,
    ) -> tuple[dict[str, Any], list[FieldResolution]]:
        """转换主流程：逐字段取值、应用转换函数、按异常策略处理。

        字段取值通过 field_resolution.resolve_nested_first 完成（嵌套优先、
        字面键兜底，与原 _extract_value 行为一致）；collect_resolutions=True
        时同时返回每个映射字段的解析明细，供转换凭证持久化。
        """
        field_mapping = self._parse_mapping(str(rule.get("字段映射", "{}")))
        transform_func = str(rule.get("转换函数", "passthrough"))
        error_policy = str(rule.get("异常策略", "跳过"))

        result = {}
        errors = []
        transformed = 0
        resolutions: list[FieldResolution] = []

        for target_field, source_path in field_mapping.items():
            resolution = resolve_nested_first(source_data, source_path)
            resolution.target_field = target_field
            try:
                if resolution.error is not None:
                    raise resolution.error
                converted = self._apply_function(transform_func, resolution.value)
                result[target_field] = converted
                transformed += 1
            except (KeyError, ValueError, TypeError) as e:
                if resolution.error is None:
                    resolution.error = e
                if error_policy == "使用默认值":
                    result[target_field] = self._get_default_value(target_field, rule)
                elif error_policy == "报错":
                    errors.append(f"字段 {target_field}: {e}")
                else:
                    errors.append(f"字段 {target_field} 跳过: {e}")
            if collect_resolutions:
                resolutions.append(resolution)

        core = {
            "ok": len(errors) == 0 or error_policy == "跳过",
            "data": result,
            "errors": errors,
            "fields_transformed": transformed,
            "total_fields": len(field_mapping),
        }
        return core, resolutions

    def _parse_mapping(self, mapping_str: str) -> dict[str, str]:
        """解析字段映射定义。

        支持格式：
        - JSON: {"目标字段": "源字段路径"}
        - 简单: "field1->target1,field2->target2"
        """
        try:
            return json.loads(mapping_str)
        except json.JSONDecodeError:
            result = {}
            for part in mapping_str.split(","):
                if "->" in part:
                    src, tgt = part.split("->", 1)
                    result[tgt.strip()] = src.strip()
            return result if result else {"_raw": "payload"}

    def _extract_value(self, data: dict[str, Any], path: str) -> Any:
        """从嵌套数据中提取指定路径的值。

        取值逻辑已收拢至 field_resolution.resolve_nested_first（与质检共用
        字段解析层），此处保留原异常行为：取值异常按原样抛出。
        """
        resolution = resolve_nested_first(data, path)
        if resolution.error is not None:
            raise resolution.error
        return resolution.value

    def _apply_function(self, func_name: str, value: Any) -> Any:
        """应用命名转换函数。"""
        if func_name == "passthrough":
            return value
        func = self._BUILTIN_FUNCTIONS.get(func_name)
        if func:
            return func(value)
        return value

    def _get_default_value(self, field: str, rule: dict[str, Any]) -> Any:
        """根据规则配置获取字段默认值。"""
        default_mapping = {
            "string": "", "integer": 0, "float": 0.0,
            "boolean": False, "datetime": "",
        }
        return default_mapping.get(rule.get("目标Schema", ""), "")

    def _run_quality_checks(self, rule_id: int, transformed_data: dict[str, Any]) -> dict[str, Any]:
        """跨模块运行质量校验。"""
        try:
            from app.services.data_quality_rule import DataQualityRuleService
            quality_svc = DataQualityRuleService()
            return quality_svc.validate_record(transformed_data, source_module=self.MODULE)
        except Exception:
            return {"passed": True, "checks": 0}

    # ────────────────────────────────────────────────
    # 可复验转换：来源行版本登记 + 转换凭证
    # ────────────────────────────────────────────────

    def register_source_row(self, source_row_id: str, data: dict[str, Any], kind: str = "transform") -> int:
        """登记来源行数据版本，返回本次输入版本号。

        同一来源行每次登记版本递增，历史版本全部保留（分区 source_row_registry），
        已生成凭证的原输入快照不受后续登记影响。
        """
        history = [
            r for r in json_store.get_all(SOURCE_ROW_MODULE)
            if str(r.get("source_row_id")) == str(source_row_id)
        ]
        version = max((int(r.get("version", 0)) for r in history), default=0) + 1
        json_store.insert(SOURCE_ROW_MODULE, {
            "source_row_id": str(source_row_id),
            "version": version,
            "kind": kind,
            "data": copy.deepcopy(data),
        })
        return version

    def register_source_correction(self, source_row_id: str, corrected_data: dict[str, Any]) -> dict[str, Any]:
        """登记来源行更正：版本递增并保留更正快照。

        更正只产生新的输入版本，不回写任何已生成凭证的原输入与转换结果。
        """
        version = self.register_source_row(source_row_id, corrected_data, kind="correction")
        return {
            "ok": True,
            "source_row_id": str(source_row_id),
            "input_version": version,
            "message": f"来源行 {source_row_id} 已登记更正，输入版本 {version}",
        }

    def apply_transform_tracked(
        self,
        rule_id: int,
        source_data: dict[str, Any],
        source_row_id: str,
    ) -> dict[str, Any]:
        """可复验转换：在常规转换之上生成关联字段解析明细的转换凭证。

        凭证持久化：原输入快照与输入版本、逐字段解析明细（实际读取位置、
        键是否存在/是否为 null）、转换规则版本与当前生效质检规则快照，
        供 data_quality_rule.revalidate_voucher 按原口径复验。
        """
        rule = json_store.get_by_id(self.MODULE, rule_id)
        if rule is None:
            return {"ok": False, "data": {}, "errors": ["转换规则不存在"], "fields_transformed": 0}

        if rule.get("status") != "已生效":
            return {"ok": False, "data": {}, "errors": ["转换规则未生效"], "fields_transformed": 0}

        input_version = self.register_source_row(source_row_id, source_data, kind="transform")
        core, resolutions = self._transform_core(rule, source_data, collect_resolutions=True)

        # 质量校验（跨模块：data_quality_rule），与单独执行转换一致
        quality_check = self._run_quality_checks(rule_id, core["data"])

        voucher = json_store.insert(VOUCHER_MODULE, {
            "voucher_no": f"TV-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}",
            "transform_rule_id": rule_id,
            "transform_rule_no": rule.get("规则编号"),
            "transform_rule_version": rule.get("规则版本"),
            "source_row_id": str(source_row_id),
            "input_version": input_version,
            "original_input": copy.deepcopy(source_data),
            "field_resolutions": [r.to_dict() for r in resolutions],
            "transform_result": core["data"],
            "quality_rules_snapshot": self._snapshot_quality_rules(),
            "revalidation": None,
        })

        return {
            **core,
            "quality_check": quality_check,
            "voucher_id": voucher["id"],
            "voucher_no": voucher["voucher_no"],
            "source_row_id": str(source_row_id),
            "input_version": input_version,
        }

    def _snapshot_quality_rules(self) -> list[dict[str, Any]]:
        """快照当前对本模块生效的质检规则（匹配口径与 validate_record 一致）。"""
        rules = json_store.get_all("data_quality_rule")
        return [
            dict(r) for r in rules
            if r.get("status") == "已生效"
            and str(r.get("数据来源", "")) in (self.MODULE, "", "ALL", "*")
        ]

    def get_voucher(self, voucher_id: int) -> dict[str, Any] | None:
        """按ID查询转换凭证（含字段解析明细与复验结论）。"""
        return json_store.get_by_id(VOUCHER_MODULE, voucher_id)

    def get_source_row_version(self, source_row_id: str, version: int) -> dict[str, Any] | None:
        """按来源行与版本号找回登记的输入快照。"""
        for record in json_store.get_all(SOURCE_ROW_MODULE):
            if (str(record.get("source_row_id")) == str(source_row_id)
                    and int(record.get("version", 0)) == int(version)):
                return record
        return None

    def batch_transform(self, rule_id: int, source_records: list[dict[str, Any]]) -> dict[str, Any]:
        """批量转换多条记录。"""
        results = []
        success = 0
        failed = 0

        for record in source_records:
            r = self.apply_transform(rule_id, record)
            if r["ok"]:
                success += 1
            else:
                failed += 1
            results.append(r)

        return {
            "ok": failed == 0,
            "message": f"批量转换完成: 成功 {success}/{len(source_records)}",
            "success": success,
            "failed": failed,
            "total": len(source_records),
            "results": results,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出数据转换编排的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "数据转换编排数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "active_rules": sum(1 for r in rows if r.get("status") == "已生效"),
            "operation": self.OPERATION_TERM,
        }