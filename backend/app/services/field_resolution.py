"""字段路径解析与取值明细：数据转换与接入质量校验共用的字段读取层。

收拢 data_transform 与 data_quality_rule 各自的字段取值逻辑：
- 字段路径解析（点号/方括号）只保留这一份实现；
- 取值策略按调用方区分：转换嵌套优先（字面键兜底）、质检仅按字面键，
  各自单独执行时的取值与空值行为与重构前完全一致；
- 每次解析产出 FieldResolution 明细，记录实际读取位置、键是否存在
  （区分"键不存在"与"值为 null"）以及读到的值，供转换凭证持久化、
  质检复验按位置重读。
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

# 读取位置前缀：nested 按嵌套路径逐层读取；literal 按整串字面键读取；missing 键不存在
LOCATION_NESTED = "nested"
LOCATION_LITERAL = "literal"
LOCATION_MISSING = "missing"

# 转换凭证与来源行版本登记使用的存储分区（转换与质检共用）
VOUCHER_MODULE = "transform_voucher"
SOURCE_ROW_MODULE = "source_row_registry"


@dataclass
class FieldResolution:
    """字段解析明细：一次字段取值的位置与结果记录。

    found=False 表示键不存在；found=True 且 is_null=True 表示值为 null。
    error 在运行时为异常对象（由转换侧按原行为重新抛出），序列化后为
    {"type": ..., "message": ...} 字典。
    """

    field_path: str                 # 调用方给出的原始字段路径/键
    segments: list[str]             # 解析后的路径段
    strategy: str                   # 取值策略：nested_first / literal / location
    read_location: str              # 实际读取位置，如 nested:user.name / literal:user.name / missing:user.name
    found: bool                     # 键是否存在
    is_null: bool                   # 键存在但值为 null
    value: Any = None               # 读取到的值（缺失或出错时为 None）
    error: Any = None               # 取值异常（运行时异常对象 / 序列化字典）
    target_field: str = ""          # 关联的转换目标字段（转换场景）

    def to_dict(self) -> dict[str, Any]:
        if self.error is None or isinstance(self.error, dict):
            error_dict = self.error
        else:
            error_dict = {"type": type(self.error).__name__, "message": str(self.error)}
        return {
            "field_path": self.field_path,
            "segments": list(self.segments),
            "strategy": self.strategy,
            "read_location": self.read_location,
            "found": self.found,
            "is_null": self.is_null,
            "value": self.value,
            "error": error_dict,
            "target_field": self.target_field,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FieldResolution":
        return cls(
            field_path=str(data.get("field_path", "")),
            segments=list(data.get("segments", [])),
            strategy=str(data.get("strategy", "")),
            read_location=str(data.get("read_location", "")),
            found=bool(data.get("found", False)),
            is_null=bool(data.get("is_null", False)),
            value=data.get("value"),
            error=data.get("error"),
            target_field=str(data.get("target_field", "")),
        )


def parse_field_path(path: str) -> list[str]:
    """解析字段路径为路径段列表。

    支持点号和方括号：
    - "name" → ["name"]
    - "user.profile.email" → ["user", "profile", "email"]
    - "items[0].id" → ["items", "[0]", "id"]
    """
    parts = re.split(r"\.|(?<=\w)(?=\[)", str(path))
    return [part.strip() for part in parts if part.strip()]


def resolve_nested_first(data: dict[str, Any], path: str) -> FieldResolution:
    """转换侧取值：嵌套路径优先，整串字面键兜底。

    语义与原 data_transform._extract_value 完全一致：
    - 逐段按嵌套读取；某段缺失时回退为当前层的整串路径字面键；
    - 路径缺失时值为 None 且 found=False，值本身为 None 时 found=True、is_null=True；
    - 途中遇到非字典结构按原样抛出 KeyError/TypeError 等（记入 error，
      由调用方重新抛出以保持原有异常行为）。
    """
    segments = parse_field_path(path)
    current: Any = data
    used_literal = False
    found = True
    error: Exception | None = None
    try:
        for seg in segments:
            if "[" in seg and "]" in seg:
                key, idx_str = seg.split("[", 1)
                idx = int(idx_str.rstrip("]"))
                if key:
                    current = current[key]
                current = current[idx]
            elif isinstance(current, dict):
                if seg in current:
                    current = current[seg]
                elif path in current:
                    current = current[path]
                    used_literal = True
                else:
                    current = None
                    found = False
            else:
                raise KeyError(f"无法从 {type(current)} 中提取 {seg}")
    except (KeyError, ValueError, TypeError, IndexError) as exc:
        error = exc
        found = False
        current = None

    if error is not None or not found:
        read_location = f"{LOCATION_MISSING}:{path}"
    elif used_literal:
        read_location = f"{LOCATION_LITERAL}:{path}"
    else:
        read_location = f"{LOCATION_NESTED}:{path}"

    return FieldResolution(
        field_path=path,
        segments=segments,
        strategy="nested_first",
        read_location=read_location,
        found=found,
        is_null=found and current is None,
        value=current,
        error=error,
    )


def resolve_literal(data: dict[str, Any], field: str) -> FieldResolution:
    """质检侧取值：仅按整串字面键读取，不展开嵌套路径。"""
    found = field in data
    value = data.get(field)
    return FieldResolution(
        field_path=field,
        segments=[field],
        strategy="literal",
        read_location=f"{LOCATION_LITERAL}:{field}" if found else f"{LOCATION_MISSING}:{field}",
        found=found,
        is_null=found and value is None,
        value=value,
    )


def read_at_location(data: dict[str, Any], read_location: str) -> FieldResolution:
    """按持久化的读取位置从给定输入重读字段值（复验专用）。

    严格按位置读取，不做策略回退：nested 只走嵌套路径，literal 只查字面键，
    missing 直接返回键不存在。保证复验读取的位置与当时实际读取的位置一致。
    """
    kind, _, ref = read_location.partition(":")
    if kind == LOCATION_LITERAL:
        resolution = resolve_literal(data, ref)
    elif kind == LOCATION_NESTED:
        resolution = _resolve_strict_nested(data, ref)
    else:
        resolution = FieldResolution(
            field_path=ref,
            segments=parse_field_path(ref),
            strategy="location",
            read_location=read_location,
            found=False,
            is_null=False,
            value=None,
        )
    resolution.strategy = "location"
    resolution.read_location = read_location
    return resolution


def _resolve_strict_nested(data: dict[str, Any], path: str) -> FieldResolution:
    """严格按嵌套路径读取（不做字面键回退），任一段缺失即视为键不存在。"""
    segments = parse_field_path(path)
    current: Any = data
    found = True
    try:
        for seg in segments:
            if "[" in seg and "]" in seg:
                key, idx_str = seg.split("[", 1)
                idx = int(idx_str.rstrip("]"))
                if key:
                    current = current[key]
                current = current[idx]
            elif isinstance(current, dict) and seg in current:
                current = current[seg]
            else:
                found = False
                current = None
                break
    except (KeyError, ValueError, TypeError, IndexError):
        found = False
        current = None
    return FieldResolution(
        field_path=path,
        segments=segments,
        strategy="location",
        read_location=f"{LOCATION_NESTED}:{path}" if found else f"{LOCATION_MISSING}:{path}",
        found=found,
        is_null=found and current is None,
        value=current,
    )
