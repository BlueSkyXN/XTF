"""Native typed client for the Feishu Base v3 record/field APIs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .auth import FeishuAuth
from .base import RetryableAPIClient
from .bitable_backend import (
    BitableBackendKind,
    CanonicalRecord,
    FieldKind,
    FieldSchema,
    IncompleteReadError,
    MutationOutcome,
    MutationReceipt,
    ReadbackStatus,
    RecordReadResult,
    UserIDType,
    as_user_id_type,
    field_is_writable,
    field_kind_from_type,
)
from .sdk import FeishuAPIError
from .url import encode_path_segment


class BaseV3MatrixError(FeishuAPIError):
    """The Base v3 matrix or pagination contract was incomplete."""

    def __init__(self, message: str, *, response_data: Optional[Dict[str, Any]] = None):
        super().__init__(
            -1, message, response_data=response_data, kind="invalid_response"
        )


@dataclass(frozen=True)
class _MatrixPage:
    records: tuple[CanonicalRecord, ...]
    fields: tuple[FieldSchema, ...]
    timezone: str
    has_more: bool
    revision: int | str | None
    ignored_fields: tuple[Mapping[str, Any], ...]
    record_not_found: tuple[str, ...]
    raw: Mapping[str, Any]


class BaseV3Backend:
    """Strict Base v3 backend; it never delegates or falls back to v1."""

    api_family = BitableBackendKind.BASE_V3
    max_batch_create_size = 200
    max_batch_update_size = 200
    max_batch_delete_size = 200
    max_batch_get_size = 200
    max_page_size = 200

    def __init__(
        self,
        auth: FeishuAuth,
        api_client: Optional[RetryableAPIClient] = None,
        *,
        user_id_type: UserIDType | str = UserIDType.OPEN_ID,
    ) -> None:
        self.auth = auth
        self.api_client = api_client or auth.api_client
        self.user_id_type = as_user_id_type(user_id_type)
        self._field_cache: dict[tuple[str, str], tuple[FieldSchema, ...]] = {}

    @staticmethod
    def _base_path(base_token: str, table_id: str, *parts: str) -> str:
        segments = [
            "https://open.feishu.cn/open-apis/base/v3/bases",
            encode_path_segment(base_token),
            "tables",
            encode_path_segment(table_id),
        ]
        segments.extend(encode_path_segment(part) for part in parts)
        return "/".join(segments)

    @staticmethod
    def _require_envelope(result: Any) -> Dict[str, Any]:
        if not isinstance(result, dict):
            raise BaseV3MatrixError("Base v3 response envelope must be an object")
        if "code" not in result or isinstance(result["code"], bool):
            raise BaseV3MatrixError(
                "Base v3 response code must be numeric", response_data=result
            )
        try:
            code = int(result["code"])
        except (TypeError, ValueError) as exc:
            raise BaseV3MatrixError(
                "Base v3 response code must be numeric", response_data=result
            ) from exc
        if code != 0:
            raise FeishuAPIError(
                code,
                str(
                    result.get("msg")
                    or result.get("message")
                    or "Base v3 request failed"
                ),
                response_data=result,
                kind="api",
            )
        data = result.get("data")
        if not isinstance(data, dict):
            raise BaseV3MatrixError(
                "Base v3 response data must be an object", response_data=result
            )
        return data

    def _call(
        self,
        method: str,
        url: str,
        *,
        retry_transport: bool = True,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        response = self.api_client.call_api(
            method,
            url,
            headers=self.auth.get_auth_headers(),
            retry_transport=retry_transport,
            **kwargs,
        )
        try:
            payload = response.json()
        except ValueError as exc:
            raise BaseV3MatrixError("Base v3 response JSON is invalid") from exc
        # HTTP errors are deliberately classified before success envelope checks.
        if response.status_code >= 400:
            if isinstance(payload, dict):
                code = payload.get("code", response.status_code)
                try:
                    code = int(code)
                except (TypeError, ValueError):
                    code = response.status_code
                raise FeishuAPIError(
                    code,
                    str(
                        payload.get("msg")
                        or payload.get("message")
                        or f"HTTP {response.status_code}"
                    ),
                    http_status=response.status_code,
                    response_data=payload,
                )
            raise FeishuAPIError(
                response.status_code,
                f"HTTP {response.status_code}",
                http_status=response.status_code,
            )
        return self._require_envelope(payload)

    @staticmethod
    def _string_list(value: Any, name: str, *, allow_empty: bool = True) -> List[str]:
        if not isinstance(value, list) or any(
            not isinstance(item, str) or not item.strip() for item in value
        ):
            raise BaseV3MatrixError(f"{name} must be a list of non-empty strings")
        if not allow_empty and not value:
            raise BaseV3MatrixError(f"{name} must not be empty")
        return list(value)

    @staticmethod
    def _field_schema(name: str, field_id: str, raw_type: str, raw: Any) -> FieldSchema:
        kind = field_kind_from_type(raw_type)
        properties = raw if isinstance(raw, dict) else {}
        multiple = bool(
            properties.get(
                "multiple",
                kind in (FieldKind.USER, FieldKind.GROUP_CHAT, FieldKind.LINK),
            )
        )
        return FieldSchema(
            id=field_id,
            name=name,
            kind=kind,
            multiple=multiple,
            writable=field_is_writable(kind),
            raw_type=raw_type,
            raw_properties=properties,
        )

    @staticmethod
    def _canonical_cell(schema: FieldSchema, value: Any) -> Any:
        if value is None:
            if schema.kind in {
                FieldKind.SELECT,
                FieldKind.USER,
                FieldKind.GROUP_CHAT,
                FieldKind.LINK,
                FieldKind.ATTACHMENT,
            }:
                return []
            return None
        if schema.kind in (FieldKind.USER, FieldKind.GROUP_CHAT, FieldKind.LINK):
            if not isinstance(value, list):
                raise BaseV3MatrixError(
                    f"field {schema.name!r} canonical ID cell must be an array"
                )
            ids: List[str] = []
            for item in value:
                if not isinstance(item, dict) or not isinstance(item.get("id"), str):
                    raise BaseV3MatrixError(
                        f"field {schema.name!r} canonical ID cell item requires id"
                    )
                ids.append(item["id"])
            return ids
        return value

    @classmethod
    def _parse_matrix(cls, data: Dict[str, Any]) -> _MatrixPage:
        timezone = data.get("timezone")
        if not isinstance(timezone, str) or not timezone.strip():
            raise BaseV3MatrixError(
                "timezone must be a non-empty string", response_data=data
            )
        names = cls._string_list(data.get("fields"), "fields")
        field_ids = cls._string_list(data.get("field_id_list"), "field_id_list")
        field_types = cls._string_list(data.get("field_type_list"), "field_type_list")
        if not (len(names) == len(field_ids) == len(field_types)):
            raise BaseV3MatrixError(
                "fields, field_id_list, and field_type_list lengths differ",
                response_data=data,
            )
        raw_fields = data.get("field_schema", data.get("field_definitions", []))
        if raw_fields and (
            not isinstance(raw_fields, list) or len(raw_fields) != len(names)
        ):
            raise BaseV3MatrixError(
                "field schema definitions length differs", response_data=data
            )
        fields = tuple(
            cls._field_schema(
                name, field_id, field_type, raw_fields[index] if raw_fields else {}
            )
            for index, (name, field_id, field_type) in enumerate(
                zip(names, field_ids, field_types)
            )
        )
        record_ids = cls._string_list(data.get("record_id_list"), "record_id_list")
        rows = data.get("data")
        if not isinstance(rows, list) or any(not isinstance(row, list) for row in rows):
            raise BaseV3MatrixError("data must be an array of rows", response_data=data)
        if len(record_ids) != len(rows):
            raise BaseV3MatrixError(
                "record_id_list and data lengths differ", response_data=data
            )
        records: List[CanonicalRecord] = []
        for index, (record_id, row) in enumerate(zip(record_ids, rows), start=1):
            if len(row) != len(fields):
                raise BaseV3MatrixError(
                    f"data row {index} has {len(row)} cells; schema has {len(fields)} columns",
                    response_data=data,
                )
            records.append(
                CanonicalRecord(
                    record_id,
                    {
                        field.name: cls._canonical_cell(field, row[pos])
                        for pos, field in enumerate(fields)
                    },
                )
            )
        has_more = data.get("has_more")
        if not isinstance(has_more, bool):
            raise BaseV3MatrixError("has_more must be a boolean", response_data=data)
        revision = data.get("rev")
        if revision is not None and not isinstance(revision, (int, str)):
            raise BaseV3MatrixError(
                "rev must be an integer or string", response_data=data
            )
        ignored = data.get("ignored_fields", [])
        if ignored is None:
            ignored = []
        if not isinstance(ignored, list) or any(
            not isinstance(item, dict) for item in ignored
        ):
            raise BaseV3MatrixError(
                "ignored_fields must be a list of objects", response_data=data
            )
        missing = data.get("record_not_found", [])
        if missing is None:
            missing = []
        missing_ids = cls._string_list(missing, "record_not_found")
        return _MatrixPage(
            tuple(records),
            fields,
            timezone,
            has_more,
            revision,
            tuple(ignored),
            tuple(missing_ids),
            data,
        )

    @staticmethod
    def _schema_key(
        fields: Iterable[FieldSchema],
    ) -> frozenset[tuple[str | None, str, str]]:
        """Return a page-order-independent schema signature.

        The matrix response uses its own column order for each page.  A field
        order change is therefore harmless as long as the same field IDs keep
        the same name and type; rows are decoded against the current page's
        column order before this signature is compared.
        """

        return frozenset(
            (field.id, field.name, str(field.raw_type)) for field in fields
        )

    def list_fields(self, base_token: str, table_id: str) -> tuple[FieldSchema, ...]:
        offset = 0
        result: List[FieldSchema] = []
        page_limit = min(self.max_page_size, 100)
        total: Optional[int] = None
        while True:
            data = self._call(
                "GET",
                self._base_path(base_token, table_id, "fields"),
                params={"offset": offset, "limit": page_limit},
            )
            raw_fields = data.get("fields")
            if not isinstance(raw_fields, list):
                raise BaseV3MatrixError("fields response must contain a list")
            raw_total = data.get("total")
            if (
                isinstance(raw_total, bool)
                or not isinstance(raw_total, int)
                or raw_total < 0
            ):
                raise BaseV3MatrixError("fields total must be a non-negative integer")
            if total is None:
                total = raw_total
            elif raw_total != total:
                raise BaseV3MatrixError("fields total changed between pages")
            for item in raw_fields:
                if not isinstance(item, dict):
                    raise BaseV3MatrixError("field entry must be an object")
                field_id = item.get("id", item.get("field_id"))
                name = item.get("name", item.get("field_name"))
                raw_type = item.get("type")
                if (
                    not isinstance(field_id, str)
                    or not field_id.strip()
                    or not isinstance(name, str)
                    or not name.strip()
                    or not isinstance(raw_type, str)
                ):
                    raise BaseV3MatrixError(
                        "field entry requires id, name, and string type"
                    )
                result.append(self._field_schema(name, field_id, raw_type, item))
            if len(result) == total:
                schemas = tuple(result)
                self._field_cache[(base_token, table_id)] = schemas
                return schemas
            if not raw_fields:
                raise BaseV3MatrixError(
                    "fields pagination returned an empty page before total"
                )
            next_offset = offset + len(raw_fields)
            if next_offset <= offset:
                raise BaseV3MatrixError("fields pagination offset did not advance")
            if next_offset > total:
                raise BaseV3MatrixError("fields page exceeded declared total")
            offset = next_offset

    def _list_record_pages(
        self,
        base_token: str,
        table_id: str,
        *,
        field_names: Sequence[str] | None = None,
    ) -> RecordReadResult:
        offset = 0
        projection_ids: List[str] | None = None
        if field_names is not None:
            cached = self._field_cache.get((base_token, table_id))
            if cached is None:
                cached = self.list_fields(base_token, table_id)
            by_name = {field.name: field for field in cached}
            unknown = [name for name in field_names if name not in by_name]
            if unknown:
                raise ValueError(f"unknown projection field(s): {unknown!r}")
            projection_ids = [
                str(by_name[name].id) for name in field_names if by_name[name].id
            ]
        first_schema: Optional[frozenset[tuple[str | None, str, str]]] = None
        first_timezone: Optional[str] = None
        first_revision: int | str | None = None
        all_records: List[CanonicalRecord] = []
        ignored: List[Mapping[str, Any]] = []
        missing: List[str] = []
        last_fields: tuple[FieldSchema, ...] = ()
        while True:
            params: Dict[str, Any] = {"offset": offset, "limit": self.max_page_size}
            if projection_ids:
                params["field_id"] = projection_ids
            try:
                page_data = self._call(
                    "GET",
                    self._base_path(base_token, table_id, "records"),
                    params=params,
                )
                page = self._parse_matrix(page_data)
            except Exception as exc:
                if not all_records:
                    raise
                partial = RecordReadResult(
                    records=tuple(all_records),
                    fields=last_fields,
                    complete=False,
                    backend=self.api_family,
                    revision=first_revision,
                    timezone=first_timezone,
                    ignored_fields=tuple(ignored),
                    record_not_found=tuple(missing),
                    raw_metadata={"offset": offset, "cause": str(exc)},
                )
                raise IncompleteReadError(
                    "Base v3 records read is incomplete", partial
                ) from exc
            last_fields = page.fields
            schema = self._schema_key(page.fields)
            if first_schema is None:
                first_schema, first_timezone, first_revision = (
                    schema,
                    page.timezone,
                    page.revision,
                )
            elif (
                schema != first_schema
                or page.timezone != first_timezone
                or page.revision != first_revision
            ):
                raise BaseV3MatrixError(
                    "record schema, timezone, or revision changed between pages"
                )
            count = len(page.records)
            if page.has_more and count == 0:
                raise BaseV3MatrixError(
                    "record pagination returned an empty page with has_more=true"
                )
            all_records.extend(page.records)
            ignored.extend(page.ignored_fields)
            missing.extend(page.record_not_found)
            if not page.has_more:
                return RecordReadResult(
                    records=tuple(all_records),
                    fields=page.fields,
                    complete=True,
                    backend=self.api_family,
                    revision=first_revision,
                    timezone=first_timezone,
                    ignored_fields=tuple(ignored),
                    record_not_found=tuple(missing),
                    raw_metadata=dict(page.raw),
                )
            next_offset = offset + count
            if next_offset <= offset:
                raise BaseV3MatrixError("record pagination offset did not advance")
            offset = next_offset

    def list_records(
        self, base_token: str, table_id: str, field_names: Sequence[str] | None = None
    ) -> RecordReadResult:
        return self._list_record_pages(base_token, table_id, field_names=field_names)

    def create_field(
        self,
        base_token: str,
        table_id: str,
        field_name: str,
        field_type: int | str = 1,
    ) -> MutationReceipt:
        """Create one field; Base v3 has no multi-field create endpoint."""
        if not isinstance(field_name, str) or not field_name.strip():
            raise ValueError("field_name must be non-empty")
        if isinstance(field_type, int):
            field_type = {
                1: "text",
                2: "number",
                3: "select",
                4: "select",
                5: "datetime",
                7: "checkbox",
            }.get(field_type, "")
        if not isinstance(field_type, str) or not field_type.strip():
            raise ValueError("field_type must be a supported field type")
        body: Dict[str, Any] = {"name": field_name, "type": field_type}
        try:
            data = self._call(
                "POST", self._base_path(base_token, table_id, "fields"), json=body
            )
        except FeishuAPIError as exc:
            if exc.kind == "transport":
                return MutationReceipt(
                    operation="create_field",
                    backend=self.api_family,
                    requested_count=1,
                    outcome=MutationOutcome.UNKNOWN_OUTCOME,
                    readback=ReadbackStatus.UNKNOWN,
                    raw_metadata={"error": str(exc)},
                )
            raise
        self._field_cache.pop((base_token, table_id), None)
        return MutationReceipt(
            operation="create_field",
            backend=self.api_family,
            requested_count=1,
            accepted_count=1,
            outcome=MutationOutcome.ACCEPTED,
            raw_metadata=data,
        )

    def batch_get_records(
        self,
        base_token: str,
        table_id: str,
        record_ids: Sequence[str],
        field_names: Sequence[str] | None = None,
    ) -> RecordReadResult:
        self._validate_ids(record_ids, self.max_batch_get_size)
        body: Dict[str, Any] = {"record_id_list": list(record_ids)}
        if field_names is not None:
            body["select_fields"] = list(field_names)
        data = self._call(
            "POST",
            self._base_path(base_token, table_id, "records", "batch_get"),
            json=body,
        )
        page = self._parse_matrix(data)
        return RecordReadResult(
            records=page.records,
            fields=page.fields,
            complete=True,
            backend=self.api_family,
            revision=page.revision,
            timezone=page.timezone,
            ignored_fields=page.ignored_fields,
            record_not_found=page.record_not_found,
            raw_metadata=dict(page.raw),
        )

    @staticmethod
    def _validate_ids(record_ids: Sequence[str], limit: int) -> None:
        if len(record_ids) > limit:
            raise ValueError(f"batch record count cannot exceed {limit}")
        if any(
            not isinstance(record_id, str) or not record_id.strip()
            for record_id in record_ids
        ):
            raise ValueError("record IDs must be non-empty strings")

    @staticmethod
    def _validate_records(
        records: Sequence[CanonicalRecord], limit: int, *, require_ids: bool = False
    ) -> None:
        if len(records) > limit:
            raise ValueError(f"batch record count cannot exceed {limit}")
        for record in records:
            if not isinstance(record, CanonicalRecord) or not isinstance(
                record.fields, dict
            ):
                raise TypeError("records must contain CanonicalRecord values")
            if require_ids and (
                not isinstance(record.record_id, str) or not record.record_id.strip()
            ):
                raise ValueError("batch update records require non-empty record_id")

    def _encode_value(self, schema: FieldSchema | None, value: Any) -> Any:
        if schema is None:
            raise ValueError("field schema is required before mutation")
        if not schema.writable:
            raise ValueError(f"field {schema.name!r} is read-only")
        if schema.kind is FieldKind.ATTACHMENT:
            raise ValueError(f"field {schema.name!r} attachment writes are unsupported")
        if value is None or value == "" or value == []:
            return value
        if schema.kind is FieldKind.SELECT:
            if not isinstance(value, (list, tuple)):
                raise ValueError(
                    f"field {schema.name!r} select value must be a sequence"
                )
            values = list(value)
            if not schema.multiple and len(values) > 1:
                raise ValueError(
                    f"field {schema.name!r} single-select accepts at most one value"
                )
            return [str(item) for item in values]
        if schema.kind in (FieldKind.USER, FieldKind.GROUP_CHAT):
            if not isinstance(value, (list, tuple)):
                raise ValueError(f"field {schema.name!r} user value must be a sequence")
            values = list(value)
            encoded = []
            for item in values:
                if isinstance(item, dict) and isinstance(item.get("id"), str):
                    encoded.append(item)
                elif isinstance(item, str) and item.strip():
                    encoded.append({"id": item})
                else:
                    raise ValueError(
                        f"field {schema.name!r} user value must contain IDs"
                    )
            return encoded
        if schema.kind is FieldKind.LINK:
            if not isinstance(value, (list, tuple)):
                raise ValueError(f"field {schema.name!r} link value must be a sequence")
            values = list(value)
            encoded = []
            for item in values:
                if isinstance(item, dict) and isinstance(item.get("id"), str):
                    encoded.append(item)
                elif isinstance(item, str) and item.strip():
                    encoded.append({"id": item})
                else:
                    raise ValueError(
                        f"field {schema.name!r} link value must contain IDs"
                    )
            return encoded
        if schema.kind is FieldKind.LOCATION:
            if (
                not isinstance(value, dict)
                or set(value) != {"lng", "lat"}
                or any(
                    isinstance(value[key], bool)
                    or not isinstance(value[key], (int, float))
                    for key in ("lng", "lat")
                )
            ):
                raise ValueError(f"field {schema.name!r} location expects {{lng, lat}}")
            return value
        if schema.kind is FieldKind.DATETIME:
            if isinstance(value, datetime):
                return value.isoformat()
            if isinstance(value, str) and value.strip():
                return value
            raise ValueError(f"field {schema.name!r} datetime expects an ISO string")
        return value

    def _encode_record(
        self, base_token: str, table_id: str, record: CanonicalRecord
    ) -> dict[str, Any]:
        cache_key = (base_token, table_id)
        if cache_key not in self._field_cache:
            self.list_fields(base_token, table_id)
        schemas = {schema.name: schema for schema in self._field_cache[cache_key]}
        unknown = [name for name in record.fields if name not in schemas]
        if unknown:
            raise ValueError(f"unknown field(s) cannot be mutated: {unknown!r}")
        return {
            name: self._encode_value(schemas.get(name), value)
            for name, value in record.fields.items()
        }

    @staticmethod
    def _mutation_receipt(
        operation: str, requested: int, data: Dict[str, Any]
    ) -> MutationReceipt:
        ignored = data.get("ignored_fields", [])
        if ignored is None:
            ignored = []
        if not isinstance(ignored, list) or any(
            not isinstance(item, dict) for item in ignored
        ):
            raise BaseV3MatrixError(
                "ignored_fields must be a list of objects", response_data=data
            )
        missing = data.get("record_not_found", [])
        if missing is None:
            missing = []
        if not isinstance(missing, list) or any(
            not isinstance(item, str) for item in missing
        ):
            raise BaseV3MatrixError(
                "record_not_found must be a list of strings", response_data=data
            )
        created = data.get("record_id_list", [])
        if created is None:
            created = []
        if not isinstance(created, list) or any(
            not isinstance(item, str) for item in created
        ):
            raise BaseV3MatrixError(
                "record_id_list must be a list of strings", response_data=data
            )
        outcome = (
            MutationOutcome.PARTIAL if ignored or missing else MutationOutcome.ACCEPTED
        )
        accepted = len(created) if created else requested
        return MutationReceipt(
            operation=operation,
            backend=BitableBackendKind.BASE_V3,
            requested_count=requested,
            accepted_count=accepted,
            record_ids=tuple(created),
            ignored_fields=tuple(ignored),
            record_not_found=tuple(missing),
            outcome=outcome,
            raw_metadata=data,
        )

    def _mutation_call(
        self,
        operation: str,
        base_token: str,
        table_id: str,
        body: Dict[str, Any],
        requested: int,
    ) -> MutationReceipt:
        try:
            data = self._call(
                "POST",
                self._base_path(base_token, table_id, "records", operation),
                json=body,
                retry_transport=operation != "batch_create",
            )
        except FeishuAPIError as exc:
            if exc.kind == "transport":
                return MutationReceipt(
                    operation=operation,
                    backend=self.api_family,
                    requested_count=requested,
                    outcome=MutationOutcome.UNKNOWN_OUTCOME,
                    readback=ReadbackStatus.UNKNOWN,
                    raw_metadata={"error": str(exc)},
                )
            raise
        return self._mutation_receipt(operation, requested, data)

    def batch_create(
        self, base_token: str, table_id: str, records: Sequence[CanonicalRecord]
    ) -> MutationReceipt:
        self._validate_records(records, self.max_batch_create_size)
        return self._mutation_call(
            "batch_create",
            base_token,
            table_id,
            {
                "create_records": [
                    self._encode_record(base_token, table_id, record)
                    for record in records
                ]
            },
            len(records),
        )

    def batch_update(
        self, base_token: str, table_id: str, records: Sequence[CanonicalRecord]
    ) -> MutationReceipt:
        self._validate_records(records, self.max_batch_update_size, require_ids=True)
        body = {
            "update_records": {
                record.record_id: self._encode_record(base_token, table_id, record)
                for record in records
            }
        }
        return self._mutation_call(
            "batch_update", base_token, table_id, body, len(records)
        )

    def batch_delete(
        self, base_token: str, table_id: str, record_ids: Sequence[str]
    ) -> MutationReceipt:
        self._validate_ids(record_ids, self.max_batch_delete_size)
        return self._mutation_call(
            "batch_delete",
            base_token,
            table_id,
            {"record_id_list": list(record_ids)},
            len(record_ids),
        )


__all__ = ["BaseV3Backend", "BaseV3MatrixError"]
