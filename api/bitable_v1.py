"""Typed Bitable v1 backend with direct ownership of its wire contract."""

from __future__ import annotations

import datetime as dt
import logging
import math
import time
import uuid
from typing import Any, Dict, List, Mapping, Optional, Sequence

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
    field_is_writable,
    field_kind_from_type,
    as_user_id_type,
)
from .sdk import FeishuAPIError, FeishuResponseParser, Page, Paginator
from .url import encode_path_segment


class BitableV1Backend:
    """Typed v1 client owning pagination, business retry, and wire parsing."""

    api_family = BitableBackendKind.BITABLE_V1
    max_page_size = 100
    max_batch_create_size = 1000
    max_batch_update_size = 1000
    max_batch_delete_size = 500
    max_batch_get_size = 100

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
        self.logger = logging.getLogger("XTF.bitable_v1")
        self._field_cache: dict[tuple[str, str], tuple[FieldSchema, ...]] = {}

    def _call(
        self,
        method: str,
        url: str,
        *,
        retry_transport: bool = True,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Share a single attempt budget across HTTP and business retries."""
        from .base import RequestAttemptBudget

        max_retries = getattr(self.api_client, "max_retries", 3)
        if not isinstance(max_retries, int):
            max_retries = 3
        budget = RequestAttemptBudget(max(0, max_retries) + 1)
        logger = logging.getLogger("XTF.v1")
        for attempt in range(max(0, max_retries) + 1):
            before = budget.remaining
            response = self.api_client.call_api(
                method,
                url,
                headers=self.auth.get_auth_headers(),
                retry_transport=retry_transport,
                attempt_budget=budget,
                **kwargs,
            )
            # Alternate transports may not implement shared accounting.
            if budget.remaining == before:
                budget.consume()
            try:
                result = FeishuResponseParser.parse(response)
                return result
            except FeishuAPIError as error:
                retryable_business_error = (
                    error.http_status is not None
                    and error.http_status < 400
                    and error.code in FeishuResponseParser.RETRYABLE_BIZ_CODES
                )
                if not retryable_business_error or not budget.remaining:
                    raise
                wait_time = (
                    error.retry_after
                    if error.retry_after is not None
                    else float(2**attempt)
                )
                logger.warning(
                    "v1 业务错误码 %s，等待 %ss 后重试", error.code, wait_time
                )
                time.sleep(wait_time)
        raise RuntimeError("v1 retry loop exited unexpectedly")

    @staticmethod
    def _page_data(result: Mapping[str, Any]) -> Dict[str, Any]:
        data = result.get("data")
        if not isinstance(data, dict):
            raise FeishuAPIError(
                -1, "v1 page data must be an object", kind="invalid_response"
            )
        if "items" not in data or "has_more" not in data:
            raise FeishuAPIError(
                -1, "v1 page must include items and has_more", kind="invalid_response"
            )
        items = data["items"]
        has_more = data["has_more"]
        page_token = data.get("page_token")
        if not isinstance(items, list):
            raise FeishuAPIError(
                -1, "v1 page items must be a list", kind="invalid_response"
            )
        if not isinstance(has_more, bool):
            raise FeishuAPIError(
                -1, "v1 page has_more must be boolean", kind="invalid_response"
            )
        if page_token is not None and not isinstance(page_token, str):
            raise FeishuAPIError(
                -1, "v1 page_token must be a string or null", kind="invalid_response"
            )
        return data

    @staticmethod
    def _field_schema(field: Dict[str, Any]) -> FieldSchema:
        raw_type = field.get("type")
        kind = field_kind_from_type(raw_type)
        raw_properties = field.get("property")
        if not isinstance(raw_properties, dict):
            raw_properties = {}
        # v1 uses 3/4 for single/multi select.  Keep the distinction in the
        # canonical ``multiple`` bit while retaining the raw numeric type.
        multiple = raw_type == 4
        return FieldSchema(
            id=(
                field.get("field_id")
                if isinstance(field.get("field_id"), str)
                else None
            ),
            name=str(field.get("field_name", "")),
            kind=kind,
            multiple=multiple,
            writable=field_is_writable(kind),
            raw_type=raw_type if isinstance(raw_type, (int, str)) else None,
            raw_properties=raw_properties,
        )

    @staticmethod
    def _canonical_value(schema: FieldSchema, value: Any) -> Any:
        if value is None:
            return [] if schema.multiple else None
        if schema.kind is FieldKind.TEXT and isinstance(value, list):
            parts = []
            for item in value:
                if isinstance(item, dict) and isinstance(item.get("text"), str):
                    parts.append(item["text"])
                else:
                    raise FeishuAPIError(
                        -1,
                        f"v1 text field {schema.name!r} has invalid rich-text cell",
                        kind="invalid_response",
                    )
            return "".join(parts)
        if schema.kind in (FieldKind.USER, FieldKind.GROUP_CHAT):
            if not isinstance(value, list):
                raise FeishuAPIError(
                    -1,
                    f"v1 ID field {schema.name!r} must be an array",
                    kind="invalid_response",
                )
            ids: List[str] = []
            for item in value:
                if not isinstance(item, dict) or not isinstance(item.get("id"), str):
                    raise FeishuAPIError(
                        -1,
                        f"v1 ID field {schema.name!r} item requires id",
                        kind="invalid_response",
                    )
                ids.append(item["id"])
            return ids
        if schema.kind is FieldKind.LINK and isinstance(value, dict):
            links = value.get("link_record_ids")
            if not isinstance(links, list) or any(
                not isinstance(item, str) for item in links
            ):
                raise FeishuAPIError(
                    -1,
                    f"v1 link field {schema.name!r} has invalid link_record_ids",
                    kind="invalid_response",
                )
            return links
        if schema.kind is FieldKind.SELECT:
            return (
                list(value) if schema.multiple and isinstance(value, list) else [value]
            )
        return value

    @classmethod
    def _record(
        cls, record: Dict[str, Any], schemas: Mapping[str, FieldSchema]
    ) -> CanonicalRecord:
        record_id = record.get("record_id")
        if record_id is not None and not isinstance(record_id, str):
            raise FeishuAPIError(
                -1, "v1 record_id 必须是字符串", kind="invalid_response"
            )
        fields = record.get("fields", {})
        if not isinstance(fields, dict):
            raise FeishuAPIError(
                -1, "v1 record fields 必须是对象", kind="invalid_response"
            )
        return CanonicalRecord(
            record_id=record_id,
            fields={
                name: (
                    cls._canonical_value(schemas[name], value)
                    if name in schemas
                    else value
                )
                for name, value in fields.items()
            },
        )

    def list_fields(self, app_token: str, table_id: str) -> tuple[FieldSchema, ...]:
        app = encode_path_segment(app_token)
        table = encode_path_segment(table_id)
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/fields"

        def fetch(page_token: Optional[str]) -> Page[Dict[str, Any]]:
            params: Dict[str, Any] = {"page_size": self.max_page_size}
            if page_token:
                params["page_token"] = page_token
            data = self._page_data(self._call("GET", url, params=params))
            items = data.get("items", [])
            if any(not isinstance(item, dict) for item in items):
                raise FeishuAPIError(
                    -1, "v1 field items must be objects", kind="invalid_response"
                )
            return Page(
                items=items,
                next_page_token=data.get("page_token"),
                has_more=data.get("has_more", False),
                raw=data,
            )

        schemas = tuple(
            self._field_schema(item)
            for item in Paginator[Dict[str, Any]]().collect(fetch)
        )
        self._field_cache[(app_token, table_id)] = schemas
        return schemas

    def _schemas(self, app_token: str, table_id: str) -> tuple[FieldSchema, ...]:
        return self._field_cache.get((app_token, table_id), ())

    @staticmethod
    def _has_value(value: Any) -> bool:
        return value is not None and value != "" and value != []

    def _encode_value(self, schema: FieldSchema | None, value: Any) -> Any:
        """Encode canonical values to the v1 cell-value shape."""

        if schema is None:
            raise ValueError("field schema is required before mutation")
        if not schema.writable:
            raise ValueError(f"field {schema.name!r} is read-only")
        if schema.kind is FieldKind.ATTACHMENT:
            # Attachments require an upload flow; ordinary record sync must not
            # pretend that a token/string is a complete attachment payload.
            raise ValueError(f"field {schema.name!r} attachment writes are unsupported")
        if not self._has_value(value):
            return value
        if schema.kind is FieldKind.NUMBER:
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise ValueError(f"field {schema.name!r} expects a finite number")
        if schema.kind is FieldKind.CHECKBOX and not isinstance(value, bool):
            raise ValueError(f"field {schema.name!r} expects a boolean")
        if schema.kind is FieldKind.SELECT:
            if schema.multiple:
                if not isinstance(value, (list, tuple)):
                    raise ValueError(
                        f"field {schema.name!r} multi-select expects a list"
                    )
                return [str(item) for item in value]
            if isinstance(value, (list, tuple)):
                if len(value) > 1:
                    raise ValueError(
                        f"field {schema.name!r} single-select accepts at most one value"
                    )
                return str(value[0]) if value else None
            return str(value)
        if schema.kind in (FieldKind.USER, FieldKind.GROUP_CHAT):
            values = value if isinstance(value, (list, tuple)) else [value]
            if not schema.multiple and len(values) > 1:
                raise ValueError(f"field {schema.name!r} accepts at most one ID")
            encoded: List[Dict[str, Any]] = []
            for item in values:
                if (
                    isinstance(item, dict)
                    and isinstance(item.get("id"), str)
                    and item["id"].strip()
                ):
                    encoded.append({"id": item["id"]})
                elif isinstance(item, str) and item.strip():
                    encoded.append({"id": item})
                else:
                    raise ValueError(
                        f"field {schema.name!r} user value must contain IDs"
                    )
            return encoded
        if schema.kind is FieldKind.LINK:
            values = value if isinstance(value, (list, tuple)) else [value]
            link_ids: List[str] = []
            for item in values:
                if (
                    isinstance(item, dict)
                    and isinstance(item.get("id"), str)
                    and item["id"].strip()
                ):
                    link_ids.append(item["id"])
                elif isinstance(item, str) and item.strip():
                    link_ids.append(item)
                else:
                    raise ValueError(
                        f"field {schema.name!r} link value must contain IDs"
                    )
            return link_ids
        if schema.kind is FieldKind.DATETIME:
            if isinstance(value, str):
                value = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
            if isinstance(value, dt.datetime):
                if value.tzinfo is None:
                    value = value.replace(tzinfo=dt.timezone.utc)
                delta = value.astimezone(dt.timezone.utc) - dt.datetime(
                    1970, 1, 1, tzinfo=dt.timezone.utc
                )
                return (
                    delta.days * 86400 + delta.seconds
                ) * 1000 + delta.microseconds // 1000
            if (
                not isinstance(value, bool)
                and isinstance(value, (int, float))
                and math.isfinite(value)
                and value == int(value)
            ):
                return int(value)
            raise ValueError(
                f"field {schema.name!r} datetime expects ISO date or integer milliseconds"
            )
        if schema.kind is FieldKind.LOCATION and not isinstance(value, (str, dict)):
            raise ValueError(f"field {schema.name!r} location value must be an object")
        return value

    def validate_records(
        self,
        records: Sequence[CanonicalRecord],
        fields: Sequence[FieldSchema],
    ) -> None:
        from .bitable_backend import validate_record_values

        validate_record_values(records, fields, self._encode_value)

    def _encode_record(
        self, app_token: str, table_id: str, record: CanonicalRecord
    ) -> dict[str, Any]:
        cache_key = (app_token, table_id)
        if cache_key not in self._field_cache:
            self.list_fields(app_token, table_id)
        schemas = {schema.name: schema for schema in self._field_cache[cache_key]}
        unknown = [name for name in record.fields if name not in schemas]
        if unknown:
            raise ValueError(f"unknown field(s) cannot be mutated: {unknown!r}")
        fields = {
            name: self._encode_value(schemas[name], value)
            for name, value in record.fields.items()
        }
        encoded: dict[str, Any] = {"fields": fields}
        if record.record_id is not None:
            encoded["record_id"] = record.record_id
        return encoded

    def create_field(
        self,
        app_token: str,
        table_id: str,
        field_name: str,
        field_type: int | str = 1,
    ) -> MutationReceipt:
        """Create one v1 field and return a typed receipt."""

        app = encode_path_segment(app_token)
        table = encode_path_segment(table_id)
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/fields"
        try:
            result = self._call(
                "POST",
                url,
                retry_transport=False,
                json={"field_name": field_name, "type": field_type},
            )
        except FeishuAPIError as exc:
            if exc.mutation_outcome_unknown:
                return self._unknown_receipt("create_field", 1, cause=exc)
            raise
        self._field_cache.pop((app_token, table_id), None)
        return MutationReceipt(
            operation="create_field",
            backend=self.api_family,
            requested_count=1,
            accepted_count=1,
            unit="field",
            outcome=MutationOutcome.ACCEPTED,
            raw_metadata=result.get("data", {}) if isinstance(result, dict) else {},
        )

    def _search_pages(
        self,
        app_token: str,
        table_id: str,
        field_names: Optional[List[str]],
    ) -> List[Dict[str, Any]]:
        app = encode_path_segment(app_token)
        table = encode_path_segment(table_id)
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/records/search"

        def fetch(token: Optional[str]) -> Page[Dict[str, Any]]:
            params: Dict[str, Any] = {"page_size": self.max_page_size}
            if token:
                params["page_token"] = token
            body: Dict[str, Any] = {}
            if field_names is not None:
                body["field_names"] = field_names
            data = self._page_data(self._call("POST", url, params=params, json=body))
            items = data.get("items", [])
            if any(not isinstance(item, dict) for item in items):
                raise FeishuAPIError(
                    -1, "v1 record items must be objects", kind="invalid_response"
                )
            return Page(
                items=items,
                next_page_token=data.get("page_token"),
                has_more=data.get("has_more", False),
                raw=data,
            )

        return Paginator[Dict[str, Any]]().collect(fetch)

    def list_records(
        self,
        app_token: str,
        table_id: str,
        field_names: Sequence[str] | None = None,
    ) -> RecordReadResult:
        fields = tuple(self.list_fields(app_token, table_id))
        schemas = {field.name: field for field in fields}
        try:
            records = tuple(
                self._record(item, schemas)
                for item in self._search_pages(
                    app_token,
                    table_id,
                    list(field_names) if field_names is not None else None,
                )
            )
        except Exception as exc:
            partial = RecordReadResult(
                records=(),
                fields=(),
                complete=False,
                backend=self.api_family,
                raw_metadata={"cause": str(exc)},
            )
            raise IncompleteReadError(
                "Bitable v1 records read is incomplete", partial
            ) from exc
        return RecordReadResult(
            records=records,
            fields=fields,
            complete=True,
            backend=self.api_family,
            raw_metadata={
                "field_names": list(field_names) if field_names is not None else None
            },
        )

    def batch_get_records(
        self,
        app_token: str,
        table_id: str,
        record_ids: Sequence[str],
        field_names: Sequence[str] | None = None,
    ) -> RecordReadResult:
        """Read up to 100 v1 records by ID using the documented batch_get API."""
        self._validate_ids(record_ids, self.max_batch_get_size)
        app = encode_path_segment(app_token)
        table = encode_path_segment(table_id)
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/records/batch_get"
        body: Dict[str, Any] = {
            "record_ids": list(record_ids),
            "user_id_type": self.user_id_type.value,
        }
        envelope = self._call(
            "POST",
            url,
            json=body,
        )
        data = envelope.get("data")
        if not isinstance(data, dict):
            raise FeishuAPIError(
                -1, "v1 batch_get data 必须是对象", kind="invalid_response"
            )
        raw_records = data.get("records")
        if not isinstance(raw_records, list) or any(
            not isinstance(record, dict) for record in raw_records
        ):
            raise FeishuAPIError(
                -1, "v1 batch_get records 必须是对象数组", kind="invalid_response"
            )
        fields = self._schemas(app_token, table_id) or self.list_fields(
            app_token, table_id
        )
        schemas = {field.name: field for field in fields}
        selected = tuple(self._record(record, schemas) for record in raw_records)
        absent = data.get("absent_record_ids", [])
        forbidden = data.get("forbidden_record_ids", [])
        if (
            not isinstance(absent, list)
            or any(not isinstance(item, str) for item in absent)
            or not isinstance(forbidden, list)
            or any(not isinstance(item, str) for item in forbidden)
        ):
            raise FeishuAPIError(
                -1,
                "v1 batch_get absent/forbidden IDs 必须是字符串数组",
                kind="invalid_response",
            )
        if field_names is not None:
            allowed = set(field_names)
            selected = tuple(
                CanonicalRecord(
                    record.record_id,
                    {
                        name: value
                        for name, value in record.fields.items()
                        if name in allowed
                    },
                )
                for record in selected
            )
        return RecordReadResult(
            records=selected,
            fields=fields,
            complete=True,
            backend=self.api_family,
            record_not_found=tuple(absent),
            raw_metadata={"targeted": True, "forbidden_record_ids": forbidden},
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
    def _validate_records(records: Sequence[CanonicalRecord], limit: int) -> None:
        if len(records) > limit:
            raise ValueError(f"batch record count cannot exceed {limit}")
        for record in records:
            if not isinstance(record, CanonicalRecord):
                raise TypeError("typed backend expects CanonicalRecord values")
            if record.record_id is not None and not record.record_id.strip():
                raise ValueError("record IDs must be non-empty strings")
            if not isinstance(record.fields, dict):
                raise ValueError("record fields must be an object")

    def _mutation_call(
        self,
        operation: str,
        app_token: str,
        table_id: str,
        body: Dict[str, Any],
        requested: int,
        *,
        client_token: bool = False,
    ) -> MutationReceipt:
        app = encode_path_segment(app_token)
        table = encode_path_segment(table_id)
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/records/{operation}"
        params: Dict[str, str] = {"user_id_type": self.user_id_type.value}
        if client_token:
            params["client_token"] = str(uuid.uuid4())
            params["ignore_consistency_check"] = "true"
        try:
            result = self._call(
                "POST",
                url,
                params=params,
                json=body,
            )
        except FeishuAPIError as exc:
            if exc.mutation_outcome_unknown:
                return self._unknown_receipt(operation, requested, cause=exc)
            raise
        try:
            data = result.get("data")
            if not isinstance(data, dict):
                raise ValueError("v1 mutation data 必须是对象")
            ignored = data.get("ignored_fields", [])
            missing = data.get("record_not_found", [])
            if not isinstance(ignored, list) or any(
                not isinstance(x, dict) for x in ignored
            ):
                raise ValueError("ignored_fields 必须为对象数组")
            if not isinstance(missing, list) or any(
                not isinstance(x, str) for x in missing
            ):
                raise ValueError("record_not_found 必须为字符串数组")
            records = data.get("records")
            record_ids: List[str] = []
            rejected: List[str] = list(missing)
            # Compatibility with existing v1 gateways which return the ID list.
            if records == [] and data.get("record_id_list"):
                records = None
            if records is not None:
                if not isinstance(records, list):
                    raise ValueError("v1 mutation records 必须为数组")
                seen: set[str] = set()
                for record in records:
                    if not isinstance(record, dict):
                        raise ValueError("v1 mutation record 必须为对象")
                    record_id = record.get("record_id")
                    if (
                        not isinstance(record_id, str)
                        or not record_id
                        or record_id in seen
                    ):
                        raise ValueError("v1 mutation record_id 缺失或重复")
                    seen.add(record_id)
                    if operation == "batch_delete":
                        deleted = record.get("deleted")
                        if not isinstance(deleted, bool):
                            raise ValueError("v1 delete response 缺少布尔 deleted")
                        if not deleted:
                            rejected.append(record_id)
                            continue
                    record_ids.append(record_id)
            else:
                ids = data.get("record_id_list")
                if not isinstance(ids, list) or any(
                    not isinstance(x, str) or not x for x in ids
                ):
                    raise ValueError("v1 mutation 响应缺少记录结果")
                if len(ids) != len(set(ids)):
                    raise ValueError("v1 mutation record_id_list 重复")
                record_ids = list(ids)
            if operation != "batch_create":
                expected = set(
                    body["records"]
                    if operation == "batch_delete"
                    else (record["record_id"] for record in body["records"])
                )
                if not set(record_ids).issubset(expected) or not set(rejected).issubset(
                    expected
                ):
                    raise ValueError("v1 mutation 返回了未请求的 record_id")
                rejected.extend(sorted(expected - set(record_ids) - set(rejected)))
            if len(record_ids) > requested:
                raise ValueError("v1 mutation 返回记录数超过请求数")
            revision = data.get("revision", data.get("rev"))
            if revision is not None and (
                isinstance(revision, bool) or not isinstance(revision, (int, str))
            ):
                raise ValueError("v1 mutation revision 必须为整数或字符串")
            accepted = len(record_ids)
            outcome = (
                MutationOutcome.ACCEPTED
                if accepted == requested and not ignored and not rejected
                else (
                    MutationOutcome.PARTIAL
                    if accepted or ignored
                    else MutationOutcome.REJECTED
                )
            )
            if accepted < requested and not rejected and not ignored:
                outcome = MutationOutcome.UNKNOWN_OUTCOME
            metadata = dict(data)
            if rejected:
                metadata["failed_record_ids"] = tuple(dict.fromkeys(rejected))
            return MutationReceipt(
                operation=operation,
                backend=self.api_family,
                requested_count=requested,
                accepted_count=accepted,
                unit="record",
                record_ids=tuple(record_ids),
                ignored_fields=tuple(ignored),
                record_not_found=tuple(dict.fromkeys(missing)),
                revision=revision,
                readback=(
                    ReadbackStatus.UNKNOWN
                    if outcome is MutationOutcome.UNKNOWN_OUTCOME
                    else ReadbackStatus.NOT_REQUESTED
                ),
                outcome=outcome,
                raw_metadata=metadata,
            )
        except (ValueError, TypeError, KeyError) as exc:
            error = FeishuAPIError(
                -1, str(exc), response_data=result, kind="invalid_response"
            )
            return self._unknown_receipt(operation, requested, cause=error)

    @staticmethod
    def _unknown_receipt(
        operation: str, requested: int, *, cause: Exception
    ) -> MutationReceipt:
        return MutationReceipt(
            operation=operation,
            backend=BitableBackendKind.BITABLE_V1,
            requested_count=requested,
            outcome=MutationOutcome.UNKNOWN_OUTCOME,
            readback=ReadbackStatus.UNKNOWN,
            raw_metadata=(
                cause.to_metadata()
                if isinstance(cause, FeishuAPIError)
                else {"error": str(cause)}
            ),
        )

    def batch_create(
        self, app_token: str, table_id: str, records: Sequence[CanonicalRecord]
    ) -> MutationReceipt:
        self._validate_records(records, self.max_batch_create_size)
        return self._mutation_call(
            "batch_create",
            app_token,
            table_id,
            {
                "records": [
                    self._encode_record(app_token, table_id, record)
                    for record in records
                ]
            },
            len(records),
            client_token=True,
        )

    def batch_update(
        self, app_token: str, table_id: str, records: Sequence[CanonicalRecord]
    ) -> MutationReceipt:
        self._validate_records(records, self.max_batch_update_size)
        if any(record.record_id is None for record in records):
            raise ValueError("v1 batch update records require record_id")
        return self._mutation_call(
            "batch_update",
            app_token,
            table_id,
            {
                "records": [
                    self._encode_record(app_token, table_id, record)
                    for record in records
                ]
            },
            len(records),
        )

    def batch_delete(
        self, app_token: str, table_id: str, record_ids: Sequence[str]
    ) -> MutationReceipt:
        self._validate_ids(record_ids, self.max_batch_delete_size)
        return self._mutation_call(
            "batch_delete",
            app_token,
            table_id,
            {"records": list(record_ids)},
            len(record_ids),
        )


__all__ = ["BitableV1Backend"]
