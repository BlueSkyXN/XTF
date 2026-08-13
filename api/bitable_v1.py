"""Typed adapter for the existing Feishu Bitable v1 wire API.

This module deliberately keeps the v1 page-token and payload shapes separate
from Base v3.  ``BitableAPI`` remains the public legacy facade; this adapter
uses its authenticated transport and business-code retry implementation.
"""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .auth import FeishuAuth
from .base import RetryableAPIClient
from .bitable import BitableAPI
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
from .sdk import FeishuAPIError, Paginator
from .url import encode_path_segment


class BitableV1Backend:
    """Typed v1 client that composes the existing ``BitableAPI`` transport."""

    api_family = BitableBackendKind.BITABLE_V1
    max_batch_create_size = BitableAPI.MAX_BATCH_CREATE_SIZE
    max_batch_update_size = BitableAPI.MAX_BATCH_UPDATE_SIZE
    max_batch_delete_size = BitableAPI.MAX_BATCH_DELETE_SIZE
    max_batch_get_size = 100

    def __init__(
        self,
        auth: FeishuAuth,
        api_client: Optional[RetryableAPIClient] = None,
        *,
        user_id_type: UserIDType | str = UserIDType.OPEN_ID,
        legacy_api: Optional[BitableAPI] = None,
    ) -> None:
        self.auth = auth
        self.api_client = api_client or auth.api_client
        self.user_id_type = as_user_id_type(user_id_type)
        self.legacy_api = legacy_api or BitableAPI(auth, self.api_client)
        self._field_cache: dict[tuple[str, str], tuple[FieldSchema, ...]] = {}

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
        schemas = tuple(
            self._field_schema(item)
            for item in self.legacy_api.list_fields(app_token, table_id)
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
            encoded: List[Dict[str, Any]] = []
            for item in values:
                if isinstance(item, dict) and isinstance(item.get("id"), str):
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
                if isinstance(item, dict) and isinstance(item.get("id"), str):
                    link_ids.append(item["id"])
                elif isinstance(item, str) and item.strip():
                    link_ids.append(item)
                else:
                    raise ValueError(
                        f"field {schema.name!r} link value must contain IDs"
                    )
            return link_ids
        if schema.kind is FieldKind.DATETIME and isinstance(value, str):
            try:
                parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
                return int(parsed.timestamp() * 1000)
            except ValueError:
                return value
        if schema.kind is FieldKind.LOCATION and not isinstance(value, (str, dict)):
            raise ValueError(f"field {schema.name!r} location value must be an object")
        return value

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
            _, result = self.legacy_api._call_api_with_biz_retry(
                "POST",
                url,
                headers=self.auth.get_auth_headers(),
                json={"field_name": field_name, "type": field_type},
            )
        except FeishuAPIError as exc:
            if exc.kind == "transport":
                return self._unknown_receipt("create_field", 1, cause=exc)
            raise
        self._field_cache.pop((app_token, table_id), None)
        return MutationReceipt(
            operation="create_field",
            backend=self.api_family,
            requested_count=1,
            accepted_count=1,
            outcome=MutationOutcome.ACCEPTED,
            raw_metadata=result.get("data", {}) if isinstance(result, dict) else {},
        )

    def _search_pages(
        self,
        app_token: str,
        table_id: str,
        field_names: Optional[List[str]],
    ) -> List[Dict[str, Any]]:
        def fetch(token: Optional[str]):
            return self.legacy_api._search_records_page(
                app_token,
                table_id,
                page_token=token,
                field_names=field_names,
            )

        return Paginator().collect(fetch)

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
        _, envelope = self.legacy_api._call_api_with_biz_retry(
            "POST",
            url,
            headers=self.auth.get_auth_headers(),
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
            _, result = self.legacy_api._call_api_with_biz_retry(
                "POST",
                url,
                headers=self.auth.get_auth_headers(),
                params=params,
                json=body,
            )
        except FeishuAPIError as exc:
            if exc.kind == "transport":
                return self._unknown_receipt(operation, requested, cause=exc)
            raise
        data = result.get("data", {}) if isinstance(result, dict) else {}
        if not isinstance(data, dict):
            raise FeishuAPIError(
                -1,
                "v1 mutation data 必须是对象",
                response_data=result,
                kind="invalid_response",
            )
        record_ids = data.get("record_id_list", [])
        if not isinstance(record_ids, list) or any(
            not isinstance(item, str) for item in record_ids
        ):
            record_ids = []
        ignored = data.get("ignored_fields", [])
        if not isinstance(ignored, list):
            ignored = []
        not_found = data.get("record_not_found", [])
        if not isinstance(not_found, list):
            not_found = []
        outcome = (
            MutationOutcome.PARTIAL
            if ignored or not_found
            else MutationOutcome.ACCEPTED
        )
        accepted = (
            len(record_ids) if operation == "batch_create" and record_ids else requested
        )
        return MutationReceipt(
            operation=operation,
            backend=self.api_family,
            requested_count=requested,
            accepted_count=accepted,
            record_ids=tuple(record_ids),
            ignored_fields=tuple(item for item in ignored if isinstance(item, dict)),
            record_not_found=tuple(item for item in not_found if isinstance(item, str)),
            outcome=outcome,
            raw_metadata=data,
        )

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
            raw_metadata={"error": str(cause)},
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
