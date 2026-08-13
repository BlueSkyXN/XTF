from api.bitable_backend import (
    BitableBackendKind,
    FieldKind,
    FieldSchema,
    IncompleteReadError,
    MutationOutcome,
    UserIDType,
    as_backend_kind,
    as_user_id_type,
    field_kind_from_type,
    field_is_writable,
)


def test_incomplete_read_error_preserves_partial_result() -> None:
    from api.bitable_backend import RecordReadResult

    partial = RecordReadResult(
        records=(),
        fields=(FieldSchema("fld", "Name", FieldKind.TEXT),),
        complete=False,
        backend=BitableBackendKind.BASE_V3,
    )

    error = IncompleteReadError("incomplete", partial)

    assert error.partial_result is partial
    assert partial.complete is False


def test_backend_and_user_id_enums_are_explicit() -> None:
    assert as_backend_kind("base_v3") is BitableBackendKind.BASE_V3
    assert (
        as_backend_kind(BitableBackendKind.BITABLE_V1) is BitableBackendKind.BITABLE_V1
    )
    assert as_user_id_type("union_id") is UserIDType.UNION_ID
    assert MutationOutcome.UNKNOWN_OUTCOME.value == "unknown_outcome"


def test_invalid_backend_and_user_id_are_rejected() -> None:
    import pytest

    with pytest.raises(ValueError):
        as_backend_kind("auto")
    with pytest.raises(ValueError):
        as_user_id_type("guess")


def test_v1_and_v3_field_type_mapping_is_protocol_neutral() -> None:
    assert field_kind_from_type(18) is FieldKind.LINK
    assert field_kind_from_type(19) is FieldKind.LOOKUP
    assert field_kind_from_type(20) is FieldKind.FORMULA
    assert field_kind_from_type(21) is FieldKind.LINK
    assert field_kind_from_type(22) is FieldKind.LOCATION
    assert field_kind_from_type(23) is FieldKind.GROUP_CHAT
    assert field_kind_from_type("datetime") is FieldKind.DATETIME
    assert field_is_writable(FieldKind.ATTACHMENT) is False
    assert field_is_writable(FieldKind.UNSUPPORTED) is False
