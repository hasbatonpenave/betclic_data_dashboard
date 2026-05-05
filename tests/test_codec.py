import pytest
from proto.codec import (
    varint, field_int, field_str, grpc_frame,
    read_varint, decode_proto,
    payload_get_match, payload_list_matches, payload_live_count,
    build_clean_json, extract_live_state,
    parse_matches_frame, parse_grpc_response,
)


def test_varint_zero():
    assert varint(0) == b'\x00'


def test_varint_single_byte():
    assert varint(1) == b'\x01'
    assert varint(127) == b'\x7f'


def test_varint_multi_byte():
    assert varint(128) == b'\x80\x01'
    assert varint(300) == b'\xac\x02'


def test_varint_roundtrip():
    for v in [0, 1, 127, 128, 300, 1000000, 2**63 - 1]:
        encoded = varint(v)
        decoded, pos = read_varint(encoded, 0)
        assert decoded == v
        assert pos == len(encoded)


def test_field_int():
    result = field_int(1, 42)
    assert len(result) > 0


def test_field_str():
    result = field_str(2, "hello")
    assert b'hello' in result
    assert len(result) == 1 + 1 + 5  # tag + length + "hello"


def test_grpc_frame():
    proto = b'\x08\x2a'
    frame = grpc_frame(proto)
    assert frame[0] == 0x00  # uncompressed flag
    assert len(frame) == 5 + len(proto)


def test_payload_get_match():
    payload = payload_get_match(1045033759662080, "fr")
    assert isinstance(payload, bytes)
    assert len(payload) > 5


def test_payload_get_match_with_market():
    payload = payload_get_match(123, "fr", "ca_ftb_rslt")
    assert isinstance(payload, bytes)
    assert b'ca_ftb_rslt' in payload


def test_payload_list_matches():
    payload = payload_list_matches("football", None, "fr", 0)
    assert isinstance(payload, bytes)
    assert len(payload) > 5


def test_payload_live_count():
    payload = payload_live_count("fr")
    assert isinstance(payload, bytes)
    assert b'fr' in payload


def test_read_varint():
    val, pos = read_varint(b'\xac\x02', 0)
    assert val == 300
    assert pos == 2


def test_decode_proto_empty():
    result = decode_proto(b'')
    assert result == {}


def test_decode_proto_varint():
    data = varint((1 << 3) | 0) + varint(42)
    result = decode_proto(data)
    assert result.get(1) == [42]


def test_decode_proto_string():
    data = varint((2 << 3) | 2) + varint(5) + b'hello'
    result = decode_proto(data)
    assert result.get(2) == ['hello']


def test_parse_grpc_response_empty():
    msgs, trailers = parse_grpc_response(b'')
    assert msgs == []
    assert trailers == {}


def test_parse_grpc_response_trailer():
    trailer_data = b'grpc-status: 0\r\ngrpc-message: OK\r\n'
    frame = b'\x80' + b'\x00\x00\x00' + bytes([len(trailer_data)]) + trailer_data
    msgs, trailers = parse_grpc_response(frame)
    assert trailers.get('grpc-status') == '0'


def test_extract_live_state_empty():
    result = extract_live_state(b'')
    assert result['is_live'] is False
    assert result['score_home'] is None
    assert result['score_away'] is None
    assert result['period'] is None
    assert result['minute'] is None


def test_build_clean_json_bad_input():
    result = build_clean_json(b'\x00\x00\x00\x00\x00')
    assert 'error' in result
