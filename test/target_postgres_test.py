from cmath import exp
from singer_target_postgresql import *
from unittest.mock import patch, mock_open
from io import StringIO


def test_emit_state():
    data = {"state": "value"}
    with patch("singer_target_postgresql.sys.stdout", new=StringIO()) as fake_out:
        emit_state(data)
        assert fake_out.getvalue() == json.dumps(data) + "\n"


def test_tosql():
    config = {"keys": ["id"], "entity": "assets_mapping_phoenix"}
    record = {
        "id": "B2093E0D-EBE4-A3B3-59DD-FBEF7B0EA749",
        "customer_id": "6f3f9645-b984-4a83-b752-8c7270afaa32",
        "external_id": "A30BE23X38EXWR",
        "name": "TUNNEL SPORT",
        "network": "amazon",
        "country": "FR",
        "total_links": 11,
        "last_seen_at": "2022-05-09 02:36:35.189000",
    }
    assert tosql(record, config) == "INSERT INTO assets_mapping_phoenix (id, customer_id, external_id, name, network, country, total_links, last_seen_at) VALUES('B2093E0D-EBE4-A3B3-59DD-FBEF7B0EA749', '6f3f9645-b984-4a83-b752-8c7270afaa32', 'A30BE23X38EXWR', 'TUNNEL SPORT', 'amazon', 'FR', '11', '2022-05-09 02:36:35.189000') ON CONFLICT (id) DO UPDATE SET customer_id=EXCLUDED.customer_id, external_id=EXCLUDED.external_id, name=EXCLUDED.name, network=EXCLUDED.network, country=EXCLUDED.country, total_links=EXCLUDED.total_links, last_seen_at=EXCLUDED.last_seen_at;"


def test_persist_lines():
    config = {"keys": ["id"], "entity": "assets_mapping_phoenix"}
    with open("test/test_data.json") as f:
        data = f.readlines()
    
    expected = [json.loads(x) for x in data]
    expected = [x["record"] for x in expected if x["type"] == "RECORD"]
    expected = [f"{tosql(x, config)}" for x in expected]
    m = mock_open()
    with patch("singer_target_postgresql.open", m, create=True) as fake_out:
        persist_lines(config, data)
    
    m.assert_called_once_with('data.sql', 'w')
    handle = m()
    handle.write.assert_called_once_with("\n".join(expected) + "\n")
