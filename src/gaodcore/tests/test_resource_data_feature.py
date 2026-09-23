"""Focused tests for the GeoJSON FeatureCollection built by
`connectors.get_resource_data_feature()`.

The engine and session are mocked, so no database is contacted. The model is a
real SQLAlchemy table so the geometry column is detected exactly as in
production; the rows are what PostGIS returns for `ST_AsGeoJSON(geom)`.
"""

from unittest.mock import MagicMock, patch

from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer, MetaData, String, Table

import connectors

URI = "postgresql://username:password@example.invalid:5432/gaodcore"

POINT = '{"type":"Point","coordinates":[-0.8773,41.6561]}'


def _model():
    return Table(
        "places",
        MetaData(),
        Column("id", Integer),
        Column("name", String),
        Column("geom", Geometry("POINT", srid=4326)),
    )


def _get_features(rows):
    query = MagicMock()
    for method in ("filter_by", "filter", "order_by", "offset", "limit"):
        getattr(query, method).return_value = query
    query.all.return_value = rows
    session = MagicMock()
    session.query.return_value = query

    with (
        patch.object(connectors, "_get_engine"),
        patch.object(connectors, "_get_model", return_value=_model()),
        patch.object(connectors, "sessionmaker", return_value=lambda: session),
    ):
        return connectors.get_resource_data_feature(
            uri=URI,
            object_location="places",
            object_location_schema="public",
            filters={},
            like="",
            fields=[],
            sort=[],
        )


def test_feature_geometry_is_parsed():
    result = _get_features([(1, "Zaragoza", POINT)])

    assert result == {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-0.8773, 41.6561]},
                "properties": {"id": 1, "name": "Zaragoza"},
            }
        ],
    }


def test_null_geometry_is_kept_as_unlocated_feature():
    # ST_AsGeoJSON(NULL) is NULL; that used to crash json.loads with a 500.
    result = _get_features([(1, "Zaragoza", POINT), (2, "Unknown", None)])

    features = result["features"]
    assert len(features) == 2
    assert features[0]["geometry"]["type"] == "Point"
    assert features[1] == {
        "type": "Feature",
        "geometry": None,
        "properties": {"id": 2, "name": "Unknown"},
    }
