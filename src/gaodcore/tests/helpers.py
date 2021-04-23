import datetime

from django.db.models import Model
from django.test import Client
from sqlalchemy import create_engine, Column, Integer, String, BigInteger, Float, Numeric, Boolean, Date, DateTime, Text
from sqlalchemy.orm import declarative_base, sessionmaker


def get_auth_client(client: Client, django_user_model: Model):
    username = "user"
    password = "password"
    user = django_user_model.objects.create_user(username=username, password=password)
    client.force_login(user)

    return client


def get_uri(host: str, port: str) -> str:
    return f"postgresql://postgres:@{host}:{port}/guillotina"


def create_connector_ga_od_core(client: Client, test_name: str, uri: str):
    return client.post('/gaodcore/connector-config/', {"name": test_name, "enabled": True, "uri": uri})


def create_table(uri: str, test_name: str):
    engine = create_engine(uri, echo=True)

    Base = declarative_base()
    Session = sessionmaker(bind=engine)

    class TestData(Base):
        __tablename__ = test_name
        id = Column(Integer, primary_key=True, autoincrement=True)
        name = Column(String, unique=True)
        size = Column(BigInteger)
        max_acceleration = Column(Float)
        weight = Column(Numeric)
        description = Column(Text)
        discover_date = Column(Date)
        destroyed_date = Column(DateTime)
        destroyed = Column(Boolean)

    Base.metadata.create_all(engine)

    session = Session()

    session.add(
        TestData(name='RX-78-2 Gundam',
                 size=18,
                 max_acceleration=0.93,
                 weight=60.0,
                 description='The RX-78-2 Gundam is the titular mobile suit of Mobile Suit Gundam television series',
                 discover_date=datetime.date(79, 9, 18),
                 destroyed_date=datetime.datetime(79, 12, 31, 12, 1, 1),
                 destroyed=True))
    session.add(TestData(name='Half Gundam', ))

    session.commit()
    session.close()


def create_view(client, test_name: str, connector_data):
    return client.post('/gaodcore/resource-config/', {
        "name": test_name,
        "enabled": True,
        "connector_config": connector_data.json()['id'],
        "object_location": test_name
    })


def create_full_example(client, host: str, port: str, test_name: str):
    uri = get_uri(host, port)
    connector_data = create_connector_ga_od_core(client, test_name, uri)
    create_table(uri, test_name)
    return create_view(client, test_name, connector_data)
