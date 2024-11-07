from datetime import date

import pytest
from sqlalchemy import Column, Integer, String, Date, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from testcontainers.postgres import PostgresContainer

from gaodcore_manager.models import ResourceConfig, ConnectorConfig

Base = declarative_base()


class Car(Base):
    """ Car model for tests.
    @param name: str - Car name
    @param brand: str - Car brand
    @param year: int - Car year
    @param price: int - Car price
    @param purchase_date: date - Car purchase date
    """
    __tablename__ = 'cars'
    name: str
    brand: str
    year: int
    purchase_date: date

    id = Column(Integer, primary_key=True)
    name = Column(String)
    brand = Column(String)
    year = Column(Integer)
    price = Column(Integer)
    purchase_date = Column(Date)

    @staticmethod
    def create_table():
        engine = create_engine(postgres.get_connection_url())
        Base.metadata.create_all(engine)

    @staticmethod
    def delete_all():
        engine = create_engine(postgres.get_connection_url())
        session = sessionmaker(bind=engine)()
        session.query(Car).delete()
        session.commit()
        session.close()

    @staticmethod
    def add_car(name: str, brand: str, year: int, price: int, purchase_date: date):
        engine = create_engine(postgres.get_connection_url())
        session = sessionmaker(bind=engine)()
        car = Car(name=name, brand=brand, year=year, price=price, purchase_date=purchase_date)
        session.add(car)
        session.commit()
        session.close()


postgres = PostgresContainer("postgres:16-alpine")


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    postgres.start()

    def remove_container():
        postgres.stop()

    request.addfinalizer(remove_container)
    # os.environ["DB_CONN"] = postgres.get_connection_url()
    # os.environ["DB_HOST"] = postgres.get_container_host_ip()
    # os.environ["DB_PORT"] = postgres.get_exposed_port(5432)
    # os.environ["DB_USERNAME"] = postgres.POSTGRES_USER
    # os.environ["DB_PASSWORD"] = postgres.POSTGRES_PASSWORD
    # os.environ["DB_NAME"] = postgres.POSTGRES_DB
    Car.create_table()


@pytest.fixture(scope="function")
def configs():
    connector_config = ConnectorConfig.objects.create(name="test",
                                                      uri=postgres.get_connection_url().replace("postgresql+psycopg2",
                                                                                                "postgresql"),
                                                      enabled=True)
    resource_config = ResourceConfig.objects.create(name="test",
                                                    connector_config=connector_config,
                                                    object_location="cars",
                                                    object_location_schema="public",
                                                    enabled=True)

    return {
        "uri": connector_config.uri,
        "location": resource_config.object_location,
        "schema": resource_config.object_location_schema,
    }


@pytest.fixture(scope="function", autouse=True)
def setup_data():
    Car.delete_all()
