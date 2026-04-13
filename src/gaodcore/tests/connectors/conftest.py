import time
from datetime import date

import pytest
from sqlalchemy import Column, Integer, String, Date, create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker
from testcontainers.postgres import PostgresContainer
from testcontainers.oracle import OracleDbContainer

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


# ---------------------------------------------------------------------------
# Oracle testcontainers fixtures
# ---------------------------------------------------------------------------

ORACLE_IMAGE = "gvenzl/oracle-xe:21-slim"
ORACLE_TEST_TABLE = "TEST_COLUMN_CASES"
ORACLE_SCHEMA = "SYSTEM"


def _wait_for_oracle(engine, timeout=120):
    for _ in range(timeout // 2):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1 FROM DUAL"))
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"Oracle container not ready after {timeout} seconds")


@pytest.fixture(scope="module")
def oracle_container():
    container = OracleDbContainer(
        image=ORACLE_IMAGE,
        oracle_password="test_password",
    )
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="module")
def oracle_engine(oracle_container):
    connection_url = oracle_container.get_connection_url()
    engine = create_engine(connection_url, max_identifier_length=128)
    _wait_for_oracle(engine)
    yield engine
    engine.dispose()


@pytest.fixture(scope="module")
def oracle_uri(oracle_container):
    return oracle_container.get_connection_url()


@pytest.fixture(scope="module")
def setup_oracle_tables(oracle_engine):
    with oracle_engine.connect() as conn:
        conn.execute(text(f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE {ORACLE_TEST_TABLE}';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
        """))
        conn.commit()

        conn.execute(text(f"""
            CREATE TABLE {ORACLE_TEST_TABLE} (
                PRODUCTID NUMBER,
                AÑO NUMBER,
                "CONTRACT STATUS" VARCHAR2(100),
                "order_id" NUMBER,
                "UserId" VARCHAR2(100)
            )
        """))
        conn.commit()

        conn.execute(text(f"""
            INSERT INTO {ORACLE_TEST_TABLE} VALUES (1, 2024, 'Active', 100, 'user_001')
        """))
        conn.commit()

    yield

    with oracle_engine.connect() as conn:
        conn.execute(text(f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE {ORACLE_TEST_TABLE}';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
        """))
        conn.commit()
