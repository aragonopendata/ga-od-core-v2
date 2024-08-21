import os
import time

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

api_base_url = os.getenv("API_BASE_URL", "http://localhost:8000/GA_OD_Core")
interval = os.getenv("INTERVAL", "5 days")


def get_connection_url() -> str:
    return f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"


def call_resource_api(resource_id: int) -> None:
    url = f"{api_base_url}/preview?resource_id={resource_id}"

    print(f"Resource ID: {resource_id}")
    print(f"URL: {url}")
    response = requests.get(url)
    print(f"Status: {response.status_code}\n")


def update_resources(connection: psycopg2.connect, interval: str = None) -> None:
    query = "SELECT * FROM gaodcore_manager_resourcesizeconfig"
    if interval and interval != "":
        query += f" WHERE updated_at < NOW() - INTERVAL '{interval}'"

    with connection.cursor() as cursor:

        cursor.execute(query)
        for row in cursor:
            resource_id = row[0]
            call_resource_api(resource_id)


def get_enabled_resources(conn: psycopg2.connect) -> list[int]:
    with conn.cursor() as cursor:
        query = "SELECT id FROM gaodcore_manager_resourceconfig WHERE enabled = true"
        cursor.execute(query)
        resource_ids = [row[0] for row in cursor]

    return resource_ids


def get_existing_resources(conn: psycopg2.connect) -> list[int]:
    with conn.cursor() as cursor:
        query = "SELECT resource_id_id FROM gaodcore_manager_resourcesizeconfig"
        cursor.execute(query)
        resource_ids = [row[0] for row in cursor]

    return resource_ids


def update_new_resources(connection: psycopg2.connect):

    resources_to_add = list(set(get_enabled_resources(connection)) - set(get_existing_resources(connection)))
    resources_to_add.sort()
    print(f"Resources to add: {len(resources_to_add)}\n")

    for resource_id in resources_to_add:
        before = time.perf_counter()
        try:
            call_resource_api(resource_id)
        except Exception as e:
            print(f"Error calling resource API: {e}")
        after = time.perf_counter()
        print(f"Time taken: {after - before:.2f} seconds\n")


def main():
    try:
        conn = psycopg2.connect(get_connection_url())
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        exit(1)

    try:
        print("Adding new resources")
        update_new_resources(conn)
        print("Updating resources")
        update_resources(conn, interval)
    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
