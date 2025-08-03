from sqlalchemy import create_engine, text


def get_postgres_engine(connection_string):

    if not connection_string:
        print("Error, connection_string is empty")
        return None

    try:
        engine = create_engine(connection_string)

        with engine.connect() as connection:
            print("Connection to PostgreSQL database successful!")

            result = connection.execute(text("SELECT version()"))
            db_version = result.scalar()
            print(f"PostgreSQL Version: {db_version}")

        return engine

    except Exception as e:
        print(f"An error occurred while connecting to the database: {e}")
        return None


