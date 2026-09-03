import psycopg2.extensions
import psycopg2.extras


def get_connection(connection_string: str) -> psycopg2.extensions.connection:
    cnx = psycopg2.connect(connection_string, cursor_factory=psycopg2.extras.DictCursor)
    return cnx
