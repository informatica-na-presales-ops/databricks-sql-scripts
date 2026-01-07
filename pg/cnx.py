import psycopg2.extras


def get_connection(connection_string: str):
    cnx = psycopg2.connect(connection_string, cursor_factory=psycopg2.extras.DictCursor)
    return cnx
