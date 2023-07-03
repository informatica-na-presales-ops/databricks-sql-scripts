import databricks.sql.client
import logging

log = logging.getLogger(__name__)


def get_connection(server_hostname: str, http_path: str, access_token: str):
    cnx = databricks.sql.connect(server_hostname, http_path, access_token)
    return cnx


def yield_rows(cnx: databricks.sql.client.Connection, sql: str, params: dict = None):
    with cnx:
        with cnx.cursor() as cur:
            cur.execute(sql, params)
            for row in cur.fetchall():
                yield row.asDict()


def get_iics_organizations(cnx: databricks.sql.client.Connection, org_last_updated_on_start):
    log.info(f'Getting IICS organizations in AWS-TS region updated on or after {org_last_updated_on_start}')
    sql = '''
        select
            customer_type, environment_type, cast(is_active as boolean) as is_active,
            cast(current_flag as boolean) is_current, cast(is_deleted as boolean) as is_deleted,
            cast(disabled as boolean) is_disabled, org_edition_type, org_expiry_on org_expires_at, org_id, org_key,
            org_last_updated_on org_last_updated_at, org_name, registration_date org_registered_at, org_type,
            org_type_lic, org_uuid, parent_org_id, pod_id, region_id, time_zone
        from prod_dev_sor.iics_organization_dim
        where region_id = 'AWS-TS'
        and org_last_updated_on >= %(org_last_updated_on_start)s
    '''
    params = {
        'org_last_updated_on_start': org_last_updated_on_start
    }
    yield from yield_rows(cnx, sql, params)
