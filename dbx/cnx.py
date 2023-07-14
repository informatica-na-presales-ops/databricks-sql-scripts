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


def get_iics_user_roles(cnx: databricks.sql.client.Connection, updated_on_start):
    log.info(f'Getting IICS user roles in AWS-TS region updated on or after {updated_on_start}')
    sql = '''
        select
            created_on created_at, created_by, cast(is_deleted as boolean) as is_deleted,
            cast(role_readonly as boolean) is_read_only, org_id, org_key, org_uuid, pod_id, region_id, role_description,
            role_id, role_name, updated_on updated_at, updated_by, user_id, user_key, user_role_key, user_role_uuid
        from prod_dev_sor.iics_user_role_dim
        where region_id = 'AWS-TS'
        and updated_on >= %(updated_on_start)s
    '''
    params = {
        'updated_on_start': updated_on_start,
    }
    yield from yield_rows(cnx, sql, params)


def get_iics_users(cnx: databricks.sql.client.Connection, update_time_start):
    log.info(f'Getting IICS users in AWS-TS region updated on or after {update_time_start}')
    sql = '''
        select
            create_time created_at, created_by, emails email, first_name, full_name,
            cast(is_deleted as boolean) as is_deleted, last_name, org_id, org_key, org_uuid, pod_id, region_id,
            update_time updated_at, updated_by, user_id, user_key, user_name
        from prod_dev_sor.iics_user_dim
        where region_id = 'AWS-TS'
        and update_time >= %(update_time_start)s
    '''
    params = {
        'update_time_start': update_time_start,
    }
    yield from yield_rows(cnx, sql, params)
