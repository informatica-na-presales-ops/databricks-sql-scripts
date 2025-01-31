import databricks.sql.client
import logging

log = logging.getLogger(__name__)


def get_connection(server_hostname: str, http_path: str, access_token: str):
    cnx = databricks.sql.connect(server_hostname, http_path, access_token)
    return cnx


def yield_rows(cnx: databricks.sql.client.Connection, sql: str, params: dict = None, close_cnx: bool = True):
    try:
        with cnx.cursor() as cur:
            cur.execute(sql, params)
            while True:
                row = cur.fetchone()
                if row is None:
                    return
                yield row.asDict()
    finally:
        if close_cnx:
            cnx.close()


def get_iics_advanced_cluster_configs(cnx: databricks.sql.client.Connection, close_cnx: bool):
    log.info('Getting IICS advanced cluster configs in AWS-TS region')
    sql = '''
        select
            cloud_type, config_id, config_key, iics_id, cast(current_flag as boolean) is_current,
            cast(isdeleted as boolean) is_deleted, master_instance_type, org_id, org_key, org_uuid, pod_id,
            worker_instance_type
        from prod_dev_sor.iics_cdi_e_cluster_config_dim
        where iics_id = 'AWS-TS'
    '''
    yield from yield_rows(cnx, sql, close_cnx=close_cnx)


def get_iics_advanced_cluster_instances(cnx: databricks.sql.client.Connection, close_cnx: bool):
    log.info('Getting IICS advanced cluster instances in AWS-TS region')
    sql = '''
        select
            agent_group_id, cluster_config_id, ephemeral_id, iics_id, instance_id, instance_key,
            cast(current_flag as boolean) is_current, cast(isdeleted as boolean) is_deleted, pod_id
        from prod_dev_sor.iics_cdi_e_instance_dim
        where iics_id = 'AWS-TS'
    '''
    yield from yield_rows(cnx, sql, close_cnx=close_cnx)


def get_iics_agents(cnx: databricks.sql.client.Connection, record_updated_at_start, close_cnx: bool):
    log.info(f'Getting IICS agents in AWS-TS region updated on or after {record_updated_at_start}')
    sql = '''
        select * from (
            select
                agent_key, org_key, region_id, pod_id, org_id, org_uuid, agent_group_id, agent_group_name,
                agent_group_desc, agent_id, agent_name, agent_host, agent_description,
                cast(agent_active as boolean) is_active, agent_platform,
                agent_last_status_change_on agent_last_status_change_at, agent_group_created_on agent_group_created_at,
                agent_group_updated_on agent_group_updated_at, agent_group_created_by, agent_group_updated_by,
                agent_created_on agent_created_at, agent_updated_on agent_updated_at, agent_created_by,
                agent_updated_by, cast(agent_group_is_deleted as boolean) as agent_group_is_deleted,
                cast(agent_is_deleted as boolean) as agent_is_deleted, cast(current_flag as boolean) is_current,
                case
                    when agent_updated_on is null then agent_group_updated_on
                    when agent_updated_on > agent_group_updated_on then agent_updated_on
                    else agent_group_updated_on
                end record_updated_at
            from prod_dev_sor.iics_agent_dim
            where region_id = 'AWS-TS') t
        where record_updated_at >= %(record_updated_at_start)s
    '''
    params = {
        'record_updated_at_start': record_updated_at_start
    }
    yield from yield_rows(cnx, sql, params, close_cnx)


def get_iics_organizations(cnx: databricks.sql.client.Connection):
    log.info(f'Getting IICS organizations in AWS-TS region')
    sql = '''
        select
            customer_type, environment_type, cast(is_active as boolean) as is_active,
            cast(current_flag as boolean) is_current, cast(is_deleted as boolean) as is_deleted,
            cast(disabled as boolean) is_disabled, org_edition_type, org_expiry_on org_expires_at, org_id, org_key,
            org_last_updated_on org_last_updated_at, org_name, registration_date org_registered_at, org_type,
            org_type_lic, org_uuid, parent_org_id, pod_id, region_id, time_zone
        from prod_dev_sor.iics_organization_dim
        where region_id = 'AWS-TS'
    '''
    yield from yield_rows(cnx, sql)


def get_iics_serverless_environments(cnx: databricks.sql.client.Connection):
    log.info('Getting IICS serverless environments in AWS-TS region')
    sql = '''
        select
            az, cloud_provider, created_time created_at, expiry_time expires_at, last_updated_time last_updated_at,
            name, org_key, org_uuid, pod_id, region, region_id, serverless_env_id, serverless_env_key, type, user_name
        from prod_dev_sor.iics_serverless_environment
        where region_id = 'AWS-TS'
    '''
    yield from yield_rows(cnx, sql)


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


def get_iics_user_weekly_logins(cnx: databricks.sql.client.Connection):
    log.info('Getting IICS user weekly login stats')
    sql = '''
        select
            week_start, email, count(*) login_count
            from (
                select
                    dateadd(day, -1, date_trunc('week', dateadd(day, 1, createtime))) week_start,
                    lower(u.emails) email
                from prod_dev_sor.iics_user_log l
                join prod_dev_sor.iics_user_dim u on u.user_id = l.objectid
                where l.region_id = 'AWS-TS'
                and l.event = 'USER_LOGIN'
                and l.eventparam = 'UI'
                and endswith(u.emails, '@informatica.com')
            )
            where week_start > dateadd(month, -1, now())
            group by week_start, email
    '''
    yield from yield_rows(cnx, sql)


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
