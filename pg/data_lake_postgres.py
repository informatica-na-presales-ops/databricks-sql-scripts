import logging
import psycopg2.extras

log = logging.getLogger(__name__)


def batch_upsert(cnx, sql, records):
    with cnx:
        with cnx.cursor() as cur:
            log.info(f'Saving {len(records)} records to Postgres')
            psycopg2.extras.execute_batch(cur, sql, records)


def batch_upsert_iics_agents(cnx, records):
    sql = '''
        insert into iics_agents (
            agent_key, org_key, region_id, pod_id, org_id, org_uuid, agent_group_id,
            agent_group_name, agent_group_desc, agent_id, agent_name, agent_host,
            agent_description, is_active, agent_platform, agent_last_status_change_at,
            agent_group_created_at, agent_group_updated_at, agent_group_created_by,
            agent_group_updated_by, agent_created_at, agent_updated_at, agent_created_by,
            agent_updated_by, agent_group_is_deleted, agent_is_deleted, is_current,
            record_updated_at
        ) values (
            %(agent_key)s, %(org_key)s, %(region_id)s, %(pod_id)s, %(org_id)s, %(org_uuid)s, %(agent_group_id)s,
            %(agent_group_name)s, %(agent_group_desc)s, %(agent_id)s, %(agent_name)s, %(agent_host)s,
            %(agent_description)s, %(is_active)s, %(agent_platform)s, %(agent_last_status_change_at)s,
            %(agent_group_created_at)s, %(agent_group_updated_at)s, %(agent_group_created_by)s,
            %(agent_group_updated_by)s, %(agent_created_at)s, %(agent_updated_at)s, %(agent_created_by)s,
            %(agent_updated_by)s, %(agent_group_is_deleted)s, %(agent_is_deleted)s, %(is_current)s,
            %(record_updated_at)s
        ) on conflict (agent_key) do update set
            org_key = excluded.org_key, region_id = excluded.region_id, pod_id = excluded.pod_id,
            org_id = excluded.org_id, org_uuid = excluded.org_uuid, agent_group_id = excluded.agent_group_id,
            agent_group_name = excluded.agent_group_name, agent_group_desc = excluded.agent_group_desc,
            agent_id = excluded.agent_id, agent_name = excluded.agent_name, agent_host = excluded.agent_host,
            agent_description = excluded.agent_description, is_active = excluded.is_active,
            agent_platform = excluded.agent_platform,
            agent_last_status_change_at = excluded.agent_last_status_change_at,
            agent_group_created_at = excluded.agent_group_created_at,
            agent_group_updated_at = excluded.agent_group_updated_at,
            agent_group_created_by = excluded.agent_group_created_by,
            agent_group_updated_by = excluded.agent_group_updated_by, agent_created_at = excluded.agent_created_at,
            agent_updated_at = excluded.agent_updated_at, agent_created_by = excluded.agent_created_by,
            agent_updated_by = excluded.agent_updated_by, agent_group_is_deleted = excluded.agent_group_is_deleted,
            agent_is_deleted = excluded.agent_is_deleted, is_current = excluded.is_current,
            record_updated_at = excluded.record_updated_at
    '''
    batch_upsert(cnx, sql, records)


def batch_upsert_iics_organizations(cnx, records):
    sql = '''
        insert into iics_organizations (
            customer_type, environment_type, is_active, is_current, is_deleted, is_disabled,
            org_edition_type, org_expires_at, org_id, org_key, org_last_updated_at, org_name,
            org_registered_at, org_type, org_type_lic, org_uuid, parent_org_id, pod_id,
            region_id, time_zone
        ) values (
            %(customer_type)s, %(environment_type)s, %(is_active)s, %(is_current)s, %(is_deleted)s, %(is_disabled)s,
            %(org_edition_type)s, %(org_expires_at)s, %(org_id)s, %(org_key)s, %(org_last_updated_at)s, %(org_name)s,
            %(org_registered_at)s, %(org_type)s, %(org_type_lic)s, %(org_uuid)s, %(parent_org_id)s, %(pod_id)s,
            %(region_id)s, %(time_zone)s
        ) on conflict (org_key) do update set
            customer_type = excluded.customer_type, environment_type = excluded.environment_type,
            is_active = excluded.is_active, is_current = excluded.is_current, is_deleted = excluded.is_deleted,
            is_disabled = excluded.is_disabled, org_edition_type = excluded.org_edition_type,
            org_expires_at = excluded.org_expires_at, org_id = excluded.org_id,
            org_last_updated_at = excluded.org_last_updated_at, org_name = excluded.org_name,
            org_registered_at = excluded.org_registered_at, org_type = excluded.org_type,
            org_type_lic = excluded.org_type_lic, org_uuid = excluded.org_uuid, parent_org_id = excluded.parent_org_id,
            pod_id = excluded.pod_id, region_id = excluded.region_id, time_zone = excluded.time_zone
    '''
    batch_upsert(cnx, sql, records)


def batch_upsert_iics_user_roles(cnx, records):
    sql = '''
        insert into iics_user_roles (
            created_at, created_by, is_deleted, is_read_only, org_id, org_key, org_uuid,
            pod_id, region_id, role_description, role_id, role_name, updated_at, updated_by,
            user_id, user_key, user_role_key, user_role_uuid
        ) values (
            %(created_at)s, %(created_by)s, %(is_deleted)s, %(is_read_only)s, %(org_id)s, %(org_key)s, %(org_uuid)s,
            %(pod_id)s, %(region_id)s, %(role_description)s, %(role_id)s, %(role_name)s, %(updated_at)s, %(updated_by)s,
            %(user_id)s, %(user_key)s, %(user_role_key)s, %(user_role_uuid)s
        ) on conflict (user_role_key) do update set
            created_at = excluded.created_at, created_by = excluded.created_by, is_deleted = excluded.is_deleted,
            is_read_only = excluded.is_read_only, org_id = excluded.org_id, org_key = excluded.org_key,
            org_uuid = excluded.org_uuid, pod_id = excluded.pod_id, region_id = excluded.region_id,
            role_description = excluded.role_description, role_id = excluded.role_id, role_name = excluded.role_name,
            updated_at = excluded.updated_at, updated_by = excluded.updated_by, user_id = excluded.user_id,
            user_key = excluded.user_key, user_role_uuid = excluded.user_role_uuid
    '''
    batch_upsert(cnx, sql, records)


def batch_upsert_iics_weekly_logins(cnx, records):
    sql = '''
        insert into iics_user_weekly_logins (
            week_start, email, login_count
        ) values (
            %(week_start)s, %(email)s, %(login_count)s
        ) on conflict (week_start, email) do update set
            login_count = excluded.login_count
    '''
    batch_upsert(cnx, sql, records)


def batch_upsert_iics_users(cnx, records):
    sql = '''
        insert into iics_users (
            created_at, created_by, email, first_name, full_name, is_deleted, last_name,
            org_id, org_key, org_uuid, pod_id, region_id, updated_at, updated_by,
            user_id, user_key, user_name
        ) values (
            %(created_at)s, %(created_by)s, %(email)s, %(first_name)s, %(full_name)s, %(is_deleted)s, %(last_name)s,
            %(org_id)s, %(org_key)s, %(org_uuid)s, %(pod_id)s, %(region_id)s, %(updated_at)s, %(updated_by)s,
            %(user_id)s, %(user_key)s, %(user_name)s
        ) on conflict (user_key) do update set
            created_at = excluded.created_at, created_by = excluded.created_by, email = excluded.email,
            first_name = excluded.first_name, full_name = excluded.full_name, is_deleted = excluded.is_deleted,
            last_name = excluded.last_name, org_id = excluded.org_id, org_key = excluded.org_key,
            org_uuid = excluded.org_uuid, pod_id = excluded.pod_id, region_id = excluded.region_id,
            updated_at = excluded.updated_at, updated_by = excluded.updated_by, user_id = excluded.user_id,
            user_name = excluded.user_name
    '''
    batch_upsert(cnx, sql, records)


def get_iics_agents_max_record_updated_at(cnx):
    sql = '''
        select max(record_updated_at) max_record_updated_at from iics_agents
    '''
    with cnx:
        with cnx.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
    if row is not None:
        return row.get('max_record_updated_at')


def get_iics_organizations_max_org_last_updated_at(cnx):
    sql = '''
        select max(org_last_updated_at) max_org_last_updated_at from iics_organizations
    '''
    with cnx:
        with cnx.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
    if row is not None:
        return row.get('max_org_last_updated_at')


def get_iics_user_logs_max_user_log_key(cnx):
    sql = '''
        select max(user_log_key) max_user_log_key from iics_user_logs
    '''
    with cnx:
        with cnx.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
    if row is not None:
        return row.get('max_user_log_key')


def get_iics_user_roles_max_updated_at(cnx):
    sql = '''
        select max(updated_at) max_updated_at from iics_user_roles
    '''
    with cnx:
        with cnx.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
    if row is not None:
        return row.get('max_updated_at')


def get_iics_users_max_updated_at(cnx):
    sql = '''
        select max(updated_at) max_updated_at from iics_users
    '''
    with cnx:
        with cnx.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
    if row is not None:
        return row.get('max_updated_at')
