import logging
import psycopg2.extras

log = logging.getLogger(__name__)


def batch_upsert(cnx, sql, records):
    with cnx:
        with cnx.cursor() as cur:
            log.info(f'Saving {len(records)} records to Postgres')
            psycopg2.extras.execute_batch(cur, sql, records)


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
            customer_type = %(customer_type)s, environment_type = %(environment_type)s, is_active = %(is_active)s,
            is_current = %(is_current)s, is_deleted = %(is_deleted)s, is_disabled = %(is_disabled)s,
            org_edition_type = %(org_edition_type)s, org_expires_at = %(org_expires_at)s, org_id = %(org_id)s,
            org_last_updated_at = %(org_last_updated_at)s, org_name = %(org_name)s,
            org_registered_at = %(org_registered_at)s, org_type = %(org_type)s, org_type_lic = %(org_type_lic)s,
            org_uuid = %(org_uuid)s, parent_org_id = %(parent_org_id)s, pod_id = %(pod_id)s, region_id = %(region_id)s,
            time_zone = %(time_zone)s
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
            created_at = %(created_at)s, created_by = %(created_by)s, is_deleted = %(is_deleted)s,
            is_read_only = %(is_read_only)s, org_id = %(org_id)s, org_key = %(org_key)s, org_uuid = %(org_uuid)s,
            pod_id = %(pod_id)s, region_id = %(region_id)s, role_description = %(role_description)s,
            role_id = %(role_id)s, role_name = %(role_name)s, updated_at = %(updated_at)s, updated_by = %(updated_by)s,
            user_id = %(user_id)s, user_key = %(user_key)s, user_role_uuid = %(user_role_uuid)s
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
            created_at = %(created_at)s, created_by = %(created_by)s, email = %(email)s, first_name = %(first_name)s,
            full_name = %(full_name)s, is_deleted = %(is_deleted)s, last_name = %(last_name)s, org_id = %(org_id)s,
            org_key = %(org_key)s, org_uuid = %(org_uuid)s, pod_id = %(pod_id)s, region_id = %(region_id)s,
            updated_at = %(updated_at)s, updated_by = %(updated_by)s, user_id = %(user_id)s, user_name = %(user_name)s
    '''
    batch_upsert(cnx, sql, records)


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
