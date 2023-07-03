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