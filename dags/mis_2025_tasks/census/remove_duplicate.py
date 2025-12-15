from airflow.providers.postgres.hooks.postgres import PostgresHook

def remove_duplicate_census(**kwargs):
    """
    Deduplicates the census table based on rowID.
    Prioritizes records where selected > 0 and random > 0.
    """
    POSTGRES_CONN_ID = kwargs.get("POSTGRES_CONN_ID", "PG-MIS-2025")
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    # The CTE identifies duplicates and ranks them based on your priorities
    # rn = 1 is the 'winner' we keep.
    dedup_sql = """
        WITH prioritized_rows AS (
            SELECT 
                instanceID,
                ROW_NUMBER() OVER (
                    PARTITION BY rowID 
                    ORDER BY 
                        (selected > 0) DESC, 
                        (random > 0) DESC,   
                        instanceID ASC       
                ) as rn
            FROM census
        )
        DELETE FROM census
        WHERE instanceID IN (
            SELECT instanceID 
            FROM prioritized_rows 
            WHERE rn > 1
        );
    """
    
    with pg.get_conn() as conn:
        with conn.cursor() as cursor:
            print("Starting deduplication process for census table...")
            cursor.execute(dedup_sql)
            row_count = cursor.rowcount
            conn.commit()
            print(f"Deduplication complete. Removed {row_count} duplicate rows.")

    return f"Removed {row_count} duplicates."