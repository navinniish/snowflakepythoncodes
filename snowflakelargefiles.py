import snowflake.connector
import os
from tqdm import tqdm

OKTA_USER = 'xxxx'

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=f'{xxxx}',
        account='xxxx',
        authenticator='externalbrowser',
        warehouse='xxx',
        role='xxx',
        database='xxx',
        schema="xxx"
    )

table_name = 'NKS_RHETORIK'
file_path = 'NKS_RHETORIK.csv'
stage_name = 'MY_INTERNAL_STAGE'

conn = get_snowflake_connection()
cursor = conn.cursor()

try:
    with tqdm(total=4, desc="Overall Progress") as pbar:
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            CONTINENT VARCHAR,
            COUNTRY VARCHAR,
            COMPANY_ID VARCHAR,
            COMPANY_NAME VARCHAR,
            COMPANY_TYPE VARCHAR,
            COMPANY_FOUNDED_DATE VARCHAR,
            COMPANY_CLOSED_DATE VARCHAR,
            COMPANY_STATUS VARCHAR,
            COMPANY_WEBSITE VARCHAR,
            COMPANY_PHONE VARCHAR,
            COMPANY_LINKEDIN_URL VARCHAR,
            COMPANY_FACEBOOK_URL VARCHAR,
            COMPANY_INSTAGRAM_URL VARCHAR,
            COMPANY_TWITTER_URL VARCHAR,
            NUMBER_OF_EMPLOYEES VARCHAR,
            NUMBER_OF_EMPLOYEES_CODE VARCHAR,
            REVENUE_CURRENCY VARCHAR,
            COMPANY_REVENUE VARCHAR,
            COMPANY_REVENUE_RANGE VARCHAR,
            PRIMARY_INDUSTRY VARCHAR,
            SECONDARY_INDUSTRY VARCHAR,
            OWNERSHIP_TYPE VARCHAR,
            STREET VARCHAR,
            CITY VARCHAR,
            STATE VARCHAR,
            ZIPCODE VARCHAR
        )
        """
        cursor.execute(create_table_query)
        pbar.update(1)
        pbar.set_description("Table created")

        cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")
        cursor.execute(f"PUT file://{file_path} @{stage_name} AUTO_COMPRESS=TRUE")
        pbar.update(1)
        pbar.set_description("File staged")

        copy_query = f"""
        COPY INTO {table_name}
        FROM @{stage_name}/{os.path.basename(file_path)}.gz
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            FIELD_DELIMITER = ','
            TRIM_SPACE = TRUE
            NULL_IF = ('NULL', 'null', '')
        )
        ON_ERROR = CONTINUE
        PURGE = TRUE
        """
        cursor.execute(copy_query)
        pbar.update(1)
        pbar.set_description("Data copied")

        merge_query = f"""
        MERGE INTO {table_name} t
        USING (SELECT * FROM {table_name}) s
        ON t.COMPANY_ID = s.COMPANY_ID
        WHEN NOT MATCHED THEN INSERT (
            CONTINENT, COUNTRY, COMPANY_ID, COMPANY_NAME, COMPANY_TYPE,
            COMPANY_FOUNDED_DATE, COMPANY_CLOSED_DATE, COMPANY_STATUS,
            COMPANY_WEBSITE, COMPANY_PHONE, COMPANY_LINKEDIN_URL,
            COMPANY_FACEBOOK_URL, COMPANY_INSTAGRAM_URL, COMPANY_TWITTER_URL,
            NUMBER_OF_EMPLOYEES, NUMBER_OF_EMPLOYEES_CODE, REVENUE_CURRENCY,
            COMPANY_REVENUE, COMPANY_REVENUE_RANGE, PRIMARY_INDUSTRY,
            SECONDARY_INDUSTRY, OWNERSHIP_TYPE, STREET, CITY, STATE, ZIPCODE
        ) VALUES (
            s.CONTINENT, s.COUNTRY, s.COMPANY_ID, s.COMPANY_NAME, s.COMPANY_TYPE,
            s.COMPANY_FOUNDED_DATE, s.COMPANY_CLOSED_DATE, s.COMPANY_STATUS,
            s.COMPANY_WEBSITE, s.COMPANY_PHONE, s.COMPANY_LINKEDIN_URL,
            s.COMPANY_FACEBOOK_URL, s.COMPANY_INSTAGRAM_URL, s.COMPANY_TWITTER_URL,
            s.NUMBER_OF_EMPLOYEES, s.NUMBER_OF_EMPLOYEES_CODE, s.REVENUE_CURRENCY,
            s.COMPANY_REVENUE, s.COMPANY_REVENUE_RANGE, s.PRIMARY_INDUSTRY,
            s.SECONDARY_INDUSTRY, s.OWNERSHIP_TYPE, s.STREET, s.CITY, s.STATE, s.ZIPCODE
        )
        """
        cursor.execute(merge_query)
        pbar.update(1)
        pbar.set_description("Data merged")

    print("Data load completed successfully.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    cursor.close()
    conn.close()
