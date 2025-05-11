import duckdb, os, logging, pandas as pd, sys, polars as pl, re

from src.databricks_io_utils import prepare_output_directory
from src.recon_utils import infer_excel_range
# from src.config import BASE_PATH

def rename_columns(table_name, mapping_df, source_col,original_to_clean):
    """
    Converts column names to clean column names or vice-versa
    """
    if original_to_clean:
        # Create a mapping from original to clean column names
        col_map = dict(zip(mapping_df[source_col], mapping_df[f"clean_{source_col}"]))
    else:
        # Create a mapping from clean to original column names
        col_map = dict(zip(mapping_df[f"clean_{source_col}"], mapping_df[source_col]))
    
    # Rename columns in the DuckDB table
    for before_col, after_col in col_map.items():
        if before_col != after_col:  # Only rename if names differ
            spark.sql(f"""CREATE OR REPLACE TEMP VIEW {table_name} AS SELECT * REPLACE ({before_col} AS {after_col}) FROM {table_name}""")

def dedup_table(source, pk):
    """deduplicates the input table based on the primary key"""

    if len(pk) > 0:

        spark.sql(f"""create or replace temp view {source}_dedup as
            select *, row_number() over (partition by {','.join(pk)}) as rn
            from {source}
        """)
        spark.sql(f"""create or replace temp view {source} as
            select * exclude(rn) from {source}_dedup where rn = 1""")
        
        spark.sql(f"""cache table {source}""")
    else:
        pass

# def create_secret_adls(account_name, sas_token_variable):
#     """create a session-scoped secret. needs sas token and account name as input arg"""
#     # print("\tCreating ADLS secret....") 
#     logging.info("\tCreating ADLS secret....") 
#     sas_token = os.environ.get(f'{sas_token_variable}') 
#     sql = f"""
#     SET azure_transport_option_type = 'curl';
#     CREATE OR REPLACE SECRET (
#     TYPE AZURE,
#     CONNECTION_STRING 'AccountName={account_name};SharedAccessSignature={sas_token}'
#     );
#     """        
#     duckdb.sql(sql)


def create_table_from_source(source_type, table_name, settings):
    """Creates in-memory cached temp views from various sources based on provided settings."""
    logging.info(f"	Creating table {table_name} from source: {source_type}")

#     CREATE TEMPORARY VIEW temp_view_name (column1 datatype, column2 datatype, ...)
# USING csv
# OPTIONS (path '/path/to/your/csv/file.csv', header 'true', delimiter ',')


    if source_type == "local_csv":
        spark.sql(rf"""
            CREATE OR REPLACE TEMPORARY VIEW {table_name} 
            USING CSV
            OPTIONS (path '{settings['file_path']}', 
                header={settings.get('has_header', True)}, 
                delimiter '{settings.get('sep', ',')}', 
                ignore_errors true);
            CACHE TABLE {table_name}
        """)

    elif source_type == "local_parquet":
        duckdb.sql(rf"""
            CREATE OR REPLACE TEMPORARY VIEW {table_name} 
            USING parquet
            OPTIONS (path '{settings['file_path']}', 
                ignore_errors true);
            CACHE TABLE {table_name}
        """)    

    elif source_type == "local_excel":
        file_path = settings['file_path']
        # skip_rows = settings.get("skip_rows", 0)
        sheet_name = settings.get("sheet_name", "Sheet1")

        # First copy file from ADLS to DBFS or local path
        dbutils.fs.cp(file_path, os.path.join("dbfs:","tmp",file_path.split("/")[-1]))

        file = pd.read_excel(os.path.join("","dbfs","tmp",file_path.split("/")[-1]), sheet_name=sheet_name)
        df = spark.createDataFrame(file)
        df.cache()
        df.createOrReplaceTempView(table_name)

    elif source_type == "uc_table":
        # create_secret_adls(settings['account_name'], settings['sas_token_variable'])
        spark.sql(f"""
            CREATE TEMP VIEW {table_name} AS
            SELECT * FROM delta.`{settings['table_url']}`
        """)
        spark.sql(f"""cache table {table_name}""")

    else:
        raise ValueError(f"Unsupported source_type: {source_type}")

def load_mapping_table_and_string_vars(path_name):
    mapping_path = os.path.join(path_name, "input", "mapping.csv")
    mapping_df = pd.read_csv(mapping_path)

    # Clean column names and add to mapping_df
    mapping_df["clean_source1"] = mapping_df["source1"].apply(lambda x: re.sub(r"[^a-zA-Z0-9]", "", x))
    mapping_df["clean_source2"] = mapping_df["source2"].apply(lambda x: re.sub(r"[^a-zA-Z0-9]", "", x))

    pk_source1 = mapping_df[mapping_df["is_pk"] == "y"]["clean_source1"].tolist()
    pk_source2 = mapping_df[mapping_df["is_pk"] == "y"]["clean_source2"].tolist()
    non_pk_source1 = mapping_df[mapping_df["is_pk"] != "y"]["clean_source1"].tolist()
    non_pk_source2 = mapping_df[mapping_df["is_pk"] != "y"]["clean_source2"].tolist()
    cols_source1 = mapping_df["clean_source1"].tolist()
    cols_source2 = mapping_df["clean_source2"].tolist()

    def prefix_cols(prefix, cols):
        return [f"{prefix}.{col}" for col in cols]

    return {
        "mapping_df": mapping_df,
        "pk_source1": pk_source1,
        "pk_source2": pk_source2,
        "non_pk_source1": non_pk_source1,
        "non_pk_source2": non_pk_source2,
        "cols_source1": cols_source1,
        "cols_source2": cols_source2,
        "source1_prefixed_string": ",".join(prefix_cols("source1", cols_source1)),
        "source2_prefixed_string": ",".join(prefix_cols("source2", cols_source2)),
        "source1_prefixed_select_string": ",".join(prefix_cols("source1", non_pk_source1)),
        "source2_prefixed_select_string": ",".join(prefix_cols("source2", non_pk_source2))
    }
####
def add_hash_column(table_name, cols, hash_col_name):
    """Adds a hash column to the databricks temp based on a list of columns."""
    col_expr = " || '|' || ".join([f"COALESCE(CAST({col} AS VARCHAR), '')" for col in cols])
    spark.sql(f"""UNCACHE TABLE {table_name}""")
    spark.sql(f"""
        CREATE OR REPLACE {table_name}_hash AS
        SELECT *, xxhash64({col_expr}) AS {hash_col_name} from {table_name}
    """)
    spark.sql(f"""CACHE TABLE {table_name}_hash""")

def write_df_to_excel(source, sheet_name, file_path):
    """Writes the output of an in-memory spark df to an Excel sheet."""
    df = spark.table(f"{source}")
    
    # Convert to Pandas
    df_pd = df_spark.toPandas()

    df.pd.to_excel(file_path, sheet_name=sheet_name, index=False)

def copy_table_disk(source, file_write_path=None, recon_scenario=None, mapping_df=None):
    """Wrapper that writes a spark df to Excel only if it has rows."""
    count = spark.sql(f"SELECT count(*) as cnt FROM {source}").collect()[0].cnt
    if count > 0:
        
        full_file_path = os.path.join(file_write_path, 'recon_output.xlsx')

        # Conditionally restore original column names (only for final output)
        if mapping_df is not None and source in ["source1", "source2"]:
            from src.databricks_utils import rename_columns_duckdb
            rename_columns_duckdb(source, mapping_df, source,False)

        write_df_to_excel(source, source, full_file_path)

        if recon_scenario is not None:
            recon_scenario.append(source)

        # logging.info(f"{source} written to {full_file_path}")


def create_exclusive_diff_tables(info):
    spark.sql(f"""
        CREATE TEMP VIEW source1_minus_source2 AS
        SELECT {info['source1_prefixed_string']} FROM source1
        LEFT JOIN source2 ON source1.pk_hash = source2.pk_hash
        WHERE source2.pk_hash IS NULL;

        CACHE TABLE source1_minus_source2;
    """)

    spark.sql(f"""
        CREATE TEMP VIEW source2_minus_source1 AS
        SELECT {info['source2_prefixed_string']} FROM source2
        LEFT JOIN source1 ON source2.pk_hash = source1.pk_hash
        WHERE source1.pk_hash IS NULL;

        CACHE TABLE source2_minus_source1;
    """)

def create_column_mismatches_table(info):
    spark.sql(rf"""
        CREATE TEMP VIEW column_mismatches AS
        SELECT * FROM (
            WITH mismatches AS (
                SELECT
                    {', '.join([f"CAST(source1.{col[8:]} AS VARCHAR) AS {col}" for col in info['source1_prefixed_select_string'].split(',')])},
                    {', '.join([f"CAST(source2.{col[8:]} AS VARCHAR) AS {col}" for col in info['source2_prefixed_select_string'].split(',')])},
                    source1.pk_hash
                FROM source1
                INNER JOIN source2
                ON source1.pk_hash = source2.pk_hash
                AND source1.non_pk_hash != source2.non_pk_hash
            ),
            mismatches_unpivot AS (
                UNPIVOT mismatches
                ON {', '.join(info['source1_prefixed_select_string'].split(','))},
                   {', '.join(info['source2_prefixed_select_string'].split(','))}
                INTO NAME col VALUE col_value
            ),
            mismatches_unpivot_source1 AS (
                SELECT mu.*, cm.col_id FROM mismatches_unpivot mu
                JOIN col_mapping cm ON mu.col = cm.col_name
                WHERE LEFT(col, 8) = 'source1_'
            ),
            mismatches_unpivot_source2 AS (
                SELECT mu.*, cm.col_id FROM mismatches_unpivot mu
                JOIN col_mapping cm ON mu.col = cm.col_name
                WHERE LEFT(col, 8) = 'source2_'
            ),
            output AS (
                SELECT
                    {', '.join(['source1.' + x for x in info['pk_source1']])},
                    mus1.pk_hash,
                    SUBSTRING(mus1.col, 7) AS source1_col_name,
                    mus1.col_value AS source1_col_val,
                    SUBSTRING(mus2.col, 7) AS source2_col_name,
                    mus2.col_value AS source2_col_val
                FROM mismatches_unpivot_source1 mus1
                INNER JOIN mismatches_unpivot_source2 mus2
                    ON mus1.pk_hash = mus2.pk_hash
                    AND mus1.col_id = mus2.col_id
                    AND mus1.col_value != mus2.col_value
                INNER JOIN source1 ON source1.pk_hash = mus1.pk_hash
            )
            SELECT * FROM output
        );

        CACHE TABLE column_mismatches;
    """)

def match_using_soft_pk(info):
    # Match rows with identical all_cols_hash values
    spark.sql(f"""
        CREATE TEMP VIEW source1_matches AS
        SELECT source1.*, source2.all_cols_hash AS matching_hash_from_other_source, 'matched' AS match_type
        FROM source1
        JOIN source2 ON source1.all_cols_hash = source2.all_cols_hash;
              
        CACHE TABLE source1_matches;
    """)

    spark.sql(f"""
        CREATE TEMP VIEW source2_matches AS
        SELECT source2.*, source1.all_cols_hash AS matching_hash_from_other_source, 'matched' AS match_type
        FROM source2
        JOIN source1 ON source2.all_cols_hash = source1.all_cols_hash;
               
        CACHE TABLE source2_matches;
    """)

    # Non-matching rows
    spark.sql(f"""
        CREATE TEMP VIEW source1_minus_source2 AS
        SELECT * FROM source1
        WHERE all_cols_hash NOT IN (SELECT all_cols_hash FROM source2);
              
        CACHE TABLE source1_minus_source2;
    """)

    spark.sql(f"""
        CREATE TEMP VIEW source2_minus_source1 AS
        SELECT * FROM source2
        WHERE all_cols_hash NOT IN (SELECT all_cols_hash FROM source1);
              
        CACHE TABLE source2_minus_source1;
    """)


def add_column(table_name, col_name, data_type = "STRING", default_val = False):
    add_col_sql = f"create or replace temp view {table_name} as add column {col_name} {data_type}"
    if default_val:
        add_col_sql += f" default {default_val}"
    # print(add_col_sql) 
    duckdb.sql(add_col_sql)

def cleanse_columns(table_name, col_list):
    """Cleanses each column in the table by removing non-alphanumeric characters."""
    if not col_list:
        logging.warning(f"No columns to cleanse in {table_name}.")
        return
    
    for col in col_list:
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW {table_name} AS
            SELECT * EXCEPT({col_list}), regexp_replace(CAST({col} AS VARCHAR), '[^a-zA-Z0-9]', '', 'g') AS {col}
            FROM {table_name}
        """)

def assign_row_numbers(info):
    """assigns row number based on pk columns"""

    for source in ["source1", "source2"]:
        col_list = ", ".join(info[f"pk_{source}"])
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW {source}_with_rn AS
            SELECT *, row_number() OVER (ORDER BY {col_list}) AS row_num
            FROM {source};
            CACHE TABLE {source}_with_rn
        """)

def tag_exact_row_matches():
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW matched AS
        SELECT
            source1.row_num AS source1_row_num,
            source2.row_num AS source2_row_num,
            row_number() OVER (ORDER BY (SELECT NULL)) AS running_row_num
        FROM source1_with_rn source1
        INNER JOIN source2_with_rn source2
        ON source1.pk_hash = source2.pk_hash
        AND source1.non_pk_hash = source2.non_pk_hash;
        CACHE TABLE matched;
    """)

    for source in ("source1", "source2"):
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW {source}_with_rn_final AS
            SELECT *, 
            CASE WHEN matched.running_row_num IS NOT NULL THEN True ELSE False END AS matched,
            matched.running_row_num as matching_row_num,
            CASE WHEN matched.running_row_num IS NOT NULL THEN 'absolute' ELSE NULL END AS match_type     
            FROM {source}_with_rn
            LEFT JOIN matched
            ON {source}_with_rn.row_num = matched.{source}_row_num;
            CACHE TABLE {source}_with_rn_final;
        """)

def tag_last_matched_row_number():
    for source in ["source1", "source2"]:
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW {source}_with_last_matched_rn AS
            SELECT *,
                   CAST(LAST_VALUE(matching_row_num IGNORE NULLS)
                   OVER (ORDER BY row_num) AS INT) AS last_matched_row_num
            FROM {source}_with_rn_final;
            CACHE TABLE {source}_with_last_matched_rn;
        """)

def run_fuzzy_matching(info):
    pk_cols = list(zip(info["pk_source1"], info["pk_source2"]))
    non_pk_cols = list(zip(info["non_pk_source1"], info["non_pk_source2"]))

    if not pk_cols:
        logging.info("No PK columns found — skipping fuzzy matching.")
        return

    num_cols = len(pk_cols)
    select_parts = []
    jws_cols = []
    ls_cols = []

    for s1, s2 in pk_cols:
        select_parts.append(f"""
            source1.{s1} AS source1_{s1},
            source2.{s2} AS source2_{s2},
            jaro_winkler_similarity(CAST(source1.{s1} AS VARCHAR), CAST(source2.{s2} AS VARCHAR)) AS jws_{s1}_{s2},
            1 - (levenshtein(CAST(source1.{s1} AS VARCHAR), CAST(source2.{s2} AS VARCHAR))::FLOAT /
                 CASE WHEN LENGTH(CAST(source1.{s1} AS VARCHAR)) > LENGTH(CAST(source2.{s2} AS VARCHAR))
                      THEN LENGTH(CAST(source1.{s1} AS VARCHAR)) ELSE LENGTH(CAST(source2.{s2} AS VARCHAR)) END) AS ls_{s1}_{s2}
        """)
        jws_cols.append(f"jws_{s1}_{s2}")
        ls_cols.append(f"ls_{s1}_{s2}")

    select_string = ",\n".join(select_parts)

    # Weighted similarity calculations
    weights = list(range(num_cols, 0, -1))
    jws_expr = " + ".join([f"{w} * {col}" for w, col in zip(weights, jws_cols)])
    ls_expr = " + ".join([f"{w} * {col}" for w, col in zip(weights, ls_cols)])
    denominator = sum(weights)

    # print(f"""
    #     CREATE OR REPLACE TEMP TABLE probable_match AS
    #     SELECT *,
    #            ({jws_expr}) / {denominator} AS jws_weighted,
    #            ({ls_expr}) / {denominator} AS ls_weighted,
    #            ROW_NUMBER() OVER (
    #                PARTITION BY source1_row_num
    #                ORDER BY CASE
    #                    WHEN ({jws_expr}) / {denominator} > ({ls_expr}) / {denominator}
    #                    THEN ({jws_expr}) / {denominator}
    #                    ELSE ({ls_expr}) / {denominator}
    #                END DESC
    #            ) AS rn
    #     FROM (
    #         SELECT {select_string},
    #                source1.row_num AS source1_row_num,
    #                source2.row_num AS source2_row_num,
    #                source1.non_pk_hash AS source1_non_pk_hash,
    #                source2.non_pk_hash AS source2_non_pk_hash
    #         FROM source1_with_last_matched_rn source1
    #         JOIN source2_with_last_matched_rn source2
    #           ON source1.last_matched_row_num = source2.last_matched_row_num
    #         WHERE source1.matched IS NULL
    #           AND source2.matched IS NULL
    #           AND source1.non_pk_hash = source2.non_pk_hash --will attempt recon only if non_pk_hashes are same
    #     )
    #     WHERE GREATEST(
    #         ({jws_expr}) / {denominator},
    #         ({ls_expr}) / {denominator}
    #     ) >= 0.7 -- Adjust threshold as needed
        

    # """)

    spark.sql(f"""
        CREATE OR REPLACE TEMP view probable_match AS
        SELECT * EXCEPT(source1_row_num, source2_row_num, source1_non_pk_hash, source2_non_pk_hash),
               ({jws_expr}) / {denominator} AS jws_weighted,
               ({ls_expr}) / {denominator} AS ls_weighted,
               ROW_NUMBER() OVER (
                   PARTITION BY source1_row_num
                   ORDER BY CASE
                       WHEN ({jws_expr}) / {denominator} > ({ls_expr}) / {denominator}
                       THEN ({jws_expr}) / {denominator}
                       ELSE ({ls_expr}) / {denominator}
                   END DESC
               ) AS rn
        FROM (
            SELECT {select_string},
                   source1.row_num AS source1_row_num,
                   source2.row_num AS source2_row_num,
                   source1.non_pk_hash AS source1_non_pk_hash,
                   source2.non_pk_hash AS source2_non_pk_hash
            FROM source1_with_last_matched_rn source1
            JOIN source2_with_last_matched_rn source2
              ON source1.last_matched_row_num = source2.last_matched_row_num
            WHERE source1.matched = False
              AND source2.matched = False
              AND source1.non_pk_hash = source2.non_pk_hash --will attempt recon only if non_pk_hashes are same
        )
        WHERE GREATEST(
            ({jws_expr}) / {denominator},
            ({ls_expr}) / {denominator}
        ) >= 0.7 -- Adjust threshold as needed
        

    """)

    logging.info("Fuzzy matching complete using all PK columns.")


def update_fuzzy_match_types(info):

    mapping_df = info["mapping_df"]
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW best_match AS
        SELECT * FROM probable_match WHERE rn = 1
    """)

    for source in ("source1", "source2"): # Update the matching_row_num and match_type for the best match
        other_source = "source2" if source == "source1" else "source1"
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW {source}_with_last_matched_rn_final AS
            SELECT * EXCEPT (matched, matching_row_num, match_type),
                   CASE WHEN best_match.rn IS NOT NULL THEN True ELSE False END AS matched,
                   best_match.{other_source}_row_num AS matching_row_num,
                   CASE WHEN best_match.rn IS NOT NULL THEN 'probable' ELSE 'none' END AS match_type
            FROM {source}_with_last_matched_rn
            LEFT JOIN best_match
            ON {source}_with_last_matched_rn.row_num = best_match.{source}_row_num
            WHERE {source}_with_last_matched_rn.row_num IS NOT NULL
        """)

    for source in ["source1", "source2"]:
        other_source = "source2" if source == "source1" else "source1"
        prefixed_string = info[f"{source}_prefixed_string"]

        spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW {source}_matches AS
        SELECT {prefixed_string}, 
        {other_source}.pk_hash AS matching_hash_from_other_source,
        {source}.match_type
        FROM {source}_with_last_matched_rn {source}
        JOIN {other_source}_with_rn {other_source}
        ON {source}.matching_row_num = {other_source}.row_num
        WHERE {source}.match_type IN ('absolute', 'probable')
        """)

    select_source1 = ",".join(info["cols_source1"])
    select_source2 = ",".join(info["cols_source2"])

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW source1_minus_source2 AS
        SELECT {select_source1} FROM source1_with_last_matched_rn WHERE match_type = 'none'
    """)

    rename_columns("source1_minus_source2", mapping_df, "source1", False)

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW source2_minus_source1 AS
        SELECT {select_source2} FROM source2_with_last_matched_rn WHERE match_type = 'none'
    """)

    rename_columns("source2_minus_source1", mapping_df, "source2", False)

def dry_run():
        if __name__ == "__main__":
            print("🔍 Dry-run test starting...")

        # Example setup for quick manual run
        settings = {
            "file_path": "inputs/td.csv",
            "delimiter": ",",
            "header": True
        }

        try:
            create_table_from_source("csv", "test_table", settings)
            print("Table created successfully!")

            add_hash_column("test_table", ["col1", "col2"], "row_hash")
            print("Hash column added.")

            cleanse_columns("test_table", ["col1", "col2"])
            print("Columns cleansed.")

            prepare_output_directory("inputs")  # This will just print the path
            print("Output directory ready.")

        except Exception as e:
            print(f"Error: {e}")
