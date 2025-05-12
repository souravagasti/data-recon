import os, logging, pandas as pd, sys, re, uuid, pyspark.pandas as ps
from math import floor, ceil
#heheheehh

from src.databricks_io_utils import prepare_output_directory
from src.recon_utils import infer_excel_range
from src.config import args
from databricks.sdk.runtime import *

# prerequisites
# creation of delta tables in "temp_recon" schema as
# it is assumed that the recon is being run in a UC enabled workspace
# session_guid = str(uuid.uuid4())
# session_guid = session_guid.replace("-", "_")
session_guid = args.run_id
print(f"Session GUID: {session_guid}")
print("args.platform",args.platform)

spark.sql("create schema if not exists temp_recon")
spark.sql(f"use schema temp_recon")

# Python3 implementation of above approach

 
def fn_jaro_winkler_similarity(s1, s2):
     #lifted from https://www.geeksforgeeks.org/jaro-and-jaro-winkler-similarity/

	# If the s are equal
	if (s1 == s2):
		return 1.0
 
	# Length of two s
	len1 = len(s1)
	len2 = len(s2)
 
	# Maximum distance upto which matching
	# is allowed
	max_dist = floor(max(len1, len2) / 2) - 1
 
	# Count of matches
	match = 0
 
	# Hash for matches
	hash_s1 = [0] * len(s1)
	hash_s2 = [0] * len(s2)
 
	# Traverse through the first
	for i in range(len1):
 
		# Check if there is any matches
		for j in range(max(0, i - max_dist), 
					min(len2, i + max_dist + 1)):
			# If there is a match
			if (s1[i] == s2[j] and hash_s2[j] == 0):
				hash_s1[i] = 1
				hash_s2[j] = 1
				match += 1
				break
 
	# If there is no match
	if (match == 0):
		return 0.0
 
	# Number of transpositions
	t = 0
	point = 0
 
	# Count number of occurrences
	# where two characters match but
	# there is a third matched character
	# in between the indices
	for i in range(len1):
		if (hash_s1[i]):
 
			# Find the next matched character
			# in second
			while (hash_s2[point] == 0):
				point += 1
 
			if (s1[i] != s2[point]):
				t += 1
			point += 1
	t = t//2
 
	# Return the Jaro Similarity
	return (match/ len1 + match / len2 +
			(match - t) / match)/ 3.0


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
    
    # Rename columns in the table
    for before_col, after_col in col_map.items():
        if before_col != after_col:  # Only rename if names differ
            spark.sql(f"""ALTER TABLE {table_name}_{session_guid} RENAME COLUMN "{before_col}" TO "{after_col}" """)

def dedup_table(source, pk):
    """deduplicates the input table based on the primary key"""

    if len(pk) > 0:
        
        spark.sql(f"""create or replace table {source}_dedup_{session_guid} as
            select *, row_number() over (partition by {','.join(pk)} order by 1) as rn
            from {source}_{session_guid}
        """)

        spark.sql(f"""create or replace table {source}_{session_guid} as
            select * except(rn) from {source}_dedup_{session_guid} where rn = 1""")
    else:
        pass

def create_secret_adls(account_name, sas_token_variable):
    """create a session-scoped secret. needs sas token and account name as input arg"""
    # print("\tCreating ADLS secret....") 
    logging.info("\tCreating ADLS secret....") 
    sas_token = os.environ.get(f'{sas_token_variable}') 
    sql1 = f"""
    SET azure_transport_option_type = 'curl';
    """    
    sql2 = f"""CREATE OR REPLACE SECRET (
    TYPE AZURE,
    CONNECTION_STRING 'AccountName={account_name};SharedAccessSignature={sas_token}'
    );"""
    spark.sql(sql1)
    spark.sql(sql2)

def create_table_from_source(source_type, table_name, settings):
    """Creates temp delta tables from various sources based on provided settings."""
    logging.info(f"	Creating table {table_name} from source: {source_type}")

    if source_type == "local_csv":

        df = spark.read.csv(settings["file_path"], header=settings.get("has_header", True), sep=settings.get("sep", ","))

        for col in df.columns:
            df = df.withColumnRenamed(col, re.sub(r'[^a-zA-Z0-9_]', '_', col)) #clean columns

        df.createOrReplaceTempView("df_temp")

        spark.sql(rf"""
        CREATE OR REPLACE TABLE {table_name}_{session_guid}
        AS SELECT * FROM df_temp
        """)

    elif source_type == "local_parquet":

        df = spark.read.parquet(settings["file_path"])

        for col in df.columns:
            df = df.withColumnRenamed(col, re.sub(r'[^a-zA-Z0-9_]', '_', col)) #clean columns

        df.createOrReplaceTempView("df_temp")

        spark.sql(rf"""
        CREATE OR REPLACE TABLE {table_name}_{session_guid}
        AS SELECT * FROM df_temp
        """)

    elif source_type == "local_excel":

        file_path = settings['file_path']
        # skip_rows = settings.get("skip_rows", 0)
        sheet_name = settings.get("sheet_name", "Sheet1")

        # First copy file from ADLS to DBFS or local path
        #install openpyxl on the node

        dbutils.fs.cp(file_path, os.path.join("dbfs:","tmp",file_path.split("/")[-1]))
        file = pd.read_excel(os.path.join("/dbfs/tmp",file_path.split("/")[-1]), sheet_name=sheet_name, engine="openpyxl",dtype=str)

        df = spark.createDataFrame(file)
        for col in df.columns:
            df = df.withColumnRenamed(col, re.sub(r'[^a-zA-Z0-9_]', '_', col)) #clean columns

        df.createOrReplaceTempView("df_temp")

        spark.sql(f"CREATE OR REPLACE TABLE {table_name}_{session_guid} AS SELECT * FROM df_temp")

        # spark.sql(f"CREATE OR REPLACE TABLE {table_name}_{session_guid} AS SELECT * FROM {df},df=df")
        
    elif source_type == "uc_table":
        # create_secret_adls(settings['account_name'], settings['sas_token_variable'])
        spark.sql(f"""
            CREATE TABLE {table_name}_{session_guid} AS
            SELECT * FROM delta.`{settings['table_url']}`
        """)

    else:
        raise ValueError(f"Unsupported source_type: {source_type}")

def load_mapping_table_and_string_vars(path_name):
    import re, uuid,os
    from pyspark.sql import functions as F
    session_guid = uuid.uuid4().hex

    mapping_path = os.path.join(path_name, "input", "mapping.csv")
    mapping_df = spark.read.csv(mapping_path, header=True)

    mapping_df = mapping_df.select('*').toPandas()


    ####### below is SPARK compatible code. But to keep it simple
    ####### and since it's a small dataset, I chose to use pandas

    # mapping_df = mapping_df.withColumn("source1", F.regexp_replace(F.col("source1"), r"[^a-zA-Z0-9]", "")).withColumn("source2", F.regexp_replace(F.col("source2"), r"[^a-zA-Z0-9]", ""))

    # cols_source1 = [row["source1"] for row in mapping_df.select("source1").collect()]
    # cols_source2 = [row["source2"] for row in mapping_df.select("source2").collect()]
    # pk_source1 = [row["source1"] for row in mapping_df[mapping_df["is_pk"] == "y"].select("source1").collect()]
    # pk_source2 = [row["source2"] for row in mapping_df[mapping_df["is_pk"] == "y"].select("source2").collect()]
    # non_pk_source1 = [row["source1"] for row in mapping_df[mapping_df["is_pk"] != "y"].select("source1").collect()]
    # non_pk_source2 = [row["source2"] for row in mapping_df[mapping_df["is_pk"] != "y"].select("source2").collect()]

    #Clean column names
    mapping_df["source1"] = mapping_df["source1"].apply(lambda x: re.sub(r"[^a-zA-Z0-9]", "_", x))
    mapping_df["source2"] = mapping_df["source2"].apply(lambda x: re.sub(r"[^a-zA-Z0-9]", "_", x))

    pk_source1 = mapping_df[mapping_df["is_pk"] == "y"]["source1"].tolist()
    pk_source2 = mapping_df[mapping_df["is_pk"] == "y"]["source2"].tolist()
    non_pk_source1 = mapping_df[mapping_df["is_pk"] != "y"]["source1"].tolist()
    non_pk_source2 = mapping_df[mapping_df["is_pk"] != "y"]["source2"].tolist()
    cols_source1 = mapping_df["source1"].tolist()
    cols_source2 = mapping_df["source2"].tolist()

    # spark.sql(f"drop table if exists col_mapping_{session_guid};")
    # spark.sql(f"create table col_mapping_{session_guid}(col_name string, col_id integer)")

    # for i,j in enumerate(zip(cols_source1, cols_source2)):
    #     spark.sql(f"INSERT INTO col_mapping_{session_guid} SELECT 'source1_{j[0]}',{i}")
    #     spark.sql(f"INSERT INTO col_mapping_{session_guid} SELECT 'source2_{j[1]}',{i}")

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

def add_hash_column(table_name, cols, hash_col_name):
    """Adds a hash column to the spark table based on a list of columns."""
    col_expr = " || '|' || ".join([f"COALESCE(CAST({col} AS STRING), '')" for col in cols])
    
    spark.sql(f"""
        ALTER TABLE {table_name}_{session_guid} ADD COLUMN {hash_col_name} STRING;
    """)
    spark.sql(f"""UPDATE {table_name}_{session_guid} SET {hash_col_name} = xxhash64({col_expr});""")

def write_df_to_excel(source, sheet_name, file_path):
    """Writes the output of an in-memory spark df to an Excel sheet."""
    df_spark = spark.table(f"{source}_{session_guid}")
    remove_cols = ["source1_row_num", "source2_row_num", "source1_non_pk_hash", "source2_non_pk_hash"]
    if any(col in df_spark.columns for col in remove_cols):
        df_spark = df_spark.drop(*remove_cols)
    
    # df_spark.write \
    #     .format("com.crealytics.spark.excel") \
    #     .option("header", True) \
    #     .option("dataAddress", f"""'{sheet_name}'!A1""") \
    #     .mode("overwrite") \
    #     .save(file_path)  
    
    # Convert to Pandas
    df_pd = df_spark.toPandas()

    local_tmp_path = f"/tmp/{session_guid}_recon_output.xlsx"
    dbfs_tmp_path = f"dbfs:/tmp/{session_guid}/recon_output.xlsx"
    # Save locally (NOT under /dbfs)

    print("local_tmp_path",local_tmp_path)
    print("dbfs_tmp_path",dbfs_tmp_path)

    if os.path.exists(local_tmp_path):
        with pd.ExcelWriter(local_tmp_path, engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
            df_pd.to_excel(writer, sheet_name=sheet_name[:31], index=False)
            print("mode append")
    else:
        with pd.ExcelWriter(local_tmp_path, engine='openpyxl', mode='w') as writer:
            df_pd.to_excel(writer, sheet_name=sheet_name[:31], index=False)
            print("mode overwrite")
    print("copied to local")

    # Copy to DBFS path
    dbutils.fs.cp(f"file:{local_tmp_path}", dbfs_tmp_path)
    print("copied to dbfs tmp")

def copy_table_disk(source, file_write_path=None, recon_scenario=None, mapping_df=None):
    """Wrapper that writes a spark df to Excel only if it has rows."""
    count = spark.sql(f"SELECT count(*) as cnt FROM {source}_{session_guid}").collect()[0].cnt
    if count > 0:
        
        full_file_path = os.path.join(file_write_path, 'recon_output.xlsx')

        write_df_to_excel(source, source, full_file_path)

        # if recon_scenario is not None:
        #     recon_scenario.append(source)

def create_exclusive_diff_tables(info):
    spark.sql(f"""
        CREATE TABLE source1_minus_source2_{session_guid} AS
        SELECT {info['source1_prefixed_string']} FROM source1_{session_guid}
        LEFT JOIN source2_{session_guid} ON source1.pk_hash = source2.pk_hash
        WHERE source2.pk_hash IS NULL
    """)

    spark.sql(f"""
        CREATE TABLE source2_minus_source1_{session_guid} AS
        SELECT {info['source2_prefixed_string']} FROM source2_{session_guid}
        LEFT JOIN source1_{session_guid} ON source2.pk_hash = source1.pk_hash
        WHERE source1.pk_hash IS NULL
    """)

def create_column_mismatches_table(info):
    spark.sql(rf"""
        CREATE OR REPLACE TABLE column_mismatches_{session_guid} AS
        SELECT * FROM (
            WITH mismatches AS (
                SELECT
                    {', '.join([f"CAST(source1.{col[8:]} AS STRING) AS {col}" for col in info['source1_prefixed_select_string'].split(',')])},
                    {', '.join([f"CAST(source2.{col[8:]} AS STRING) AS {col}" for col in info['source2_prefixed_select_string'].split(',')])},
                    source1.pk_hash
                FROM source1_{session_guid}
                INNER JOIN source2_{session_guid}
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
                JOIN col_mapping_{session_guid} cm ON mu.col = cm.col_name
                WHERE LEFT(col, 8) = 'source1_'
            ),
            mismatches_unpivot_source2 AS (
                SELECT mu.*, cm.col_id FROM mismatches_unpivot mu
                JOIN col_mapping_{session_guid} cm ON mu.col = cm.col_name
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
                INNER JOIN source1_{session_guid} ON source1.pk_hash = mus1.pk_hash
            )
            SELECT * FROM output
        )
    """)

def match_using_soft_pk(info):

    # Match rows with identical all_cols_hash values
    spark.sql(f"""
        CREATE TABLE source1_matches_{session_guid} AS
        SELECT source1.*, source2.all_cols_hash AS matching_hash_from_other_source, 'matched' AS match_type
        FROM source1_{session_guid} source1
        JOIN source2_{session_guid} source2 
        ON source1.all_cols_hash = source2.all_cols_hash
    """)

    spark.sql(f"""
        CREATE TABLE source2_matches_{session_guid} AS
        SELECT source2.*, source1.all_cols_hash AS matching_hash_from_other_source, 'matched' AS match_type
        FROM source2_{session_guid} source2
        JOIN source1_{session_guid} source1
        ON source2.all_cols_hash = source1.all_cols_hash
    """)

    # Non-matching rows
    spark.sql(f"""
        CREATE TABLE source1_minus_source2_{session_guid} AS
        SELECT * FROM source1_{session_guid}
        WHERE all_cols_hash NOT IN (SELECT all_cols_hash FROM source2_{session_guid})
    """)

    spark.sql(f"""
        CREATE TABLE source2_minus_source1_{session_guid} AS
        SELECT * FROM source2_{session_guid}
        WHERE all_cols_hash NOT IN (SELECT all_cols_hash FROM source1_{session_guid})
    """)
    
def add_column(table_name, col_name, data_type = "STRING", default_val = False):
    add_col_sql = f"alter table {table_name}_{session_guid} add column {col_name} {data_type}"
    if default_val:
        add_col_sql += f" default {default_val}"
    # print(add_col_sql) 
    spark.sql(add_col_sql)

def cleanse_columns(table_name, col_list):
    """Cleanses each column in the table by removing non-alphanumeric characters."""
    if not col_list:
        logging.warning(f"No columns to cleanse in {table_name}.")
        return

    update_sql = ", ".join([
        f"{col} = regexp_replace(CAST({col} AS STRING), '[^a-zA-Z0-9]', '')"
        for col in col_list
    ])
    spark.sql(f"UPDATE {table_name}_{session_guid} SET {update_sql}")

def assign_row_numbers(info):
    """assigns row number based on pk columns"""

    for source in ["source1", "source2"]:
        col_list = ", ".join(info[f"pk_{source}"])
        spark.sql(f"""
            CREATE OR REPLACE TABLE {source}_with_rn_{session_guid} AS
            SELECT *, row_number() OVER (ORDER BY {col_list}) AS row_num
            FROM {source}_{session_guid}
        """)

def tag_exact_row_matches():

    spark.sql(f"""
        CREATE OR REPLACE TABLE matched_{session_guid} AS
        SELECT
            source1.row_num AS source1_row_num,
            source2.row_num AS source2_row_num,
            row_number() OVER (ORDER BY 1) AS running_row_num
        FROM source1_with_rn_{session_guid} source1
        INNER JOIN source2_with_rn_{session_guid} source2
        ON source1.pk_hash = source2.pk_hash
        AND source1.non_pk_hash = source2.non_pk_hash
    """)

    for source, other_source in [("source1", "source2"), ("source2", "source1")]:

        spark.sql(f"""
            ALTER TABLE {source}_with_rn_{session_guid} ADD COLUMN matched BOOLEAN;
        """)

        spark.sql(f"""
            ALTER TABLE {source}_with_rn_{session_guid} ADD COLUMN matching_row_num INTEGER;
        """)

        spark.sql(f"""
            ALTER TABLE {source}_with_rn_{session_guid} ADD COLUMN match_type STRING;
        """)
        print(4.1)
        spark.sql(f"""
    MERGE INTO {source}_with_rn_{session_guid} AS target
    USING (
        SELECT * FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY {source}_row_num ORDER BY running_row_num) AS rn
            FROM matched_{session_guid}
        ) tmp
        WHERE rn = 1
    ) AS matched
    ON target.row_num = matched.{source}_row_num
    WHEN MATCHED THEN UPDATE SET
        matched = TRUE,
        matching_row_num = matched.running_row_num,
        match_type = 'absolute'
""")

        print(4.2)
        spark.sql(f"""UPDATE {source}_with_rn_{session_guid}
            SET matched = False
            WHERE matched != True
            """)
        print(4.3)

def tag_last_matched_row_number():

    for source in ["source1", "source2"]:
        spark.sql(f"""
        CREATE OR REPLACE TABLE {source}_with_last_matched_rn_{session_guid} AS
        SELECT *,
            CAST(
                LAST_VALUE(matching_row_num, TRUE)
                OVER (ORDER BY row_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS INT) AS last_matched_row_num
        FROM {source}_with_rn_{session_guid}
        """)

def run_fuzzy_matching(info):
    pk_cols = list(zip(info["pk_source1"], info["pk_source2"]))
    non_pk_cols = list(zip(info["non_pk_source1"], info["non_pk_source2"]))

    from pyspark.sql.functions import expr, greatest, length,udf
    from pyspark.sql.types import DoubleType
    jaro_winkler_similarity=udf(fn_jaro_winkler_similarity, DoubleType())
    spark.udf.register("jaro_winkler_similarity", jaro_winkler_similarity)

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
            jaro_winkler_similarity(CAST(source1.{s1} AS STRING), CAST(source2.{s2} AS STRING)) AS jws_{s1}_{s2},
            1 - (levenshtein(CAST(source1.{s1} AS STRING), CAST(source2.{s2} AS STRING))::FLOAT /
                 CASE WHEN LENGTH(CAST(source1.{s1} AS STRING)) > LENGTH(CAST(source2.{s2} AS STRING))
                      THEN LENGTH(CAST(source1.{s1} AS STRING)) ELSE LENGTH(CAST(source2.{s2} AS STRING)) END) AS ls_{s1}_{s2}
        """)
        jws_cols.append(f"jws_{s1}_{s2}")
        ls_cols.append(f"ls_{s1}_{s2}")

    select_string = ",\n".join(select_parts)

    # Weighted similarity calculations
    weights = list(range(num_cols, 0, -1))
    jws_expr = " + ".join([f"{w} * {col}" for w, col in zip(weights, jws_cols)])
    ls_expr = " + ".join([f"{w} * {col}" for w, col in zip(weights, ls_cols)])
    denominator = sum(weights)

    spark.sql(f"""
        CREATE OR REPLACE TABLE probable_match_{session_guid} AS
        SELECT *,
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
            FROM source1_with_last_matched_rn_{session_guid} source1
            JOIN source2_with_last_matched_rn_{session_guid} source2
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

    spark.sql(f"""
        CREATE OR REPLACE TABLE best_match_{session_guid} AS
        SELECT * FROM probable_match_{session_guid} WHERE rn = 1
    """)

    for source, other_source in [("source1", "source2"), ("source2", "source1")]:
        # Corrected: Use MERGE INTO instead of UPDATE ... FROM
        spark.sql(f"""
            MERGE INTO {source}_with_last_matched_rn_{session_guid} AS target
            USING best_match_{session_guid} AS best_match
            ON target.row_num = best_match.{source}_row_num
            WHEN MATCHED THEN UPDATE SET
                target.matching_row_num = best_match.{other_source}_row_num,
                target.match_type = 'probable'
        """)

        spark.sql(f"""
            UPDATE {source}_with_last_matched_rn_{session_guid}
            SET match_type = 'none'
            WHERE match_type IS NULL
        """)

    for source in ["source1", "source2"]:
        other_source = "source2" if source == "source1" else "source1"
        prefixed_string = info[f"{source}_prefixed_string"]

        spark.sql(f"""
            CREATE OR REPLACE TABLE {source}_matches_{session_guid} AS
            SELECT {prefixed_string},
                   {other_source}.pk_hash AS matching_hash_from_other_source,
                   {source}.match_type
            FROM {source}_with_last_matched_rn_{session_guid} {source}
            JOIN {other_source}_with_rn_{session_guid} {other_source}
              ON {source}.matching_row_num = {other_source}.row_num
            WHERE {source}.match_type IN ('absolute', 'probable')
        """)

    select_source1 = ",".join(info["cols_source1"])
    select_source2 = ",".join(info["cols_source2"])

    spark.sql(f"""
        CREATE OR REPLACE TABLE source1_minus_source2_{session_guid} AS
        SELECT {select_source1}
        FROM source1_with_last_matched_rn_{session_guid}
        WHERE match_type = 'none'
    """)

    # rename_columns("source1_minus_source2", mapping_df, "source1", False)

    spark.sql(f"""
        CREATE OR REPLACE TABLE source2_minus_source1_{session_guid} AS
        SELECT {select_source2}
        FROM source2_with_last_matched_rn_{session_guid}
        WHERE match_type = 'none'
    """)

    # rename_columns("source2_minus_source1", mapping_df, "source2", False)

    # # Clean up the probable_match table
    # for column in ["source1_row_num", "source2_row_num", "source1_non_pk_hash", "source2_non_pk_hash"]:
    #     spark.sql(f"ALTER TABLE probable_match_{session_guid} DROP COLUMN {column}")


# cleanup_utils.py
def drop_temp_tables(session_guid):
    tables = [
        "source1", "source2", "source1_matches", "source2_matches",
        "source1_minus_source2", "source2_minus_source1", "probable_match"
    ]
    for table in tables:
        full_table = f"{table}_{session_guid}"
        spark.sql(f"DROP TABLE IF EXISTS {full_table}")

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
