# Directory Structure (suggested):
# recon_project/
# ├── inputs/
# │   ├── csv_template.json
# │   ├── excel_template.json
# │   ├── parquet_template.json
# │   └── delta_template.json
# └── src/
#     ├── __init__.py
#     ├── config.py
#     ├── recon.py
#     ├── io_utils.py
#     ├── duckdb_utils.py
#     └── recon_runner.py

# Here's a clean version of "recon.py" after breaking concerns into modules.
# Starting with src/recon.py:

from src.config import args
session_guid = args.run_id
platform = args.platform

import logging

# print(f"Recon module initialized with platform: {args.platform}")

if args.platform == "duckdb":
    from src.duckdb_utils import *
    from src.duckdb_io_utils import *
if args.platform == "databricks":
    from src.databricks_utils import *
    from src.databricks_io_utils import *
if args.platform == "duckdb_on_databricks":
    from src.duckdb_utils import *
    from src.databricks_io_utils import *

class Recon:
    def __init__(self, path_name, source_type_1, settings1, source_type_2, settings2):
        # print(f"Recon module initialized with platform: {args.platform}")
        # print("some1")
        self.path_name = path_name
        self.source_type_1 = source_type_1
        self.settings1 = settings1
        self.source_type_2 = source_type_2
        self.settings2 = settings2
        # print("inside init")
        self.file_write_path = prepare_output_directory(self.path_name,session_guid)
        # args.run_id = self.run_id
        self.recon_scenario = []

    def setup(self):
        mapping_path = os.path.join(self.path_name, "input", "mapping.csv")
        self.info = load_mapping_table_and_string_vars(mapping_path)
        create_table_from_source(self.source_type_1, "source1", self.settings1)
        create_table_from_source(self.source_type_2, "source2", self.settings2)
        # rename_columns("source1", self.info["mapping_df"], "source1",True)
        # rename_columns("source2", self.info["mapping_df"], "source2",True)
        # print("renaming done")
  
    def run(self, recon_type):
        logging.info(f"Performing recon of type: {recon_type}")
        if recon_type == "pk_standard":
            self.write_recon_with_pk_results()
        elif recon_type == "soft_pk_and_cleanup":
            self.write_recon_with_soft_pk_and_cleanup_results()
        elif recon_type == "hierarchical_data":
            self.write_recon_hierarchical_data_results()
        if args.platform == "databricks":
            logging.info("Dropping temporary tables")
            drop_temp_tables(args.run_id)

    def write_recon_with_pk_results(self):
        """Performs PK-based recon and exports mismatches to disk."""

        # Step 1: Generate hashes for PK and non-PK columns
        for source in ["source1", "source2"]:
            add_hash_column(source, self.info[f'pk_{source}'], "pk_hash")
            add_hash_column(source, self.info[f'non_pk_{source}'], "non_pk_hash")

        # Step 2: Find exclusive records between the two tables
        create_exclusive_diff_tables(self.info)

        # Step 3: Find column-level mismatches for matching PKs with differing values
        create_column_mismatches_table(self.info)

        # Step 4: Write recon outputs to disk
        for table in ["source1_minus_source2", "source2_minus_source1", "column_mismatches"]:
            copy_table_disk(table, self.file_write_path, self.recon_scenario, self.info["mapping_df"])
        if args.platform == "databricks":
            full_file_path = os.path.join(self.file_write_path, 'recon_output.xlsx')
            dbutils.fs.cp(f"dbfs:/tmp/{args.run_id}/recon_output.xlsx", full_file_path)
            # logging.info(f"{source} written to {full_file_path}")
            # print(f"{source} written to {full_file_path}")

            ##put mapping_df in config.py later

    def write_recon_with_soft_pk_and_cleanup_results(self):
        """Performs recon using soft PK logic and cleanses columns."""

        # Step 1: Cleanse string columns of special characters
        for source in ["source1", "source2"]:
            cleanse_columns(source, self.info[f"cols_{source}"])

        # Step 2: Add placeholder 'matched' columns for tracking
        for source in ["source1", "source2"]:
            add_column(source, "matched")

        # Step 3: Generate full row hash for match detection
        for source in ["source1", "source2"]:
            add_hash_column(source, self.info[f"cols_{source}"], "all_cols_hash")

        # Step 4: Apply logic to compare rows and tag matches
        match_using_soft_pk(self.info)

        # Step 5: Write all outputs to disk
        for table in [
            "source1_matches", "source2_matches",
            "source1_minus_source2", "source2_minus_source1",
            "source1", "source2"
        ]:
            copy_table_disk(table, self.file_write_path)
                        
            if args.platform == "databricks":
                full_file_path = os.path.join(self.file_write_path, 'recon_output.xlsx')
                local_tmp_path = f"/tmp/recon_output_{session_guid}.xlsx"

                # dbutils.fs.cp(f"dbfs:/tmp/recon_output_{args.run_id}.xlsx", full_file_path)
                dbutils.fs.cp(f"file:{local_tmp_path}", f"dbfs:/tmp/recon_output_{session_guid}.xlsx")
                dbutils.fs.cp(f"dbfs:/tmp/recon_output_{session_guid}.xlsx", full_file_path)
            
                # logging.info(f"{source} written to {full_file_path}")
                # print(f"{source} written to {full_file_path}")

    # def write_recon_soft_pk_fuzzy_data_results(self):
    #     """Performs recon on data with soft pks using hierarchical fuzzy logic."""

    #     # Step 1: Clean and deduplicate both tables
    #     for source in ["source1", "source2"]:
    #         # dedup_table(source, self.info[f'pk_{source}'])
    #         cleanse_columns(source, self.info[f'pk_{source}'])
    #         add_hash_column(source, self.info[f'pk_{source}'], "pk_hash")
    #         if len(self.info[f'non_pk_{source}']) > 0:
    #             add_hash_column(source, self.info[f'non_pk_{source}'], "non_pk_hash")
    #         else:
    #             #add non_pk_hash column so that the fuzzy match function can be called
    #             # even if there are no non_pk columns
    #             # in the source table
    #             add_column(source, "non_pk_hash", "INT",-1)

    #     # Step 2: Generate row numbers for both datasets
    #     assign_row_numbers(self.info)

    #     # Step 3: Find exact hash matches and tag as 'absolute' matches
    #     tag_exact_row_matches()

    #     # Step 4: Propagate last matched row number for remaining unmatched rows
    #     tag_last_matched_row_number()

    #     # Step 5: Generate probable fuzzy matches using Jaro-Winkler & Levenshtein
    #     run_fuzzy_matching(self.info)

    #     # Step 6: Tag match types and update original datasets
    #     update_fuzzy_match_types(self.info)

    #     # Step 7: Write output to disk
    #     for table in [
    #         "source1_matches", "source2_matches",
    #         "probable_match",
    #         "source1_minus_source2", "source2_minus_source1",
    #         "source1", "source2"
    #     ]:
    #         copy_table_disk(table, self.file_write_path)


    def write_recon_hierarchical_data_results(self):
        """Performs recon on hierarchical data using fuzzy logic."""
        # Step 1: Clean and deduplicate both tables
        for source in ["source1", "source2"]:
            dedup_table(source, self.info[f'pk_{source}'])
            cleanse_columns(source, self.info[f'cols_{source}'])
            add_hash_column(source, self.info[f'pk_{source}'], "pk_hash")
            if len(self.info[f'non_pk_{source}']) > 0:
                add_hash_column(source, self.info[f'non_pk_{source}'], "non_pk_hash")
            else:
                #add non_pk_hash column so that the fuzzy match function can be called
                # even if there are no non_pk columns
                # in the source table
                add_column(source, "non_pk_hash", "INT")
        # Step 2: Generate row numbers for both datasets
        assign_row_numbers(self.info)
        # Step 3: Find exact hash matches and tag as 'absolute' matches
        tag_exact_row_matches()
        # Step 4: Propagate last matched row number for remaining unmatched rows
        tag_last_matched_row_number()
        # Step 5: Generate probable fuzzy matches using Jaro-Winkler & Levenshtein
        run_fuzzy_matching(self.info)
        # Step 6: Tag match types and update original datasets
        update_fuzzy_match_types(self.info)
        # Step 7: Write output to disk
        for table in [
            "source1_matches", "source2_matches",
            "probable_match",
            "source1_minus_source2", "source2_minus_source1",
            "source1", "source2"
        ]:
            copy_table_disk(table, self.file_write_path)

        full_file_path = os.path.join(self.file_write_path, 'recon_output.xlsx')

        if args.platform == "databricks":
            #do an extra copy to the recon_output file from temp location
            dbutils.fs.cp(f"dbfs:/tmp/{args.run_id}/recon_output.xlsx", full_file_path)

        # logging.info(f"{source} written to {full_file_path}")
        # print(f"{source} written to {full_file_path}")



