
def run_recon_notebook(platform, source1_settings, source2_settings, recon_type, path_name, run_mode="actual"):
    """
    Notebook-friendly wrapper to run the reconciliation job without using CLI.
    Keeps in-memory context for debugging.
    """
    
    import os,sys,logging, uuid
    
    if platform == "duckdb":
        PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        if PROJECT_ROOT not in sys.path:
            print(f"Adding {PROJECT_ROOT} to sys.path")
            sys.path.insert(0, PROJECT_ROOT)

        from src.duckdb_io_utils import setup_logging, load_json_config, archive_input_files
    if platform == "databricks":
        PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
        if PROJECT_ROOT not in sys.path:
            print(f"Adding {PROJECT_ROOT} to sys.path")
            sys.path.insert(0, PROJECT_ROOT)

        from src.databricks_io_utils import setup_logging, load_json_config, archive_input_files
    
    from src.config import args
    
    args.platform = platform
    args.source1_settings = source1_settings
    args.source2_settings = source2_settings
    args.recon_type = recon_type
    args.path_name = path_name
    args.run_mode = run_mode

    args.run_id = str(uuid.uuid4()).replace("-", "_")
    print(f"args.run_id: {args.run_id}")
    
    from src.recon import Recon
    from src.recon_utils import validate_settings

    # Load settings from the two JSON files
    settings1 = load_json_config(source1_settings)
    settings2 = load_json_config(source2_settings)

    if run_mode == "debug":
        logging.info("Debug mode: Skipping validation checks.")
    else:
    # After loading the JSON configs
        validate_settings(settings1, ["file_path", "source_type"], "source1") #add logic to check if filepaths are valid
        validate_settings(settings2, ["file_path", "source_type"], "source2")
    
    recon = Recon(
        path_name=path_name,
        source_type_1=settings1.get("source_type", "local_csv"),
        settings1=settings1,
        source_type_2=settings2.get("source_type", "local_csv"),
        settings2=settings2
    )
    print("recon.file_write_path", recon.file_write_path)
    # Set up logging
    setup_logging(recon.file_write_path)
 
    recon.setup()
    recon.run(recon_type)

    mapping_path = os.path.join(path_name, "input", "mapping.csv")
    
    #archive files
    archive_input_files(
    recon.file_write_path,
    (source1_settings, "settings1.json"),
    (source2_settings, "settings2.json"),
    (mapping_path, "mapping.csv")
)

    return recon  # Optional: return for additional inspection/debugging

if __name__ == "__main__":
    # # Example usage:
    recon = run_recon_notebook(
        platform="duckdb",
        source1_settings="input/settings1.json",
        source2_settings="input/settings2.json",
        recon_type="hierarchical_data",
        path_name="/Users/souravagasti/Downloads/recon-project"

    )

    # Example usage 2:
    # recon = run_recon_notebook(
    #     platform="databricks",
    #     source1_settings="abfss://org1@souravagastiadls.dfs.core.windows.net/ext_cat/vol/recon-project/input/settings1.json",
    #     source2_settings="abfss://org1@souravagastiadls.dfs.core.windows.net/ext_cat/vol/recon-project/input/settings2.json",
    #     recon_type="hierarchical_data",
    #     path_name="abfss://org1@souravagastiadls.dfs.core.windows.net/ext_cat/vol/recon-project/",
    #     run_mode="debug"
    # )


