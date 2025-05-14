
def main():

    import json, os, logging, sys, argparse, uuid

    parser = argparse.ArgumentParser(description="Run data reconciliation job.")

    parser.add_argument("--platform", choices=["duckdb", "databricks", "duckdb_on_databricks"], default="duckdb", help="Execution platform")
    parser.add_argument("--source1_settings", required=True, help="Path to source1 JSON config")
    parser.add_argument("--source2_settings", required=True, help="Path to source2 JSON config")
    parser.add_argument("--recon_type", required=True, help="Recon type (e.g. pk_standard, hierarchical_data)")
    parser.add_argument("--path_name", required=True, help="Base path containing 'input/' and 'mapping.csv'")
    parser.add_argument("--run_mode", choices=["debug", "actual"], default="actual", help="pass debug to skip checks")

    args_input = parser.parse_args()

    if args_input.platform == "duckdb":
        PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        if PROJECT_ROOT not in sys.path:
            # print(f"Adding {PROJECT_ROOT} to sys.path")
            sys.path.insert(0, PROJECT_ROOT)

        from src.duckdb_io_utils import setup_logging, load_json_config, archive_input_files
        
    if args_input.platform == "databricks":
        PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
        if PROJECT_ROOT not in sys.path:
            # print(f"Adding {PROJECT_ROOT} to sys.path")
            sys.path.insert(0, PROJECT_ROOT)

        from src.databricks_io_utils import setup_logging, load_json_config, archive_input_files

    if args_input.platform == "duckdb_on_databricks":
        PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
        if PROJECT_ROOT not in sys.path:
            # print(f"Adding {PROJECT_ROOT} to sys.path")
            sys.path.insert(0, PROJECT_ROOT)

        from src.databricks_io_utils import setup_logging, load_json_config, archive_input_files

    
    from src.config import args

    args.platform = args_input.platform
    args.source1_settings = args_input.source1_settings
    args.source2_settings = args_input.source2_settings
    args.recon_type = args_input.recon_type
    args.path_name = args_input.path_name
    args.run_mode = args_input.run_mode

    args.run_id = str(uuid.uuid4()).replace("-", "_")
    # print(f"args.run_id: {args.run_id}")
    
    from src.recon import Recon
    from src.recon_utils import validate_settings

    # Load settings from the two JSON files
    settings1 = load_json_config(args_input.source1_settings)
    settings2 = load_json_config(args_input.source2_settings)

    if args_input.run_mode == "debug":
        logging.info("Debug mode: Skipping validation checks.")
    else:
    # After loading the JSON configs
        validate_settings(settings1, ["file_path", "source_type"], "source1") #add logic to check if filepaths are valid
        validate_settings(settings2, ["file_path", "source_type"], "source2")
    
    recon = Recon(
        path_name=args_input.path_name,
        source_type_1=settings1.get("source_type", "local_csv"),
        settings1=settings1,
        source_type_2=settings2.get("source_type", "local_csv"),
        settings2=settings2
    )
    print("recon.file_write_path", recon.file_write_path)
    # Set up logging
    setup_logging(recon.file_write_path)

    recon.setup()
    recon.run(args_input.recon_type)

    mapping_path = os.path.join(args_input.path_name, "input", "mapping.csv")
    
    from src.config import args

    if args.mismatches_found:
    #archive files
        archive_input_files(
        recon.file_write_path,
        (args_input.source1_settings, "settings1.json"),
        (args_input.source2_settings, "settings2.json"),
        (mapping_path, "mapping.csv"))

        # full_file_path = os.path.join(args.path_name,args.run_id)
        # print(f"Mismatches found during reconciliation and logged to {full_file_path}")
        # logging.info(f"Mismatches found during reconciliation and logged to {full_file_path}")

    else:
        print("No mismatches found during reconciliation!")
        logging.info("No mismatches found during reconciliation!")

    return recon  # Optional: return for additional inspection/debugging


if __name__ == "__main__":
    main()
