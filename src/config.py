    # args.platform = args_input.platform
    # args.source1_settings = args_input.source1_settings
    # args.source2_settings = args_input.source2_settings
    # args.recon_type = args_input.recon_type
    # args.path_name = args_input.path_name
    # args.run_mode = args_input.run_mode


class Args:

    def __init__(self):
        self.platform = ""
        self.source1_settings = ""
        self.source2_settings = ""
        self.recon_type = ""
        self.path_name = ""
        self.run_mode = ""
        self.run_id = ""


args = Args()
# Base path for project - customize if needed
# BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# print(f"Base path set to: {BASE_PATH}")

# print(PROJECT_ROOT)