import uuid, os, json
import shutil, logging

def prepare_output_directory(base_path,run_id):
    """Generates a unique output directory under the given base path."""
    # run_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    output_dir = os.path.join(base_path, "run_output", run_id)
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

def archive_input_files(output_dir, *files):
    os.makedirs(output_dir, exist_ok=True)
    for path, new_name in files:
        shutil.copy(path, os.path.join(output_dir, new_name))

def setup_logging(output_dir):
    os.makedirs(output_dir, exist_ok=True)
    log_path = os.path.join(output_dir, "recon.log")

    logging.basicConfig(
        filename=log_path,
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)

def load_json_config(path):
    with open(path, 'r') as f:
        return json.load(f)



