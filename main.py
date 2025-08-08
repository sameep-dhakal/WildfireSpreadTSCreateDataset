import ee
import yaml
import tqdm
import os
import time

from DataPreparation.DatasetPrepareService import DatasetPrepareService

# === Configuration ===
KEY_FILE = '/Users/sameeps/Documents/Dr-Malof/WIldfire/WildfireSpreadTSCreateDataset/abc.json'
SERVICE_ACCOUNT = 'yourname@serviceaccount.iam.gserviceaccount.com'
CONFIG_FOLDER = "config"
N_BUFFER_DAYS = 4
MAX_QUEUE = 2900
QUEUE_CHECK_INTERVAL = 60  # in seconds

# === GEE Auth ===
credentials = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, KEY_FILE)
ee.Initialize(credentials)

def wait_for_available_task_slot():
    """Wait until Earth Engine task queue has room."""
    while True:
        task_list = ee.data.getTaskList()
        running_or_ready = [t for t in task_list if t['state'] in ('READY', 'RUNNING')]
        if len(running_or_ready) < MAX_QUEUE:
            return
        print(f"🕒 Task queue full ({len(running_or_ready)} tasks). Waiting {QUEUE_CHECK_INTERVAL}s...")
        time.sleep(QUEUE_CHECK_INTERVAL)

def run_fire_task(location, config):
    """Submit GEE task for one fire, retrying if necessary."""
    while True:
        wait_for_available_task_slot()
        dataset_pre = DatasetPrepareService(location=location, config=config)
        try:
            dataset_pre.extract_dataset_from_gee_to_gcloud('32610', n_buffer_days=N_BUFFER_DAYS)
            dataset_pre.download_data_from_gcloud_to_local()
            return  # Success
        except Exception as e:
            print(f"❌ Retrying {location} due to error:\n{e}")
            time.sleep(5)

# === Main loop ===
if __name__ == '__main__':
    all_failed_locations = []

    config_files = sorted([
        f for f in os.listdir(CONFIG_FOLDER)
        if f.endswith(".yml") or f.endswith(".yaml")
    ])

    for config_file in config_files:
        config_path = os.path.join(CONFIG_FOLDER, config_file)
        print(f"\n🚀 Processing config file: {config_path}")

        with open(config_path, "r", encoding="utf8") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)

        fire_names = list(config.keys())
        for skip_key in ["output_bucket", "rectangular_size", "year"]:
            fire_names = [f for f in fire_names if f != skip_key]

        for fire_id in tqdm.tqdm(fire_names, desc=f"🔥 Fires in {config_file}"):
            print(f"\n🔁 Starting: {fire_id}")
            run_fire_task(fire_id, config)

        print(f"✅ Finished all fires in {config_file}")

    print("\n🎉 All config files processed successfully.")
