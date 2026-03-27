from pyslurmutils.client.rest.base import SlurmBaseRestClient
# --- Configuration ---
SLURM_URL = ""
USER = ""
TOKEN = ""

client = SlurmBaseRestClient(url=SLURM_URL, user_name=USER, token=TOKEN)

# --- Target job name ---
target_name = "mvp_processing_test_corestats_24_1"

# --- Fetch all your jobs ---
jobs = client.get_all_job_properties(all_users=False)

# --- Filter jobs by name ---
matching_jobs = [job for job in jobs if job.name == target_name]

if not matching_jobs:
    print(f"No jobs found with name: {target_name!r}")
else:
    for job in matching_jobs:
        jid = job.job_id
        # Cancel the job
        client.cancel_job(jid)
        print(f"Cancelled job {jid} ({job.name})")