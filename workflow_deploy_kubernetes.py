# For execution on a Prefect server the secrets have to be set - see README.MD for details.

# Run this for CLOUD execution:
#   prefect cloud login

# To add a schedule:
#   * Go to "Deployments"
#   * Click on the workflow name
#   * Click on "+ Schedule" (top right corner)

# Run this for LOCAL execution:
#   prefect config unset PREFECT_API_URL
#   prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
#   prefect server start

from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/MaRDI4NFDI/mardiKG_paper2data_linker.git",
        entrypoint="workflow_main.py:process_datasets",
    ).deploy(
        name="paper2data_linker",
        work_pool_name="K8WorkerPool",
        parameters={
            "lakefs_url": "https://lake-bioinfmed.zib.de",
            "lakefs_repo": "mardi-workflows-files",
            "lakefs_path": "mardiKG_paper2data_linker/"
        },
        job_variables={
            "image": "ghcr.io/mardi4nfdi/mardikg_paper2data_linker:latest",
        },
    )



