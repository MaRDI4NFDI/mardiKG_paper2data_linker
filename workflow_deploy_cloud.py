# For execution on a Prefect server the secrets have to be set - see README.MD for details.

# Run this for CLOUD execution:
#   prefect cloud login

# To add a schedule:
#   * Go to "Deployments"
#   * Click on the workflow name
#   * Click on "+ Schedule" (top right corner)

from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/MaRDI4NFDI/mardiKG_paper2data_linker.git",
        entrypoint="workflow_main.py:process_datasets",
    ).deploy(
        name="paper2data_linker",
        work_pool_name="CloudWorkerPool",
        parameters={
            "lakefs_url": "https://lake-bioinfmed.zib.de",
            "lakefs_repo": "mardi-workflows-files",
            "lakefs_path": "mardiKG_paper2data_linker/",
            "last_index_to_crawl": 968
        },
        job_variables={"pip_packages": [
            "boto3",
            "botocore",
            "crawl4ai==0.4.248",
            "ijson",
            "lakefs-sdk",
            "minio",
            "git+https://github.com/MaRDI4NFDI/mardiclient.git",
            "git+https://github.com/MaRDI4NFDI/mardiportal-workflowtools.git"
        ]},
    )



