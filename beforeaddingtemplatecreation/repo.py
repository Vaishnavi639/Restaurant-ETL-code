# repository.py
import os
from datetime import datetime
from dagster import (
    repository, FilesystemIOManager, multiprocess_executor, 
    ScheduleDefinition, schedule, ScheduleEvaluationContext, RunRequest,
    RunsFilter, DagsterRunStatus
)
from etl_jobs.onboarding_jobs import user_onboarding_graph
from etl_jobs.mdb_jobs import mdb_etl_graph
from etl_jobs.incremental_jobs import incremental_update_graph
from etl_jobs.poll_meta_verification_flow_jobs import poll_meta_verification_flow_create_graph
from etl_jobs.image_scraping_jobs import image_processing_job
from ops.image_scraping import get_total_available_jobs_and_allocations
from resources.api_client import api_client_resource
import debugpy
from dotenv import load_dotenv

load_dotenv()

# Import restaurant graphs
from etl_jobs.restaurant_menu_pdf_jobs import (
    restaurant_production_etl,
    test_local_full_etl
)

if os.getenv("DEBUG_MODE") == "true":
    if not debugpy.is_client_connected():
        debugpy.listen(("0.0.0.0", 5678))
        print("🛠 Debugger is waiting for client to attach...")
        debugpy.wait_for_client()

backend_host_url = os.getenv("BACKEND_HOST_URL")
business_url = f"{backend_host_url}"

# Existing jobs
user_onboarding_job = user_onboarding_graph.to_job(
    name="user_onboarding_job",
    executor_def=multiprocess_executor,
    resource_defs={
        "io_manager": FilesystemIOManager(base_dir="/tmp/io_manager_storage"),
        "api_client": api_client_resource.configured({"base_url": business_url})
    },
    tags={"dagster/priority": "10"} 
)

mdb_etl_job = mdb_etl_graph.to_job(
    name="mdb_etl_job",
    executor_def=multiprocess_executor,
    resource_defs={"io_manager": FilesystemIOManager(base_dir="/tmp/io_manager_storage")},
    tags={"dagster/priority": "5"} 
)

incremental_json_update_job = incremental_update_graph.to_job(
    name="incremental_json_update_job",
    executor_def=multiprocess_executor,
    resource_defs={"io_manager": FilesystemIOManager(base_dir="/tmp/io_manager_storage")},
    tags={"dagster/priority": "10"} 
)

poll_meta_verification_flow_create_job = poll_meta_verification_flow_create_graph.to_job(
    name="poll_meta_verification_flow_create_job",
    executor_def=multiprocess_executor,
    resource_defs={"io_manager": FilesystemIOManager(base_dir="/tmp/io_manager_storage")},
    tags={"dagster/priority": "3"} 
)

@schedule(
    job=image_processing_job,
    cron_schedule="*/25 * * * *",
    name="smart_image_processing_schedule",
    description="Smart schedule that allocates products by business and creates jobs"
)
def smart_image_processing_schedule(context: ScheduleEvaluationContext):
    current_time = datetime.now()
    schedule_run_id = f"img_schedule_{current_time.strftime('%Y%m%d_%H%M%S')}"
    
    runs_filter = RunsFilter(
        job_name="image_processing_job",
        statuses=[DagsterRunStatus.QUEUED, DagsterRunStatus.STARTED, DagsterRunStatus.STARTING]
    )
    
    running_jobs = context.instance.get_runs(filters=runs_filter)
    running_count = len(running_jobs)
    max_concurrent_jobs = 10
    
    context.log.info(f"Currently running jobs: {running_count}/{max_concurrent_jobs}")
    
    if running_count >= max_concurrent_jobs:
        context.log.info(f"At max capacity. Skipping.")
        return []
    
    available_slots = max_concurrent_jobs - running_count
    
    try:
        job_allocations = get_total_available_jobs_and_allocations(available_slots, logger=context.log)
    except Exception as e:
        context.log.error(f"Critical failure: {e}")
        raise
    
    if not job_allocations:
        return []
    
    run_requests = []
    for i, allocation in enumerate(job_allocations):
        try:
            business_info = allocation.get("business_info", {})
            products = allocation.get("products", [])
            job_number = allocation.get("job_number", i + 1)
            
            run_request = RunRequest(
                run_key=f"{schedule_run_id}_job_{job_number:02d}",
                run_config={
                    "ops": {
                        "set_products_processing": {
                            "config": {
                                "business_info": business_info,
                                "products": products,
                                "job_number": job_number
                            }
                        }
                    }
                },
                tags={
                    "schedule_run": schedule_run_id,
                    "job_number": str(job_number),
                    "business_name": business_info.get("businessName", "Unknown"),
                    "business_account_id": business_info.get("businessAccountId", ""),
                    "product_count": str(len(products))
                }
            )
            run_requests.append(run_request)
        except Exception as e:
            context.log.error(f"Failed to create run request: {e}")
    
    return run_requests

poll_meta_verification_schedule = ScheduleDefinition(
    job=poll_meta_verification_flow_create_job,
    cron_schedule="0 */6 * * *",
    name="poll_meta_verification_every_6_hours",
    description="Runs poll_meta_verification_flow_create_job every 6 hours",
)

# Restaurant jobs
restaurant_production_etl_job = restaurant_production_etl.to_job(
    name="restaurant_production_etl_job",
    executor_def=multiprocess_executor,
    resource_defs={"io_manager": FilesystemIOManager(base_dir="/tmp/io_manager_storage")},
    tags={"type": "restaurant_menu", "stage": "production"}
)



test_local_full_etl_job = test_local_full_etl.to_job(
    name="test_local_full_etl_job",
    executor_def=multiprocess_executor,
    resource_defs={"io_manager": FilesystemIOManager(base_dir="/tmp/io_manager_storage")},
    tags={"type": "restaurant_menu", "stage": "test"}
)

@repository
def deploy_docker_repository():
    return [
        user_onboarding_job,
        mdb_etl_job,
        incremental_json_update_job,
        poll_meta_verification_flow_create_job,
        image_processing_job,
        restaurant_production_etl_job,
        test_local_full_etl_job,
        poll_meta_verification_schedule,
        smart_image_processing_schedule,
    ]
