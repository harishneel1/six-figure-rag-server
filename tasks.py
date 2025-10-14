from celery import Celery
from database import supabase
import time


# Create Celery app
celery_app = Celery(
    'document_processor', #Name of our Celery app
    broker="redis://localhost:6379/0", # Where tasks are queued
    backend="redis://localhost:6379/0" # Where results are stored 
)

@celery_app.task
def process_document(document_id: str):
    """
    Simple test task
    """

    # step 1: Update status to "processing"
    supabase.table("project_documents").update({
        "processing_status": "processing"
    }).eq("id", document_id).execute()

    print(f"Processing document {document_id}")

    # step 2: Simulate actual work (partitioning, chunking, etc)
    time.sleep(5) # In real implementation, this is where we will process the document 

    # step 3: update status to "completed"
    supabase.table("project_documents").update({
        "processing_status": "completed"
    }).eq("id", document_id).execute()

    print(f"Celery task completed for document: {document_id}")

    return {
        "status": "success", 
        "document_id": document_id
    }
   
