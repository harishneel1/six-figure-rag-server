from celery import Celery
from database import BUCKET_NAME, s3_client, supabase
import time
from unstructured.partition.pdf import partition_pdf
from unstructured.partition.docx import partition_docx
from unstructured.partition.html import partition_html
from unstructured.chunking.title import chunk_by_title
import os



# Create Celery app
celery_app = Celery(
    'document_processor', #Name of our Celery app
    broker="redis://localhost:6379/0", # Where tasks are queued
    backend="redis://localhost:6379/0" # Where results are stored 
)


def update_status(document_id: str, status: str, details: dict = None):
    """ Update document processing status with optional details """

    # Get current document 
    result = supabase.table("project_documents").select("processing_details").eq("id", document_id).execute()

    # Start with existing details or empty dict
    current_details = {}

    if result.data and result.data[0]["processing_details"]:
        current_details = result.data[0]["processing_details"]

    
    # Add new details if provided
    if details: 
        current_details.update(details)
    

    # Update document 
    supabase.table("project_documents").update({
        "processing_status": status, 
        "processing_details": current_details
    }).eq("id", document_id).execute()

@celery_app.task
def process_document(document_id: str):
    """
        Real document Processing
    """

    try: 

        doc_result = supabase.table("project_documents").select("*").eq("id", document_id).execute()
        document = doc_result.data[0]

        # step 1: Download and partition 
        update_status(document_id, "partitioning")
        elements = download_and_partition(document_id, document)


        # step 2: Chunk elements 
        chunks, chunking_metrics = chunk_elements(elements)
        update_status(document_id, "summarising", {
            "chunking": chunking_metrics
        })

        #3 Step 3: Summarising chunks 

        #4 Step 4: Vectorization & storing 
        
    

        return {
            "status": "success", 
            "document_id": document_id
        }

    except Exception as e: 
        pass
   

def download_and_partition(document_id: str, document: dict):
    """ Download document from S3 / Crawl URL and partition into elements  """
    
    print(f"Downloading and partitioning document {document_id}")

    source_type = document.get("source_type", "file")

    if source_type == "url":
        # Crawl URL 
        pass

    else:
        # Handle file processing
        
        s3_key = document["s3_key"]
        filename = document["filename"]
        file_type = filename.split(".")[-1].lower()

        #  Download to a temporary location 
        temp_file = f"/tmp/{document_id}.{file_type}"
        s3_client.download_file(BUCKET_NAME, s3_key, temp_file)

        elements = partition_document(temp_file, file_type, source_type="file")


    elements_summary = analyze_elements(elements)

    update_status(document_id, "chunking", {
        "partitioning": {
            "elements_found": elements_summary
        }
    })
    os.remove(temp_file)

    return elements



def partition_document(temp_file: str, file_type: str, source_type: str = "file"):
    """ Partition document based on file type and source type """

    if source_type == "url": 
        pass

    if file_type == "pdf":
        return partition_pdf(
            filename=temp_file,  # Path to your PDF file
            strategy="hi_res", # Use the most accurate (but slower) processing method of extraction
            infer_table_structure=True, # Keep tables as structured HTML, not jumbled text
            extract_image_block_types=["Image"], # Grab images found in the PDF
            extract_image_block_to_payload=True # Store images as base64 data you can actually use
        )



def analyze_elements(elements):
    """ Count different types of elements found in the document """

    text_count = 0
    table_count = 0
    image_count = 0
    title_count = 0
    other_count = 0

    # Go through each element and count what type it is 
    for element in elements: 
        element_name = type(element).__name__ #Get the class name like "Table" or "NarrativeText"

        if element_name == "Table":
            table_count += 1
        elif element_name == "Image": 
            image_count += 1
        elif element_name in ["Title", "Header"]:
            title_count += 1
        elif element_name in ["NarrativeText", "Text", "ListItem", "FigureCaption"]:
            text_count += 1
        else:
            other_count += 1

    # Return a simple dictionary
    return {
        "text": text_count,
        "tables": table_count,
        "images": image_count,
        "titles": title_count,
        "other": other_count
    }


def chunk_elements(elements):
    """ Chunk elements using title-based strategy and collect metrics """

    print("🔨 Creating smart chunks...")
    
    chunks = chunk_by_title(
        elements, # The parsed PDF elements from previous step
        max_characters=3000, # Hard limit - never exceed 3000 characters per chunk
        new_after_n_chars=2400, # Try to start a new chunk after 2400 characters
        combine_text_under_n_chars=500 # Merge tiny chunks under 500 chars with neighbors
    )

    # Collect chunking metrics 
    total_chunks = len(chunks)

    chunking_metrics = {
        "total_chunks": total_chunks
    }

    print(f"✅ Created {total_chunks} chunks from {len(elements)} elements")

    return chunks, chunking_metrics