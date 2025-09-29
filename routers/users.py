from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from database import supabase

router = APIRouter(
    tags=["users"]
) 


@router.post("/create-user")
async def clerk_webhook(webhook_data: dict):
    try:
        # Get the event type and user data from Clerk
        event_type = webhook_data.get("type")

        if event_type == "user.created":
            user_data = webhook_data.get("data", {})
            clerk_id = user_data.get("id")

        if not clerk_id:
            raise HTTPException(status_code=400, detail="No user ID in webhook")

        # Create user in our database
        result = supabase.table('users').insert({
            "clerk_id": clerk_id
        }).execute()

        return {
                "message": "User created successfully",
                "data": result.data[0]
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Webhook processing failed: {str(e)}")
