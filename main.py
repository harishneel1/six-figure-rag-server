from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os 
from routers import users
 
load_dotenv() 

# Create FastAPI app
app = FastAPI(
    title="Six-Figure AI Engineering API",
    description="Backend API for Six-Figure AI Engineering application",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(users.router) 


# Health check endpoints
@app.get("/")
async def root():
    return {"message": "Six-Figure AI Engineering app is running!"}

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "version": "1.0.0"
    }

@app.post("/create-user")
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



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)