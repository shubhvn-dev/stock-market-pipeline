import google.generativeai as genai
import os
from dotenv import load_dotenv

load_dotenv()

def check_models():
    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key:
        print("GEMINI_API_KEY not found in environment variables")
        return
    
    try:
        genai.configure(api_key=api_key)
        
        print("Available Gemini models:")
        for model in genai.list_models():
            if 'generateContent' in model.supported_generation_methods:
                print(f"- {model.name}")
    
    except Exception as e:
        print(f"Error checking models: {e}")

if __name__ == "__main__":
    check_models()
