import json, requests, re
from django.conf import settings


class AIMicroservice:
    def __init__(self):
        self.url = settings.AI_SERVICE_URL
        self.api_key = settings.AI_SERVICE_KEY
        # self.open_ai_key = os.getenv("OPEN_AI_KEY")

    def get_textgen_response(self,prompt):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-api-key": self.api_key,
        }
        textgen_payload = {
            "service": "textgeneration",
            "provider_name": "openai",
            "model": "gpt-4o-mini",
            "payload": {
                "prompt": prompt,
                "max_completion_tokens": 500,
                "temperature": 0.7,
            },
        } 
        response = requests.post(self.url, headers=headers, data=json.dumps(textgen_payload))
        if response.status_code == 200:
            textgen = response.json()
            str_obj = textgen["result"].strip()
            if str_obj:  # make sure it's not empty
                try:
                    cleaned = re.sub(r"^```.*\n|```$", "", str_obj)
                    data = json.loads(cleaned)
            
                except json.JSONDecodeError:
                    print("AI response is not valid JSON:", str_obj)
                    data = {}
            else:
                print("AI returned an empty response")
                data = {}
            usage_obj = textgen["usage"]
        else:
            return {"Error": response.text}
        return {"data":data, "usage":usage_obj}
    
    def get_sst_response(self, audio_b64):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-api-key": self.api_key,
        }
        stt_payload = {
            "service": "stt",
            "provider_name": "openai",
            "model": "whisper-1",
            "payload": {
                "audio_base64": audio_b64,
                "language": "en",
                "format": "mp3"
            },
        }

        try:
            stt_response = requests.post(self.url, headers=headers, json=stt_payload)
            stt_data = stt_response.json()
        except Exception as e:
            return {"error": f"STT request failed: {str(e)}"}

        if stt_response.status_code != 200 or "result" not in stt_data:
            return {"error": "Failed to transcribe audio", "details": stt_data}
        
        transcribed_text = stt_data["text"]
        if not transcribed_text:
            return {"error": "STT response missing text", "details": stt_data}

        return transcribed_text