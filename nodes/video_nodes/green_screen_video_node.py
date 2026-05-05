import os
import uuid
import requests
from ..common import (
    bria_json_headers,
    poll_status_until_completed,
)
from .video_utils import upload_video_to_s3


class GreenScreenVideoNode():
    """
    Applies green-screen (chroma key) background removal using the Bria API
    (POST /v2/video/edit/green_screen). Output is a processed video with a solid-color background.
    """
    @classmethod
    def INPUT_TYPES(self):
        return {
            "required": {
                "api_key": ("STRING", {"default": "BRIA_API_TOKEN"}),
                "video_url": ("STRING", {
                    "default": "",
                    "tooltip": "Local path or publicly accessible URL of the video to process.",
                }),
            },
            "optional": {
                "green_shade": ([
                    "broadcast_green",
                    "chroma_green",
                    "blue_screen",
                ], {"default": "broadcast_green"}),
                "output_container_and_codec": ([
                    "mp4_h264",
                    "mp4_h265",
                    "webm_vp9",
                    "mov_h265",
                    "mov_proresks",
                    "mkv_h264",
                    "mkv_h265",
                    "mkv_vp9",
                    "gif"
                ], {"default": "mp4_h264"}),
                "preserve_audio": ("BOOLEAN", {"default": True}),
            }
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("result_video_url",)
    CATEGORY = "API Nodes"
    FUNCTION = "execute"

    def __init__(self):
        self.api_url = "https://engine.prod.bria-api.com/v2/video/edit/green_screen"

    def execute(
        self,
        api_key,
        video_url,
        green_shade="broadcast_green",
        output_container_and_codec="mp4_h264",
        preserve_audio=True,
    ):
        if api_key.strip() == "" or api_key.strip() == "BRIA_API_TOKEN":
            raise Exception("Please insert a valid API key.")

        if not video_url or not str(video_url).strip():
            raise Exception("video_url is required: provide a local path or a publicly accessible video URL.")

        if os.path.exists(video_url):
            filename = f"{str(uuid.uuid4())}_{os.path.basename(video_url)}"
            input_video_url = upload_video_to_s3(video_url, filename, api_key)
            if not input_video_url or not (
                input_video_url.startswith("http://") or input_video_url.startswith("https://")
            ):
                raise Exception(f"Failed to upload video to S3. Got: {input_video_url}")
        else:
            input_video_url = video_url.strip()

        try:
            print("Calling Bria API for video green screen...")
            payload = {
                "video": input_video_url,
                "green_shade": green_shade,
                "output_container_and_codec": output_container_and_codec,
                "preserve_audio": preserve_audio,
            }
            headers = bria_json_headers(api_key)


            response = requests.post(self.api_url, json=payload, headers=headers)

            if response.status_code == 200 or response.status_code == 202:
                print("Initial video green-screen request accepted, polling for completion...")
                response_dict = response.json()

                status_url = response_dict.get("status_url")
                request_id = response_dict.get("request_id")

                if not status_url:
                    raise Exception("No status_url returned from API")

                print(f"Request ID: {request_id}, Status URL: {status_url}")

                final_response = poll_status_until_completed(
                    status_url, api_key, timeout=3600, check_interval=5
                )

                result_video_url = final_response["result"]["video_url"]

                print(f"Video processing completed. Result URL: {result_video_url}")
                return (result_video_url,)

            raise Exception(f"Error: API request failed with status code {response.status_code} {response.text}")

        except Exception as e:
            raise Exception(f"{e}")