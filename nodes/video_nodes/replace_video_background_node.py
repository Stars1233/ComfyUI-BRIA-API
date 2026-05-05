import os
import uuid
import requests
from ..common import (
    bria_json_headers,
    normalize_images_input,
    poll_status_until_completed,
    upload_pil_image_to_temp
)
from .video_utils import upload_video_to_s3


class ReplaceVideoBackgroundNode():
    """
    Composites a new background (image or video URL, or an IMAGE from another node) behind the
    foreground video using the Bria API (POST /v2/video/edit/replace_background).

    When ``background_image`` is connected, only the first image is used (no batch); it is uploaded
    via the platform anonymous image presigned URL (same pattern as video) and the resulting
    ``https://temp.bria.ai/...`` URL is sent in ``background_url``.

    The background asset must match the foreground aspect ratio; otherwise the API may return
    BACKGROUND_ASPECT_RATIO_MISMATCH (surfaced with foreground and background aspect ratio values).
    """
    @classmethod
    def INPUT_TYPES(self):
        return {
            "required": {
                "api_key": ("STRING", {"default": "BRIA_API_TOKEN"}),
                "video_url": ("STRING", {
                    "default": "",
                    "tooltip": "Local path or publicly accessible URL of the foreground video.",
                }),
            },
            "optional": {
                "background_url": ("STRING", {
                    "default": "",
                    "tooltip": "Public HTTPS image or video URL, if not using background_image.",
                }),
                "background_image": ("IMAGE",),
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
        self.api_url = "https://engine.prod.bria-api.com/v2/video/edit/replace_background"

    @staticmethod
    def _background_image_to_temp_url(background_image, api_key):
        """First image only; upload to temp bucket (format from file_name extension; .png if none)."""
        if background_image is None:
            return None
        try:
            pil_images = normalize_images_input(background_image)
        except (ValueError, TypeError) as e:
            raise Exception(f"Invalid background_image: {e}") from e
        if not pil_images:
            raise Exception("background_image produced no images.")
        file_name = f"{uuid.uuid4()}_background"
        return upload_pil_image_to_temp(pil_images[0], api_key, file_name=file_name)

    def execute(
        self,
        api_key,
        video_url,
        background_url="",
        background_image=None,
        output_container_and_codec="mp4_h264",
        preserve_audio=True,
    ):
        if api_key.strip() == "" or api_key.strip() == "BRIA_API_TOKEN":
            raise Exception("Please insert a valid API key.")

        if not video_url or not str(video_url).strip():
            raise Exception("video_url is required: provide a local path or a publicly accessible video URL.")

        bg_from_image = self._background_image_to_temp_url(background_image, api_key)
        bg_from_url = str(background_url).strip() if background_url else ""

        if bg_from_image:
            bg = bg_from_image
        elif bg_from_url:
            bg = bg_from_url
        else:
            raise Exception(
                "Provide either background_image (IMAGE from Load Image, Generate Image, etc.) "
                "or a non-empty background_url (HTTPS image or video URL)."
            )

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
            print("Calling Bria API for video replace background...")
            payload = {
                "video": input_video_url,
                "background_url": bg,
                "output_container_and_codec": output_container_and_codec,
                "preserve_audio": preserve_audio,
            }

            headers = bria_json_headers(api_key)

            response = requests.post(self.api_url, json=payload, headers=headers)

            if response.status_code == 200 or response.status_code == 202:
                print("Initial video replace-background request accepted, polling for completion...")
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
