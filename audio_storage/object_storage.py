"""Module 2: Audio Storage — S3-compatible object storage (MinIO for local dev).

DESIGN.md §4: audio never flows through the app server. This module only ever
does two things: PUT bytes fetched from the source URL, and GET a signed URL
for playback — the actual audio bytes never pass through Python on the read side.
"""

import aioboto3
import boto3

from common.config import get_settings

settings = get_settings()


def storage_key_for(podcast_id: int, episode_id: int) -> str:
    return f"podcasts/{podcast_id}/episodes/{episode_id}/audio.mp3"


async def upload_audio(storage_key: str, content: bytes, content_type: str = "audio/mpeg") -> None:
    session = aioboto3.Session()
    async with session.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
    ) as s3:
        await s3.put_object(
            Bucket=settings.s3_bucket,
            Key=storage_key,
            Body=content,
            ContentType=content_type,
        )


async def download_audio(storage_key: str) -> bytes:
    """Used by Module 3 (Transcription) to read back what Module 2 stored —
    transcription depends on Module 2's storage abstraction, not the original
    source URL, so a re-transcribe never re-hits the podcast's own server.
    """
    session = aioboto3.Session()
    async with session.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
    ) as s3:
        obj = await s3.get_object(Bucket=settings.s3_bucket, Key=storage_key)
        return await obj["Body"].read()


def signed_playback_url(storage_key: str, expires_in: int = 3600) -> str:
    """Presigned URL generation is a local signing operation, not a network call —
    safe to do with the sync boto3 client even from an async request handler.
    """
    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_public_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
    )
    return client.generate_presigned_url(
        "get_object",
        Params={"Bucket": settings.s3_bucket, "Key": storage_key},
        ExpiresIn=expires_in,
    )
