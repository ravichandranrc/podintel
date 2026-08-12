from pathlib import Path

from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def format_duration(seconds: float | int | None) -> str:
    if not seconds:
        return "—"
    seconds = int(seconds)
    minutes, secs = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m {secs}s"


def format_timestamp(seconds: float | int | None) -> str:
    if seconds is None:
        return "0:00"
    seconds = int(seconds)
    minutes, secs = divmod(seconds, 60)
    return f"{minutes}:{secs:02d}"


templates.env.filters["duration"] = format_duration
templates.env.filters["timestamp"] = format_timestamp
