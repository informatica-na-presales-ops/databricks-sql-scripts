def human_duration(duration: int) -> str:
    if duration > 60:
        minutes = duration // 60
        seconds = duration % 60
        return f'{minutes}m{seconds}s'
    return f'{duration}s'
