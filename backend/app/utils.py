def _get_days(model_id: str):
    l = model_id.split('_')
    return int(l[1]), int(l[2])