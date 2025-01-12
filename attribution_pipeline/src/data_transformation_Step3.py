# Prepares and chunks data for the API.

def prepare_payload(data):
    payload = data.groupby('conv_id').apply(
        lambda x: x.to_dict(orient='records')
    ).to_dict()
    return payload

def chunk_data(data, chunk_size):
    keys = list(data.keys())
    for i in range(0, len(keys), chunk_size):
        yield {k: data[k] for k in keys[i:i + chunk_size]}
