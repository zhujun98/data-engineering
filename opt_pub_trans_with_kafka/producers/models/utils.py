

def normalize_station_name(name):
    return name.lower().replace(
        "/", "_and_").replace(
        " ", "_").replace(
        "-", "_").replace(
        "'", "")
