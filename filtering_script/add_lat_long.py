from geopy.geocoders import Nominatim
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

# 1) Same geolocator, same timeout, no retries, silent failures
_geolocator = Nominatim(user_agent="longLatApp")

@lru_cache(maxsize=None)
def _geocode_cached(address: str):
    try:
        loc = _geolocator.geocode(address, timeout=10)
        if loc:
            return loc.latitude, loc.longitude
    except Exception:
        pass
    return None, None

def add_lat_long(df: pd.DataFrame, log, max_workers: int = 8) -> pd.DataFrame:
    # Build list of (index, address) for rows needing geocode
    to_process = []
    for idx, row in df[df["locationLat"].isna()].iterrows():
        if isinstance(row["locationAddress"], str) and " " in row["locationAddress"]:
            addr = row["locationAddress"]
        else:
            parts = [
                row.get(f) for f in
                ["locationNeighbourhood","locationDistrict","locationCity","locationCountry"]
                if row.get(f)
            ]
            addr = ", ".join(parts).replace("Located in","").strip()
        to_process.append((idx, addr))

    total = len(to_process)
    if total == 0:
        log.info("No new addresses to geocode.")
        return df

    # 2) Extract unique addresses
    unique_addrs = list({addr for _, addr in to_process})
    log.info(f"Geocoding {len(unique_addrs)} unique addresses (of {total} total rows) in parallel using {max_workers} threads.")

    # 3) Run geocoding in parallel
    geocoded = {}
    with ThreadPoolExecutor(max_workers=min(max_workers, len(unique_addrs))) as executor:
        future_to_addr = {executor.submit(_geocode_cached, addr): addr for addr in unique_addrs}
        for future in as_completed(future_to_addr):
            addr = future_to_addr[future]
            lat, lon = future.result()
            geocoded[addr] = (lat, lon)
            # **log each completion immediately**
            log.info(f"→ Thread finished: {addr!r} → lat={lat}, lon={lon}")

    # 4) Map results back onto the DataFrame, printing progress exactly as before
    for count, (idx, addr) in enumerate(to_process, start=1):
        lat, lon = geocoded.get(addr, (None, None))
        df.at[idx, "locationLat"] = lat
        df.at[idx, "locationLon"] = lon
        print(f"Processed {count}/{total} listings. Adding latitude/longitude: {lat}, {lon}")

    return df
