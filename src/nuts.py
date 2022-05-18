import re
from pathlib import Path

import datazimmer as dz
import geopandas as gpd
import h3pandas  # noqa: F401
import pandas as pd
import requests

nuts_api_root = dz.SourceUrl(
    "https://gisco-services.ec.europa.eu/distribution/v2/nuts/"
)


class HexagonHash(dz.AbstractEntity):
    hid = dz.Index & str
    level = int


class NutsRegion(dz.AbstractEntity):

    nid = dz.Index & str
    level = int
    name = str
    country_code = str


class NutsLocator(dz.AbstractEntity):

    region = NutsRegion
    hexagon = HexagonHash


nuts_table = dz.ScruTable(NutsRegion)
locator_table = dz.ScruTable(NutsLocator)
h3_table = dz.ScruTable(HexagonHash)


@dz.register_data_loader
def load_data():

    h3_level = 6

    resp = requests.get(nuts_api_root)
    gdfs = []
    tmp_zip_path = Path("tmp.shp.zip")
    years = set(re.compile(r"nuts-(\d{4})-files").findall(resp.text))
    for year in years:
        url = f"{nuts_api_root}shp/NUTS_RG_60M_{year}_4326.shp.zip"
        resp = requests.get(url)
        if not resp.ok:
            continue
        tmp_zip_path.write_bytes(resp.content)
        gdfs.append(gpd.read_file(tmp_zip_path.as_posix()).assign(year=year))

    tmp_zip_path.unlink()

    gdf = (
        pd.concat(gdfs)
        .rename(
            columns={
                "CNTR_CODE": NutsRegion.country_code,
                "NAME_LATN": NutsRegion.name,
                "LEVL_CODE": NutsRegion.level,
            }
        )
        .assign(
            **{NutsRegion.nid: lambda df: df["NUTS_ID"] + "-" + df["year"].astype(str)}
        )
    )

    nuts_table.replace_all(gdf)
    locator_df = (
        gdf.loc[:, [NutsRegion.nid, "geometry"]]
        .h3.polyfill(h3_level, explode=True)
        .rename(
            columns={
                NutsRegion.nid: NutsLocator.region.nid,
                "h3_polyfill": NutsLocator.hexagon.hid,
            }
        )
    )

    locator_table.replace_all(locator_df)

    h3_table.replace_all(
        locator_df.drop_duplicates(subset=[NutsLocator.hexagon.hid])
        .assign(**{HexagonHash.level: h3_level})
        .rename(columns={NutsLocator.hexagon.hid: HexagonHash.hid})
    )
