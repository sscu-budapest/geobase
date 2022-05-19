from functools import partial
from multiprocessing import cpu_count
from pathlib import Path
from tempfile import TemporaryDirectory

import datazimmer as dz
import requests
from aswan import get_soup
from atqo import parallel_map
from pyrosm import OSM

fabrik_url = dz.SourceUrl("https://download.geofabrik.de/")


class AdministrativeUnit(dz.AbstractEntity):
    aid = dz.Index & str
    name = str
    level = int
    geometry = str
    country_id = str


osm_admin_table = dz.ScruTable(AdministrativeUnit)


@dz.register_data_loader
def load_osm_admin():
    fabrik_soup = get_soup(fabrik_url)
    continents = _get_subregions(fabrik_soup)

    country_links = []
    for continent in continents:
        continent_soup = get_soup(f"{fabrik_url}{continent}")
        country_links += [
            f"{fabrik_url}{a['href']}"
            for a in continent_soup.find_all("a", text="[.osm.pbf]")
        ]

    parallel_map(
        partial(proc_country_link, table=osm_admin_table),
        set(country_links),
        workers=min(cpu_count() // 2, 20),
    )


def proc_country_link(country_link, table: dz.ScruTable):
    country_id = country_link.split("/")[-1].split("-latest")[0]
    with TemporaryDirectory() as tmpdir:
        tmp_osm = Path(tmpdir, "tmp.osm.pbf")
        tmp_osm.write_bytes(requests.get(country_link).content)
        osm = OSM(tmp_osm.as_posix())
        boundary_df = osm.get_boundaries()
    table.extend(
        boundary_df.dropna(subset=["admin_level"])
        .rename(
            columns={
                "id": AdministrativeUnit.aid,
                "admin_level": AdministrativeUnit.level,
            }
        )
        .assign(**{AdministrativeUnit.country_id: country_id})
    )


def _get_subregions(soup):
    return [td.find("a")["href"] for td in soup.find_all("td", class_="subregion")]
