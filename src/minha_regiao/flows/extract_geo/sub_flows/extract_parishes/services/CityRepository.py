from minha_regiao.database.DatabaseManager import connection
from minha_regiao.entity.City import City
from minha_regiao.flows.extract_geo.services.FuzzyMatch import DEFAULT_THRESHOLD, resolve_name

# A parish's 6-digit INE code is its city's 4-digit INE code plus a 2-digit
# suffix identifying the parish within it (e.g. city "0101" + "03" ->
# freguesia "010103"), so the link is a deterministic prefix match rather
# than a lookup that needs to change per election-results era.
CITY_INE_CODE_LENGTH = 4

# Mainland districts are numbered 01-18; Açores and Madeira use other
# prefixes, which additionally differ between the pre-2013 election
# spreadsheet's own numbering and the current INE scheme cities are stored
# under, so a parish's INE code prefix can't be trusted to resolve its city
# there. Restricting the name-based fallback to this case avoids it
# masking real mismatches for mainland parishes, where the prefix is reliable.
MAINLAND_DISTRICT_CODES = frozenset(f"{i:02d}" for i in range(1, 19))


async def load_cities(db_url: str) -> list[City]:
    async with connection(db_url):
        return await City.all()


def index_cities_by_ine_code(cities: list[City]) -> dict[str, City]:
    return {city.ine_code: city for city in cities}


def index_cities_by_name(cities: list[City]) -> dict[str, City]:
    return {city.name: city for city in cities}


def is_mainland_ine_code(ine_code: str) -> bool:
    return ine_code[:2] in MAINLAND_DISTRICT_CODES


def match_city_by_parish_ine_code(parish_ine_code: str, cities_by_ine_code: dict[str, City]) -> City | None:
    return cities_by_ine_code.get(parish_ine_code[:CITY_INE_CODE_LENGTH])


def match_city_by_parish_name(
    parish_name: str, cities_by_name: dict[str, City], threshold: float = DEFAULT_THRESHOLD
) -> City | None:
    matched_name = resolve_name(parish_name, cities_by_name, threshold)
    return cities_by_name[matched_name] if matched_name is not None else None
