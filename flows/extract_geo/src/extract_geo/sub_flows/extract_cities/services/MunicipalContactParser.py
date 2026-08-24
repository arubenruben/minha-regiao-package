from scrapling.parser import Selector

from extract_geo.sub_flows.extract_cities.schema.MunicipalContact import MunicipalContact


def parse_municipal_contact_row(row: Selector) -> MunicipalContact:
    cells = row.css("td")

    # The e-mail/web cell holds two anchors: the first is a (sometimes
    # malformed) mailto link whose visible text is the address, the second
    # is the website link, often left empty.
    links = cells[4].css("a")
    email = links[0].text.strip() if len(links) > 0 else ""
    website = links[1].attrib.get("href", "").strip() if len(links) > 1 else ""

    return MunicipalContact(
        municipality=cells[0].text.strip(),
        president=cells[1].text.strip(),
        address=cells[2].get_all_text(separator=", ", strip=True),
        phone=cells[3].text.strip(),
        email=email or None,
        website=website or None,
    )
