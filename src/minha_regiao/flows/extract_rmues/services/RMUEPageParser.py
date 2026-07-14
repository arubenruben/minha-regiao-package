from scrapling.parser import Selector

from minha_regiao.flows.extract_rmues.schema.RMUERegulation import RegulationDocument, RMUERegulation

# The DR page lists two labelled sections per municipality, each followed by
# zero or more <p><a>...</a></p> links (zero when the municipality hasn't
# reported a regulation). The section label is the only way to tell which
# block a given link belongs to.
URBANIZATION_LABEL = "Regulamento Municipal da Urbanização e da Edificação"
FEES_LABEL = "Regulamento de Taxas e Cauções por Operações Urbanísticas"


def parse_rmue_regulations(page: Selector) -> list[RMUERegulation]:
    entries: list[RMUERegulation] = []
    current: RMUERegulation | None = None
    section: str | None = None

    for element in page.css("h2, p"):
        if element.tag == "h2":
            if current is not None:
                entries.append(current)
            current = RMUERegulation(municipality=element.get_all_text(strip=True))
            section = None
            continue

        if current is None:
            continue

        strong = element.css("strong")
        if strong:
            label = strong[0].get_all_text(strip=True)
            section = {URBANIZATION_LABEL: "urbanization", FEES_LABEL: "fees"}.get(label)
            continue

        if section is None:
            continue

        documents = [
            RegulationDocument(name=a.get_all_text(strip=True), dre_url=href.strip())
            for a in element.css("a")
            if (href := a.attrib.get("href"))
        ]
        if not documents:
            continue

        if section == "urbanization":
            current.urbanization_documents.extend(documents)
        else:
            current.fee_documents.extend(documents)

    if current is not None:
        entries.append(current)

    return entries
