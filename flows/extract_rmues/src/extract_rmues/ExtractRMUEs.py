from scrapling.fetchers import StealthyFetcher

RMUE_URL = "https://diariodarepublica.pt/dr/geral/areas-tematicas/regul-municipais"

page = StealthyFetcher.fetch(RMUE_URL, headless=True, network_idle=True)

print(f"status: {page.status}")

headings = page.css("h2")

print(f"found {len(headings)} <h2> municipality headings")



#for heading in headings[:5]:
#    print(f"  - {heading.get_all_text(strip=True)}")
