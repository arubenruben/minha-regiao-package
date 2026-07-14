from scrapling.fetchers import StealthyFetcher

url = "https://diariodarepublica.pt/dr/geral/areas-tematicas/regul-municipais"

page = StealthyFetcher.fetch(url, headless=True, network_idle=True)


page.prettify()