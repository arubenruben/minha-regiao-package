class GazetteNoticeNotFoundError(Exception):
    """Raised when the Diário da República notice a regulation document is
    supposed to be (identified by its doc_type/number/year, from
    extract_pdms.services.SnitSearch.parse_pdf_metadata) can't be located
    inside the downloaded page's extracted text -- either because the
    doc_type has no known DR heading phrase yet, or because no line in the
    text matches "<heading phrase> n.º <number>/<year>".

    Deliberately not swallowed by a silent fallback to the full page text:
    the matching mechanism (extract_pdms.services.GazetteSegmenter) is
    heuristic and not proven exhaustive against every DR formatting
    variant, so a miss should surface loudly (as DocumentStatus.
    NOTICE_NOT_FOUND, see extract_pdms.tasks.ExtractPdfText) rather than be
    hidden behind a "looks fine" result that's actually the wrong -- or an
    unrelated municipality's -- content.
    """
