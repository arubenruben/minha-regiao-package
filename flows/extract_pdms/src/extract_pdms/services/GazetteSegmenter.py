import re

from extract_pdms.exception.GazetteNoticeNotFoundError import GazetteNoticeNotFoundError

# A downloaded regulation PDF is not scoped to one regulation -- it's a raw
# Diário da República page range, which bundles the target notice alongside
# unrelated notices from other municipalities (and other entities: personnel
# appointments, competitions, editais, ...) published on the same page(s).
# This module locates the one notice that matches a RegulationDocument's own
# doc_type/number/year (parsed from its PDF filename by
# extract_pdms.services.SnitSearch.parse_pdf_metadata) and slices the text
# down to just that notice, so extract_pdms.services.StructureParser only
# ever sees the regulation it's actually meant to parse.

# dre.pt's own PDF-filename doc_type abbreviation (see SnitSearch.
# PDF_FILENAME_PATTERN) mapped to the heading phrase it's printed as in the
# gazette body, e.g. "AVISO 5423_2014.pdf" -> "Aviso n.º 5423/2014". Extend
# this as new doc_types are encountered -- an unmapped one is treated as a
# match failure (see find_notice_text) rather than guessed at.
_DOC_TYPE_HEADINGS: dict[str, str] = {
    "AVISO": "Aviso",
    "EDITAL": "Edital",
    "DECL": "Declaração",
    "DECL RET": "Declaração de Retificação",
    "DESP": "Despacho",
    "PORT": "Portaria",
    "RCM": "Resolução do Conselho de Ministros",
    "RES": "Resolução",
    "DEC REG": "Decreto Regulamentar",
    "DEC LEI": "Decreto-Lei",
    "DEC": "Decreto",
    "LEI": "Lei",
    "DELIB": "Deliberação",
}

# Marks the start of a new publishing entity's section (e.g. "MUNICÍPIO DE
# GAVIÃO"). Required to be the only content on its line, same anchoring
# rationale as extract_pdms.services.StructureParser's headers: it's how
# these are actually typeset, and it's what keeps this from matching an
# entity name mentioned in running prose.
_ENTITY_HEADER_RE = re.compile(r"^\s*MUNIC[IÍ]PIO\s+D[EO]\s+\S.*$", re.IGNORECASE)

# Marks the start of any notice ("Aviso n.º 5420/2014", "Edital n.º
# 328/2014", "Declaração de Retificação n.º .../...", ...), regardless of
# doc_type -- used only to find where the *next* notice begins (i.e. where
# the target notice's own text ends), not to identify the target itself.
# The short, non-greedy label deliberately caps how much of the line can
# precede "n.º NNNN/YYYY", so this can't accidentally swallow a full
# sentence that happens to end in a similarly-shaped reference.
_NOTICE_HEADER_RE = re.compile(r"^\s*[^\n]{1,60}?\s+n\.?\s*[ºo]\s*\d+\s*/\s*\d{4}\s*\.?\s*$")


def _target_header_pattern(heading_phrase: str, number: str, year: int) -> re.Pattern[str]:
    return re.compile(
        rf"^\s*{re.escape(heading_phrase)}\s+n\.?\s*[ºo]\s*{re.escape(number)}\s*/\s*{year}\s*\.?\s*$",
        re.IGNORECASE,
    )


def find_notice_text(text: str, doc_type: str, number: str, year: int) -> str:
    """Returns the slice of `text` -- from its own "<heading> n.º
    <number>/<year>" header line up to (but not including) whichever comes
    first of the next entity header or the next notice header, or the end
    of `text` if neither occurs again -- that belongs to the notice
    identified by `doc_type`/`number`/`year`.

    Raises GazetteNoticeNotFoundError, rather than falling back to the full
    text, when `doc_type` has no known heading mapping or when no line in
    `text` matches the resulting header: both are signs the matching
    mechanism needs to be extended, not conditions to paper over.
    """
    heading_phrase = _DOC_TYPE_HEADINGS.get(doc_type.upper())
    if heading_phrase is None:
        raise GazetteNoticeNotFoundError(
            f"No known Diário da República heading phrase for doc_type {doc_type!r} "
            "-- add it to extract_pdms.services.GazetteSegmenter._DOC_TYPE_HEADINGS"
        )

    lines = text.splitlines()
    target_pattern = _target_header_pattern(heading_phrase, number, year)

    start = next((index for index, line in enumerate(lines) if target_pattern.match(line.strip())), None)
    if start is None:
        raise GazetteNoticeNotFoundError(
            f"Could not find a {heading_phrase!r} n.º {number}/{year} header in the extracted text"
        )

    end = next(
        (
            index
            for index in range(start + 1, len(lines))
            if _ENTITY_HEADER_RE.match(lines[index].strip()) or _NOTICE_HEADER_RE.match(lines[index].strip())
        ),
        len(lines),
    )

    return "\n".join(lines[start:end]).strip()
