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
# entity name mentioned in running prose -- e.g. a notice's own body text
# referring back to "o município de Aguiar da Beira estabelece...", which,
# once the PDF's line-wrapping happens to land "município de Aguiar da
# Beira estabelece..." at the start of a line, is otherwise
# indistinguishable from a real "MUNICÍPIO DE AGUIAR DA BEIRA" header by
# leading text alone. What actually distinguishes the two is that the real
# header is typeset entirely in upper case and contains nothing else, while
# running prose is not -- so, unlike a bare `\S.*$` tail (which imposes no
# such constraint and was matching straight through into lower-case prose),
# this requires case-sensitive "MUNICÍPIO"/"MUNICIPIO" and forbids any
# lower-case letter (plain or accented) anywhere in the rest of the line.
_ENTITY_HEADER_RE = re.compile(r"^\s*MUNIC[IÍ]PIO\s+D[EO]\s+[^a-zà-ÿ\n]+$")

# Optional annotation dre.pt sometimes prints between the heading phrase and
# "n.º", e.g. "Aviso (extrato) n.º 15185/2018".
_QUALIFIER_FRAGMENT = r"(?:\s*\([^)]{1,40}\))?"

# Known heading phrases (see _DOC_TYPE_HEADINGS), longest first so a more
# specific phrase (e.g. "Resolução do Conselho de Ministros") wins over a
# shorter one that's also a valid heading on its own (e.g. "Resolução").
_KNOWN_HEADINGS_ALTERNATION = "|".join(
    re.escape(phrase) for phrase in sorted(set(_DOC_TYPE_HEADINGS.values()), key=len, reverse=True)
)

# A modern notice's header stands alone on its own line, with the body
# starting on the next one -- hence anchoring a header match with `$`. Old
# (pre-2000s) gazette pages instead run the heading, "n.º NNNN/YYYY", an
# optional trailing "(N.ª série)" qualifier, and the notice's own body text
# together as one continuous paragraph, e.g. "Declaração n.º 4/2004 (2.ª
# série).— Torna-se público que..." (DECL 4/2004,
# https://snit-mais.dgterritorio.gov.pt/SNIT/Diplomas/DECL%204_2004.pdf).
# There, what marks the header's end isn't end-of-line but the ".—"
# (period + em/en dash) that opens the body -- so a header match ends in
# either an end-of-line (modern) or that trailing qualifier followed by
# ".—" (old), never just an open-ended "anything can follow".
_HEADER_TERMINATOR = rf"(?:{_QUALIFIER_FRAGMENT}\s*\.\s*[—–]|\s*\.?\s*$)"

# Marks the start of any notice ("Aviso n.º 5420/2014", "Edital n.º
# 328/2014", "Declaração de Retificação n.º .../...", ...), regardless of
# doc_type -- used only to find where the *next* notice begins (i.e. where
# the target notice's own text ends), not to identify the target itself.
# The short, non-greedy label deliberately caps how much of the line can
# precede "n.º NNNN/YYYY", so this can't accidentally swallow a full
# sentence that happens to end in a similarly-shaped reference. That
# generic form requires a modern 4-digit year, since a looser 2-digit year
# on an arbitrary label spuriously matches inline legal citations that
# happen to end a wrapped line (e.g. "..., no Decreto-Lei n.º 46/94") --
# _HEADER_TERMINATOR is what keeps this safe even for the old run-on style,
# since a real citation continues past the year with more of its own
# sentence (", de 22 de Setembro"), never a bare period-dash. Old
# (pre-2000) gazette pages print 2-digit years even in real notice headers
# (e.g. "Resolução do Conselho de Ministros n.º 180/97"), so those are only
# recognised when anchored to one of the known heading phrases, which is
# specific enough to not false-positive the same way.
_NOTICE_HEADER_RE = re.compile(
    r"^\s*(?:"
    r"[^\n]{1,60}?\s+n\.?\s*[ºo]\s*\d+\s*/\s*\d{4}"
    r"|"
    rf"(?:{_KNOWN_HEADINGS_ALTERNATION}){_QUALIFIER_FRAGMENT}\s+n\.?\s*[ºo]\s*\d+\s*/\s*\d{{2,4}}"
    rf")(?:\s*/\s*\d+)?{_HEADER_TERMINATOR}",
    re.IGNORECASE,
)


def _target_header_pattern(heading_phrase: str, number: str, year: int, suffix: int | None) -> re.Pattern[str]:
    two_digit_year = f"{year % 100:02d}"
    suffix_fragment = rf"\s*/\s*{suffix}" if suffix is not None else ""
    return re.compile(
        rf"^\s*{re.escape(heading_phrase)}{_QUALIFIER_FRAGMENT}\s+n\.?\s*[ºo]\s*{re.escape(number)}"
        rf"\s*/\s*(?:{year}|{two_digit_year}){suffix_fragment}{_HEADER_TERMINATOR}",
        re.IGNORECASE,
    )


def find_notice_text(text: str, doc_type: str, number: str, year: int, suffix: int | None = None) -> str:
    """Returns the slice of `text` -- from its own "<heading> n.º
    <number>/<year>" header line up to (but not including) whichever comes
    first of the next entity header or the next notice header, or the end
    of `text` if neither occurs again -- that belongs to the notice
    identified by `doc_type`/`number`/`year`/`suffix`.

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
    target_pattern = _target_header_pattern(heading_phrase, number, year, suffix)

    # A multi-page notice's own heading is often reprinted verbatim as a
    # running page header (seen on e.g. "Declaração n.º 55/2024/2", a
    # 2-page notice whose heading appears a first time atop page 1's
    # running header, again as the notice's real heading right after its
    # "MUNICÍPIO DE ..." entity line, and a third time atop page 2). Prefer
    # whichever match is immediately preceded by an entity header -- that's
    # the real section start -- and only fall back to the first match when
    # none is (e.g. national-level notices like a Resolução do Conselho de
    # Ministros, which aren't published under a "MUNICÍPIO DE ..." entity).
    start_candidates = [index for index, line in enumerate(lines) if target_pattern.match(line.strip())]
    if not start_candidates:
        raise GazetteNoticeNotFoundError(
            f"Could not find a {heading_phrase!r} n.º {number}/{year} header in the extracted text"
        )
    start = next(
        (index for index in start_candidates if index > 0 and _ENTITY_HEADER_RE.match(lines[index - 1].strip())),
        start_candidates[0],
    )

    # Excludes lines that are just a repeat of the target's own header --
    # e.g. that same running-header reprint on page 2 -- from ending the
    # slice early; only a genuinely different entity/notice header does.
    end = next(
        (
            index
            for index in range(start + 1, len(lines))
            if not target_pattern.match(lines[index].strip())
            and (_ENTITY_HEADER_RE.match(lines[index].strip()) or _NOTICE_HEADER_RE.match(lines[index].strip()))
        ),
        len(lines),
    )

    return "\n".join(lines[start:end]).strip()
