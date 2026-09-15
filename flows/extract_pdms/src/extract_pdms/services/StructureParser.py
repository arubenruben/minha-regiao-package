import re

from extract_pdms.schema.RegulationStructure import ArticleSection

_ROMAN = r"[IVXLCDM]+"

# Every header recognised below is required to be the *only* content on its
# line (anchored with both ^ and $, modulo surrounding whitespace) -- that's
# how these headings are actually typeset in a Diário da República
# regulation (e.g. "CAPÍTULO III" alone on its line, its heading on the
# next). It also happens to be what keeps this parser from misfiring on the
# gazette's own running page furniture (page numbers, "Diário da República,
# 2.ª série — N.º 82 — ..." masthead, "PARTE H" section marker on a 2019+
# masthead): that text always shares a line with other content, so it never
# matches a fully-anchored pattern.
_PARTE_RE = re.compile(r"^\s*PARTE\s+([A-Z])\s*\.?\s*$", re.IGNORECASE)
_TITULO_RE = re.compile(rf"^\s*T[IÍ]TULO\s+({_ROMAN})\s*\.?\s*$", re.IGNORECASE)
_CAPITULO_RE = re.compile(rf"^\s*CAP[IÍ]TULO\s+({_ROMAN})\s*\.?\s*$", re.IGNORECASE)
# Matched before SECÇÃO would even be tried (see _HEADER_MATCHERS order),
# but note it wouldn't be ambiguous either way: "^\s*SEC..." can't match a
# "SUBSECÇÃO ..." line since the very next character after "S" is "U", not
# "E" -- the anchors alone already disambiguate the two.
_SUBSECCAO_RE = re.compile(rf"^\s*SUBSEC[CÇ][AÃ]O\s+({_ROMAN})\s*\.?\s*$", re.IGNORECASE)
_SECCAO_RE = re.compile(rf"^\s*SEC[CÇ][AÃ]O\s+({_ROMAN})\s*\.?\s*$", re.IGNORECASE)
# "º" is the masculine ordinal indicator (U+00BA) Diário da República text
# consistently uses ("Artigo 1.º"); "o" is accepted too as a fallback for
# PDFs where font substitution drops the real glyph. An optional "-A"-style
# suffix covers articles inserted by later amendments (e.g. "Artigo 10.º-A").
_ARTIGO_RE = re.compile(r"^\s*Artigo\s+(\d+)\.?\s*[ºo](-[A-Z])?\s*\.?\s*$", re.IGNORECASE)

# A heuristic for "this line is body prose, not a heading" -- used to decide
# whether the line right after a header is that header's own title/heading
# or already the start of the section's content. Covers the numbered-
# paragraph and alínea conventions Portuguese legal text opens articles
# with: "1 — ...", "a) ...", "i) ...".
_BODY_START_RE = re.compile(r"^(\d+\s*[-–—]|[a-z]\)|[ivxlcdm]+\))", re.IGNORECASE)

# Tried in this order for every non-blank line; order matters only in that
# it's the order headings can plausibly nest (Parte down to Artigo), not for
# disambiguation -- each pattern is mutually exclusive by construction.
_HEADER_MATCHERS = (
    ("parte", _PARTE_RE),
    ("titulo", _TITULO_RE),
    ("capitulo", _CAPITULO_RE),
    ("subseccao", _SUBSECCAO_RE),
    ("seccao", _SECCAO_RE),
    ("artigo", _ARTIGO_RE),
)

_MAX_HEADING_LENGTH = 150


def _is_header_line(line: str) -> bool:
    return any(pattern.match(line) for _, pattern in _HEADER_MATCHERS)


def _consume_heading(lines: list[str], index: int) -> tuple[int, str | None]:
    """Looks at `lines[index]` -- the line right after a header was matched
    -- and decides whether it's that header's own heading/title line or
    already body content. Returns `(next_index, heading)`: `next_index` is
    `index + 1` (heading consumed) when it looks like a heading, otherwise
    `index` unchanged so the caller re-processes that same line as body (or
    as the next header, if it turns out to be one).
    """
    if index >= len(lines):
        return index, None

    candidate = lines[index].strip()
    if not candidate:
        return index, None
    if _is_header_line(candidate):
        return index, None
    if _BODY_START_RE.match(candidate):
        return index, None
    if len(candidate) > _MAX_HEADING_LENGTH:
        return index, None

    return index + 1, candidate


def parse_structure(text: str) -> list[ArticleSection]:
    """Splits a regulation's raw extracted text into one `ArticleSection`
    per Artigo, carrying whichever Parte/Título/Capítulo/Secção/Subsecção
    heading was most recently opened above it.

    Any text before the first recognised header (announcement/deliberação
    preamble, masthead noise, ...) is dropped -- it isn't part of any
    Artigo, and the document's full raw text is already preserved separately
    (see RegulationDocument.text). Likewise, entering a header level resets
    every level nested *below* it (a new CAPÍTULO clears the current
    Secção/Subsecção, a new TÍTULO also clears the current Capítulo, ...),
    but never the levels *above* it.
    """
    lines = text.splitlines()

    heading_state: dict[str, str | None] = {
        "parte": None,
        "parte_heading": None,
        "titulo": None,
        "titulo_heading": None,
        "capitulo": None,
        "capitulo_heading": None,
        "seccao": None,
        "seccao_heading": None,
        "subseccao": None,
        "subseccao_heading": None,
    }

    # Levels nested at or below each key, cleared whenever that key's level
    # is (re-)entered.
    _RESET_BELOW: dict[str, tuple[str, ...]] = {
        "parte": ("titulo", "capitulo", "subseccao", "seccao"),
        "titulo": ("capitulo", "subseccao", "seccao"),
        "capitulo": ("subseccao", "seccao"),
        "seccao": ("subseccao",),
        "subseccao": (),
    }

    sections: list[ArticleSection] = []
    current: dict[str, object] | None = None

    def flush() -> None:
        nonlocal current
        if current is None:
            return
        body = "\n".join(current["body"]).strip()  # type: ignore[arg-type]
        if body or current["artigo_heading"]:
            sections.append(
                ArticleSection(
                    parte=heading_state["parte"],
                    parte_heading=heading_state["parte_heading"],
                    titulo=heading_state["titulo"],
                    titulo_heading=heading_state["titulo_heading"],
                    capitulo=heading_state["capitulo"],
                    capitulo_heading=heading_state["capitulo_heading"],
                    seccao=heading_state["seccao"],
                    seccao_heading=heading_state["seccao_heading"],
                    subseccao=heading_state["subseccao"],
                    subseccao_heading=heading_state["subseccao_heading"],
                    artigo=current["artigo"],  # type: ignore[arg-type]
                    artigo_heading=current["artigo_heading"],  # type: ignore[arg-type]
                    text=body,
                )
            )
        current = None

    index = 0
    total = len(lines)

    while index < total:
        raw_line = lines[index]
        line = raw_line.strip()

        if not line:
            if current is not None:
                current["body"].append("")  # type: ignore[union-attr]
            index += 1
            continue

        matched_level = next((level for level, pattern in _HEADER_MATCHERS if pattern.match(line)), None)

        if matched_level == "artigo":
            flush()
            match = _ARTIGO_RE.match(line)
            assert match is not None
            number, suffix = match.group(1), match.group(2) or ""
            current = {"artigo": f"{number}.º{suffix}", "artigo_heading": None, "body": []}
            index += 1
            index, heading = _consume_heading(lines, index)
            current["artigo_heading"] = heading
            continue

        if matched_level is not None:
            match = next(pattern for level, pattern in _HEADER_MATCHERS if level == matched_level).match(line)
            assert match is not None
            value = match.group(1).upper()

            if heading_state[matched_level] == value:
                # The same header value recurring with nothing at this
                # level (or above) having changed in between is virtually
                # always running page-header noise re-extracted from a
                # Diário da República masthead (e.g. "PARTE H" repeated on
                # every page), not a genuine re-entry into the same Parte/
                # Título/Capítulo/Secção/Subsecção -- skip it rather than
                # spuriously flushing the in-progress Artigo and wiping out
                # nested headings that are still in force.
                index += 1
                continue

            flush()
            for cleared in _RESET_BELOW[matched_level]:
                heading_state[cleared] = None
                heading_state[f"{cleared}_heading"] = None
            heading_state[matched_level] = value
            index += 1
            index, heading = _consume_heading(lines, index)
            heading_state[f"{matched_level}_heading"] = heading
            continue

        if current is not None:
            current["body"].append(raw_line)  # type: ignore[union-attr]
        index += 1

    flush()
    return sections
