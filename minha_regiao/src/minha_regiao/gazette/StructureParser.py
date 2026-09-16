import re

from minha_regiao.gazette.RegulationStructure import (
    Article,
    Chapter,
    Part,
    Section,
    StructureNode,
    Subsection,
    Title,
)

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

# Nesting depth, shallowest to deepest. Unlike _HEADER_MATCHERS above (whose
# order is just match-checking order, see its own comment), order here is
# significant: it's the order these levels actually nest in a Diário da
# República regulation, and it drives how deep a new header pops the
# currently open container stack (see parse_structure).
_LEVELS = ("parte", "titulo", "capitulo", "seccao", "subseccao")

_LEVEL_CLASS: dict[str, type] = {
    "parte": Part,
    "titulo": Title,
    "capitulo": Chapter,
    "seccao": Section,
    "subseccao": Subsection,
}

# Field each level's container class holds its immediate child containers
# (the next level down) under. Subsecção has none -- it's the deepest
# container level, holding only Artigos (via the `articles` field every
# level shares).
_CHILD_FIELD: dict[str, str] = {
    "parte": "titles",
    "titulo": "chapters",
    "capitulo": "sections",
    "seccao": "subsections",
}

# Levels nested at or below each key, cleared whenever that key's level is
# (re-)entered.
_RESET_BELOW: dict[str, tuple[str, ...]] = {
    "parte": ("titulo", "capitulo", "seccao", "subseccao"),
    "titulo": ("capitulo", "seccao", "subseccao"),
    "capitulo": ("seccao", "subseccao"),
    "seccao": ("subseccao",),
    "subseccao": (),
}


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


def parse_structure(text: str) -> list[StructureNode]:
    """Splits a regulation's raw extracted text into a nested Parte > Título
    > Capítulo > Secção > Subsecção > Artigo tree.

    Any text before the first recognised header (announcement/deliberação
    preamble, masthead noise, ...) is dropped -- it isn't part of any
    Artigo, and the document's full raw text is already preserved separately
    (see RegulationDocument.text). Levels a given regulation doesn't use are
    simply absent from the tree; the returned list's items are whichever
    level(s) the regulation actually opens at (see StructureNode).

    Entering a header level resets every level nested *below* it (a new
    CAPÍTULO clears the current Secção/Subsecção, a new TÍTULO also clears
    the current Capítulo, ...), but never the levels *above* it -- modelled
    here as popping the open-container stack down to (and including) the
    re-entered level before attaching the new node.
    """
    lines = text.splitlines()

    heading_state: dict[str, str | None] = {level: None for level in _LEVELS}

    roots: list[StructureNode] = []
    # Currently open containers, outermost first, each holding the level
    # name alongside the node itself (the level name is what's needed to
    # look up which of the node's own fields new children attach under).
    stack: list[tuple[str, StructureNode]] = []

    def attach_article(article: Article) -> None:
        parent = stack[-1][1] if stack else None
        parent.articles.append(article) if parent is not None else roots.append(article)  # type: ignore[union-attr]

    current: dict[str, object] | None = None

    def flush() -> None:
        nonlocal current
        if current is None:
            return
        body = "\n".join(current["body"]).strip()  # type: ignore[arg-type]
        if body or current["artigo_heading"]:
            attach_article(
                Article(
                    number=current["artigo"],  # type: ignore[arg-type]
                    heading=current["artigo_heading"],  # type: ignore[arg-type]
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
            heading_state[matched_level] = value

            depth = _LEVELS.index(matched_level)
            while stack and _LEVELS.index(stack[-1][0]) >= depth:
                stack.pop()

            # `matched_level` can still be more than one level below
            # whatever's left open on the stack -- e.g. a SUBSECÇÃO
            # directly under a CAPÍTULO, with no SECÇÃO in between.
            # Synthesize an empty container for each skipped intermediate
            # level (number="" marks it as not itself drawn from a header
            # line) so the new node always lands in a field typed for its
            # own level, never a shallower one -- attaching it directly to
            # the open CAPÍTULO above would silently put e.g. a Subsection
            # into a `sections: list[Section]` field.
            while stack and _LEVELS.index(stack[-1][0]) + 1 < depth:
                skip_parent_level, skip_parent_node = stack[-1]
                skipped_level = _LEVELS[_LEVELS.index(skip_parent_level) + 1]
                placeholder = _LEVEL_CLASS[skipped_level](number="")
                getattr(skip_parent_node, _CHILD_FIELD[skip_parent_level]).append(placeholder)  # type: ignore[index]
                stack.append((skipped_level, placeholder))

            node = _LEVEL_CLASS[matched_level](number=value)
            parent_level, parent_node = stack[-1] if stack else (None, None)
            if parent_node is not None:
                getattr(parent_node, _CHILD_FIELD[parent_level]).append(node)  # type: ignore[index]
            else:
                roots.append(node)
            stack.append((matched_level, node))

            index += 1
            index, heading = _consume_heading(lines, index)
            node.heading = heading  # type: ignore[union-attr]
            continue

        if current is not None:
            current["body"].append(raw_line)  # type: ignore[union-attr]
        index += 1

    flush()
    return roots
