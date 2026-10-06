import re
import unicodedata
from collections import defaultdict
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from itertools import combinations, count

import Levenshtein
from pydantic import BaseModel

from minha_regiao.gazette.RegulationStructure import (
    Article,
    Chapter,
    Part,
    Section,
    StructureNode,
    Subsection,
    Title,
)

# An aviso that amends a regulation typically quotes each article it changes
# ("«Artigo 58.º ...») and then republishes the whole consolidated regulation
# as an annex ("CAPÍTULO I ... Artigo 58.º ..."). StructureParser has no
# notion of quotation, so it reads both copies as real articles. This module
# collapses those copies back into one, keeping the one that sits in the
# fullest hierarchy.
#
# The bias throughout is toward keeping: a duplicate left in is harmless, an
# article merged away wrongly is lost content. In particular the "Tabela"
# annex of a fee regulation repeats an article's number *and* heading dozens
# of times with different amounts -- merging those would drop precisely the
# amounts, which is why number + heading alone is never enough (see
# _is_same_article).

_SIMILARITY_THRESHOLD = 0.95

# Breadcrumb of an article that hangs directly off the document root, outside
# any Parte/Título/Capítulo/Secção/Subsecção.
ROOT_LOCATION = "raiz"

_CONTAINER_LABELS: dict[type, str] = {
    Part: "Parte",
    Title: "Título",
    Chapter: "Capítulo",
    Section: "Secção",
    Subsection: "Subsecção",
}

# Field each container class holds its immediate child containers under.
# Subsecção is the deepest level and has none.
_CHILD_CONTAINER_FIELDS: dict[type, str] = {
    Part: "titles",
    Title: "chapters",
    Chapter: "sections",
    Section: "subsections",
}

# Month names are letters-only in both layouts ("janeiro" in 2019+, "Abril" in
# the older one), hence a loose word match instead of a month list.
_DATE = r"\d{1,2}\s+de\s+[^\W\d_]+\s+de\s+\d{4}"
_ISSUE_NUMBER = r"N\.?\s*[ºo°]\s*\d{1,3}"
_PAGE_MARKER = r"P[aá]g\.?\s*\d+"
_SERIES = r"Di[aá]rio\s+da\s+Rep[uú]blica,?\s*\d\.?\s*[ªa]\s*s[eé]rie"

# Layout 2019+: "N.º 5 7 de janeiro de 2022 Pág. 725". The "Pág." marker is
# what ties this to page furniture (a bare "N.º 5 7 de janeiro de 2022" could
# in principle be prose); it's printed after the date on one page parity and
# before it on the other. The "Diário da República, 2.ª série" and "PARTE H"
# masthead pieces sometimes ride along in front of the issue number.
_NEW_LAYOUT_CORE = rf"(?:{_SERIES}\s*)?(?:PARTE\s+[A-Z]\s+)?{_ISSUE_NUMBER}\s+{_DATE}"
_NEW_LAYOUT_HEADER = rf"(?:{_PAGE_MARKER}\s+{_NEW_LAYOUT_CORE}|{_NEW_LAYOUT_CORE}\s+{_PAGE_MARKER})"

# Older layout: "Diário da República, 2.ª série — N.º 82 — 28 de Abril de 2009",
# with the page number ("16 704", thousands separated by a space) printed at
# the far edge of the line.
_OLD_LAYOUT_HEADER = rf"{_SERIES}(?:\s*-?\s*[AB])?\s*[—–-]\s*{_ISSUE_NUMBER}\s*[—–-]\s*{_DATE}"
_PAGE_NUMBER = r"\d{1,3}(?:[ \t]\d{3})*"

# The old layout's page number is only dropped when it sits at the start/end
# of its line -- consuming a bare number glued after a header in the middle of
# a line would risk eating the content that follows it (e.g. an article's
# leading "1").
_OLD_LAYOUT_RE = re.compile(
    rf"(?:^[ \t]*{_PAGE_NUMBER}[ \t]+)?{_OLD_LAYOUT_HEADER}(?:[ \t]+{_PAGE_NUMBER}[ \t]*$)?",
    re.IGNORECASE | re.MULTILINE,
)
_NEW_LAYOUT_RE = re.compile(_NEW_LAYOUT_HEADER, re.IGNORECASE)

# A hyphen (or soft hyphen) closing a line between two letters: the word was
# broken by the PDF's line wrapping ("de-\nvidamente"), not a real compound.
_HYPHEN_BREAK_RE = re.compile(r"(?<=[^\W\d_])[-­][ \t]*\r?\n[ \t]*(?=[^\W\d_])")
_DIGITS_RE = re.compile(r"\d+")
_NON_ALNUM_RE = re.compile(r"[^0-9a-z]+")


class DuplicateArticle(BaseModel):
    """One Artigo removed by `deduplicate_structure`, and the copy that was
    kept in its place.
    """

    article: Article
    # Breadcrumb of the removed copy: "raiz" or e.g. "Capítulo III › Secção IV".
    location: str
    kept: Article
    kept_location: str


class DeduplicationResult(BaseModel):
    structure: list[StructureNode]
    # Removed copies, in document order.
    duplicates: list[DuplicateArticle]
    # Containers (Parte/Título/Capítulo/Secção/Subsecção) removed because
    # deduplication left them empty, outermost first. A container that was
    # already empty in the parsed text is never listed here -- it stays.
    emptied: list[StructureNode]


@dataclass(frozen=True)
class _Located:
    """An Artigo together with where it sits in the tree. `order` is its
    index in document order and doubles as its identity -- the same Article
    instance can legitimately appear twice in a hand-built tree.
    """

    order: int
    article: Article
    path: tuple[str, ...]

    @property
    def depth(self) -> int:
        return len(self.path)

    @property
    def location(self) -> str:
        return " › ".join(self.path) if self.path else ROOT_LOCATION


@dataclass(frozen=True)
class _Fingerprint:
    number: str
    digits: tuple[str, ...]
    content: str


def _strip_accents(text: str) -> str:
    return "".join(char for char in unicodedata.normalize("NFKD", text) if not unicodedata.combining(char))


def _normalize(text: str) -> str:
    """Lower-cases, strips accents and collapses everything that isn't a
    letter or digit into single spaces. NFKD turns the ordinal indicators
    into plain letters, so "10.º-A" becomes "10 o a".
    """
    return _NON_ALNUM_RE.sub(" ", _strip_accents(text).lower()).strip()


def _strip_page_furniture(text: str) -> str:
    """Removes the Diário da República running headers/footers the PDF
    extraction glues into the middle of a regulation's text, in either
    layout. Replaced by nothing rather than a space so that a header landing
    between a hyphen and its continuation ("de-" ... "vidamente") still lets
    the hyphenation be undone afterwards.
    """
    return _NEW_LAYOUT_RE.sub("", _OLD_LAYOUT_RE.sub("", text))


def _clean_content(article: Article) -> str:
    """The article's heading and body as one string, with page furniture and
    line-break hyphenation removed. Still carries its digits and accents --
    see `_fingerprint`.

    Heading and body are taken together because a long heading wraps onto a
    second line in the PDF while the parser only takes the first line as the
    heading; which half lands in `heading` and which in `text` depends on the
    column width of each copy.
    """
    content = f"{article.heading}\n{article.text}" if article.heading else article.text
    return _HYPHEN_BREAK_RE.sub("", _strip_page_furniture(content))


def _fingerprint(article: Article) -> _Fingerprint:
    cleaned = _clean_content(article)
    return _Fingerprint(
        number=_normalize(article.number),
        digits=tuple(sorted(_DIGITS_RE.findall(cleaned))),
        content=_normalize(cleaned),
    )


def _is_same_article(first: _Fingerprint, second: _Fingerprint) -> bool:
    """Two Artigos are the same logical article only when all three hold:
    same (normalized) number, same digits in the content, and heading + body
    at least 95% similar. The digits check is what protects a fee table's
    differing amounts; the number check is what keeps "10.º-A" apart from
    its neighbour "10.º".
    """
    return (
        first.number == second.number
        and first.digits == second.digits
        and Levenshtein.ratio(first.content, second.content) >= _SIMILARITY_THRESHOLD
    )


def _container_label(node: StructureNode) -> str:
    # A synthesized placeholder level (see StructureParser) has no number.
    return f"{_CONTAINER_LABELS[type(node)]} {node.number}".strip()


def _child_containers(node: StructureNode) -> list[StructureNode]:
    field = _CHILD_CONTAINER_FIELDS.get(type(node))
    return list(getattr(node, field)) if field else []


def _locate_articles(nodes: list[StructureNode]) -> list[_Located]:
    """Every Artigo in the tree, in document order. Within a container its
    own articles come first, then its child containers -- the parser can't
    attach an article to a container once a nested one has opened.
    `_rebuild` walks in this same order.
    """
    located: list[_Located] = []

    def visit(node_list: list[StructureNode], path: tuple[str, ...]) -> None:
        for node in node_list:
            if isinstance(node, Article):
                located.append(_Located(order=len(located), article=node, path=path))
                continue

            inner_path = (*path, _container_label(node))
            for article in node.articles:
                located.append(_Located(order=len(located), article=article, path=inner_path))
            visit(_child_containers(node), inner_path)

    visit(nodes, ())
    return located


def _connected_groups(items: list[_Located], similar: Callable[[_Located, _Located], bool]) -> list[list[_Located]]:
    """Union-find over every pair of `items` for which `similar` holds, so
    that A~B and B~C put all three together. Grouping transitively keeps the
    outcome independent of the order pairs are compared in, which is also
    what makes `deduplicate_structure` idempotent: after one pass no two
    survivors are similar, or they'd have been joined.
    """
    parent = {item.order: item.order for item in items}

    def find(order: int) -> int:
        while parent[order] != order:
            parent[order] = parent[parent[order]]
            order = parent[order]
        return order

    for first, second in combinations(items, 2):
        if find(first.order) != find(second.order) and similar(first, second):
            parent[find(second.order)] = find(first.order)

    groups: dict[int, list[_Located]] = defaultdict(list)
    for item in items:
        groups[find(item.order)].append(item)
    return list(groups.values())


def _group_same_article(located: list[_Located]) -> list[list[_Located]]:
    fingerprints = {item.order: _fingerprint(item.article) for item in located}

    # Cheap bucket first: only articles sharing number and digits can match,
    # so the (comparatively costly) Levenshtein ratio runs within a bucket.
    buckets: dict[tuple[str, tuple[str, ...]], list[_Located]] = defaultdict(list)
    for item in located:
        fingerprint = fingerprints[item.order]
        buckets[(fingerprint.number, fingerprint.digits)].append(item)

    def similar(first: _Located, second: _Located) -> bool:
        return _is_same_article(fingerprints[first.order], fingerprints[second.order])

    return [group for bucket in buckets.values() for group in _connected_groups(bucket, similar)]


def _pick_kept(group: list[_Located]) -> _Located:
    # Deepest wins -- that's the copy with the full Parte/Título/Capítulo/
    # Secção hierarchy -- and the last one on a tie, since the republication
    # comes after the citation.
    return max(group, key=lambda item: (item.depth, item.order))


def _rebuild(
    nodes: list[StructureNode], removed: set[int], counter: Iterator[int]
) -> tuple[list[StructureNode], list[StructureNode]]:
    """Copies `nodes` without the articles whose document-order index is in
    `removed`, dropping containers that deduplication left empty. Returns the
    new list plus the dropped containers (outermost first). `counter` yields
    the next article's document-order index and must be walked exactly as
    `_locate_articles` does.
    """
    rebuilt: list[StructureNode] = []
    emptied: list[StructureNode] = []

    for node in nodes:
        if isinstance(node, Article):
            if next(counter) not in removed:
                rebuilt.append(node.model_copy())
            continue

        was_empty = not node.articles and not _child_containers(node)
        articles = [article.model_copy() for article in node.articles if next(counter) not in removed]

        update: dict[str, object] = {"articles": articles}
        emptied_below: list[StructureNode] = []
        child_field = _CHILD_CONTAINER_FIELDS.get(type(node))
        if child_field:
            children, emptied_below = _rebuild(list(getattr(node, child_field)), removed, counter)
            update[child_field] = children

        rebuilt_node = node.model_copy(update=update)
        if not was_empty and not articles and not _child_containers(rebuilt_node):
            emptied.append(rebuilt_node)
        else:
            rebuilt.append(rebuilt_node)
        emptied.extend(emptied_below)

    return rebuilt, emptied


def deduplicate_structure(nodes: list[StructureNode]) -> DeduplicationResult:
    """Collapses the same Artigo appearing more than once in a regulation's
    structure -- typically an amendment aviso's quoted article plus the
    consolidated regulation republished in its annex -- into a single copy.

    Two articles are the same when their normalized number, their digits and
    their heading + body (>= 0.95 Levenshtein similarity) all agree; see
    `_is_same_article`. Of each group the deepest copy is kept, the last one
    on a tie. A container emptied by that is removed, recursively upward,
    unless it was already empty as parsed.

    Never mutates `nodes` (the result holds a fresh tree) and is idempotent.
    """
    located = _locate_articles(nodes)

    removed: dict[int, DuplicateArticle] = {}
    for group in _group_same_article(located):
        if len(group) < 2:
            continue
        kept = _pick_kept(group)
        for item in group:
            if item is not kept:
                removed[item.order] = DuplicateArticle(
                    article=item.article,
                    location=item.location,
                    kept=kept.article,
                    kept_location=kept.location,
                )

    structure, emptied = _rebuild(nodes, set(removed), count())
    return DeduplicationResult(
        structure=structure,
        duplicates=[removed[order] for order in sorted(removed)],
        emptied=emptied,
    )


def describe_duplicates(duplicates: list[DuplicateArticle], limit: int = 10) -> str:
    """One-line summary of the first `limit` removed articles, for logging."""
    shown = [
        f"Artigo {duplicate.article.number} ({duplicate.location}) -> mantido em {duplicate.kept_location}"
        for duplicate in duplicates[:limit]
    ]
    remaining = len(duplicates) - len(shown)
    if remaining > 0:
        shown.append(f"... e mais {remaining}")
    return "; ".join(shown)
