import pytest

from minha_regiao.gazette.RegulationStructure import Article
from minha_regiao.gazette.StructureParser import parse_structure

# Header lines as they come out of PDF text extraction: the amendment suffix
# of an "aditado" article ("10.º-A") is frequently split from the ordinal by
# whitespace, and some fonts lose the "º" glyph altogether.
_ADDED_ARTICLE_HEADERS = [
    "Artigo 10.º-A",
    "Artigo 10.º -A",
    "Artigo 10.º - A",
    "Artigo 10.º-a",
    "Artigo 10.o -A",
]


def _regulation(added_article_header: str) -> str:
    return "\n".join(
        [
            "Artigo 10.º",
            "Prazos",
            "1 — Os prazos contam-se em dias úteis.",
            "",
            added_article_header,
            "Prazos especiais",
            "1 — Os prazos especiais contam-se em dias seguidos.",
            "",
            "Artigo 11.º",
            "Entrada em vigor",
            "O presente regulamento entra em vigor no dia seguinte ao da sua publicação.",
        ]
    )


@pytest.mark.parametrize("header", _ADDED_ARTICLE_HEADERS)
def test_added_article_header_is_recognised_and_number_normalised(header: str) -> None:
    nodes = parse_structure(_regulation(header))

    assert all(isinstance(node, Article) for node in nodes)
    assert [node.number for node in nodes] == ["10.º", "10.º-A", "11.º"]


@pytest.mark.parametrize("header", _ADDED_ARTICLE_HEADERS)
def test_added_article_is_not_swallowed_by_previous_article(header: str) -> None:
    previous, added, _ = parse_structure(_regulation(header))

    assert previous.text == "1 — Os prazos contam-se em dias úteis."
    assert "Prazos especiais" not in previous.text
    assert "dias seguidos" not in previous.text

    assert added.heading == "Prazos especiais"
    assert added.text == "1 — Os prazos especiais contam-se em dias seguidos."


def test_plain_article_number_has_no_suffix() -> None:
    (article,) = parse_structure("Artigo 1.º\nObjeto\nO presente regulamento define as regras.")

    assert article.number == "1.º"


def test_prose_mentioning_an_added_article_is_not_a_header() -> None:
    text = "\n".join(
        [
            "Artigo 10.º",
            "Prazos",
            "Aplica-se o disposto no Artigo 10.º -A do presente regulamento.",
        ]
    )

    (article,) = parse_structure(text)

    assert article.number == "10.º"
    assert "Artigo 10.º -A" in article.text
