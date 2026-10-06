import pytest

from minha_regiao.gazette.RegulationStructure import (
    Article,
    Chapter,
    Part,
    Section,
    StructureNode,
    Subsection,
    Title,
)
from minha_regiao.gazette.StructureDeduplicator import (
    deduplicate_structure,
    describe_duplicates,
)
from minha_regiao.gazette.StructureParser import parse_structure

_OCCUPATION_TEXT = (
    "1 — A ocupação do espaço público por esplanadas fica sujeita a licença municipal.\n"
    "2 — O pedido é instruído com planta de localização à escala 1:1000.\n"
    "3 — A licença é válida por 12 meses."
)


def _article(
    number: str = "58.º",
    heading: str | None = "Ocupação do espaço público",
    text: str = _OCCUPATION_TEXT,
) -> Article:
    return Article(number=number, heading=heading, text=text)


def _numbers(articles: list[Article]) -> list[str]:
    return [article.number for article in articles]


def _all_articles(nodes: list[StructureNode]) -> list[Article]:
    """Every Artigo in the tree, in document order (own articles first)."""
    found: list[Article] = []
    for node in nodes:
        if isinstance(node, Article):
            found.append(node)
            continue
        found.extend(node.articles)
        for field in ("titles", "chapters", "sections", "subsections"):
            found.extend(_all_articles(getattr(node, field, [])))
    return found


def test_root_copy_is_removed_in_favour_of_the_copy_inside_a_chapter_and_section() -> None:
    cited = _article()
    republished = _article()
    structure = [
        cited,
        Chapter(
            number="III",
            sections=[Section(number="IV", articles=[republished])],
        ),
    ]

    result = deduplicate_structure(structure)

    assert len(result.structure) == 1
    chapter = result.structure[0]
    assert isinstance(chapter, Chapter)
    assert chapter.sections[0].articles == [republished]
    assert len(result.duplicates) == 1
    duplicate = result.duplicates[0]
    assert duplicate.article == cited
    assert duplicate.location == "raiz"
    assert duplicate.kept == republished
    assert duplicate.kept_location == "Capítulo III › Secção IV"
    assert result.emptied == []


def test_deeper_copy_wins_regardless_of_order() -> None:
    cited = _article()
    republished = _article()
    structure = [
        Chapter(number="III", sections=[Section(number="IV", articles=[republished])]),
        cited,
    ]

    result = deduplicate_structure(structure)

    assert len(result.structure) == 1
    assert len(_all_articles(result.structure)) == 1
    assert result.duplicates[0].location == "raiz"
    assert result.duplicates[0].kept_location == "Capítulo III › Secção IV"


def test_at_equal_depth_the_last_copy_is_kept() -> None:
    first = _article()
    second = _article()
    structure = [
        Chapter(number="I", articles=[first]),
        Chapter(number="II", articles=[second]),
    ]

    result = deduplicate_structure(structure)

    assert result.duplicates[0].location == "Capítulo I"
    assert result.duplicates[0].kept_location == "Capítulo II"
    # Capítulo I only held the duplicate, so it goes away with it.
    assert [node.number for node in result.structure] == ["II"]


def test_root_level_copies_keep_the_last_one() -> None:
    result = deduplicate_structure([_article(), _article()])

    assert len(result.structure) == 1
    assert result.duplicates[0].location == "raiz"
    assert result.duplicates[0].kept_location == "raiz"


def test_container_left_empty_is_removed_along_with_its_emptied_ancestors() -> None:
    cited = _article()
    republished = _article()
    structure = [
        Part(
            number="A",
            titles=[
                Title(
                    number="II",
                    chapters=[Chapter(number="III", sections=[Section(number="IV", articles=[cited])])],
                )
            ],
        ),
        Part(
            number="B",
            titles=[
                Title(
                    number="I",
                    chapters=[
                        Chapter(
                            number="I",
                            sections=[Section(number="I", subsections=[Subsection(number="I", articles=[republished])])],
                        )
                    ],
                )
            ],
        ),
    ]

    result = deduplicate_structure(structure)

    assert [node.number for node in result.structure] == ["B"]
    assert [(type(node).__name__, node.number) for node in result.emptied] == [
        ("Part", "A"),
        ("Title", "II"),
        ("Chapter", "III"),
        ("Section", "IV"),
    ]
    assert all(not node.articles for node in result.emptied)


def test_container_that_only_loses_some_articles_is_kept() -> None:
    cited = _article()
    other = _article(number="59.º", heading="Outro", text="Texto distinto do artigo 59.")
    republished = _article()
    structure = [
        Chapter(number="I", articles=[other, cited]),
        Chapter(number="II", articles=[republished]),
    ]

    result = deduplicate_structure(structure)

    assert [node.number for node in result.structure] == ["I", "II"]
    first_chapter = result.structure[0]
    assert isinstance(first_chapter, Chapter)
    assert first_chapter.articles == [other]
    assert result.emptied == []


def test_container_with_a_surviving_child_container_is_kept() -> None:
    cited = _article()
    surviving = _article(number="1.º", heading="Objeto", text="O presente regulamento define as regras.")
    republished = _article()
    structure = [
        Chapter(
            number="I",
            articles=[cited],
            sections=[Section(number="I", articles=[surviving])],
        ),
        Chapter(number="II", articles=[republished]),
    ]

    result = deduplicate_structure(structure)

    first_chapter = result.structure[0]
    assert isinstance(first_chapter, Chapter)
    assert first_chapter.articles == []
    assert first_chapter.sections[0].articles == [surviving]


def test_container_that_was_already_empty_is_kept_as_parsed() -> None:
    cited = _article()
    republished = _article()
    structure = [
        Chapter(number="I", heading="Já vazio"),
        Chapter(number="II", sections=[Section(number="I")], articles=[cited]),
        Chapter(number="III", articles=[republished]),
    ]

    result = deduplicate_structure(structure)

    numbers = [node.number for node in result.structure]
    assert numbers == ["I", "II", "III"]
    # Capítulo II lost its only article but still holds an (originally
    # empty) Secção, so it isn't empty either.
    assert result.emptied == []


def test_same_number_with_different_heading_and_text_are_different_articles() -> None:
    first = _article(heading="Ocupação do espaço público", text=_OCCUPATION_TEXT)
    second = _article(
        heading="Publicidade",
        text="1 — A afixação de publicidade depende de licença.\n2 — O pedido é apreciado em 30 dias.",
    )

    result = deduplicate_structure([first, Chapter(number="I", articles=[second])])

    assert len(_all_articles(result.structure)) == 2
    assert result.duplicates == []


@pytest.mark.parametrize("amounts", [("12,50 €", "15,00 €"), ("100", "1000")])
def test_same_number_and_heading_with_different_amounts_are_different_articles(amounts: tuple[str, str]) -> None:
    # The "Tabela" annex of a fee regulation repeats an article's number and
    # heading for every fee, with a different amount each time.
    first, second = (
        _article(heading="Licença", text=f"Pela emissão da licença é devida a taxa de {amount}.")
        for amount in amounts
    )

    result = deduplicate_structure([Chapter(number="I", articles=[first]), Chapter(number="II", articles=[second])])

    assert result.duplicates == []
    assert len(_all_articles(result.structure)) == 2


def test_many_tabela_copies_with_distinct_amounts_are_all_kept() -> None:
    copies = [
        _article(number="3.º", heading="Taxas", text=f"Ocupação de via pública, por metro quadrado: {value},00 €")
        for value in range(1, 31)
    ]

    result = deduplicate_structure([Chapter(number="Anexo", articles=copies)])

    assert result.duplicates == []
    assert len(_all_articles(result.structure)) == 30


def test_added_article_is_never_merged_with_its_neighbour() -> None:
    base = _article(number="10.º")
    added = _article(number="10.º-A")

    result = deduplicate_structure([base, added])

    assert _numbers(_all_articles(result.structure)) == ["10.º", "10.º-A"]
    assert result.duplicates == []


@pytest.mark.parametrize("revoked_number", ["10.º-A", "10.º - A", "10.º -a"])
def test_added_article_number_spelling_is_normalised_before_comparing(revoked_number: str) -> None:
    result = deduplicate_structure([_article(number="10.º-A"), _article(number=revoked_number)])

    assert len(_all_articles(result.structure)) == 1


def test_two_revoked_articles_without_a_body_and_with_the_same_number_are_one() -> None:
    first = Article(number="7.º", heading="(Revogado.)", text="")
    second = Article(number="7.º", heading="(Revogado.)", text="")

    result = deduplicate_structure([first, Chapter(number="II", articles=[second])])

    assert len(_all_articles(result.structure)) == 1
    assert result.duplicates[0].location == "raiz"


def test_articles_with_neither_heading_nor_body_and_the_same_number_are_one() -> None:
    result = deduplicate_structure([Article(number="7.º", text=""), Article(number="7.º", text="")])

    assert len(_all_articles(result.structure)) == 1


def test_revoked_articles_with_different_numbers_are_different() -> None:
    result = deduplicate_structure(
        [
            Article(number="7.º", heading="(Revogado.)", text=""),
            Article(number="8.º", heading="(Revogado.)", text=""),
        ]
    )

    assert len(_all_articles(result.structure)) == 2


def test_new_layout_page_header_in_the_middle_of_the_text_is_ignored() -> None:
    plain = _article()
    split_across_pages = _article(
        text=_OCCUPATION_TEXT.replace(
            "licença municipal.\n",
            "licença municipal.N.º 5 7 de janeiro de 2022 Pág. 725\n",
        )
    )

    result = deduplicate_structure([split_across_pages, Chapter(number="I", articles=[plain])])

    assert len(_all_articles(result.structure)) == 1


def test_new_layout_page_header_with_the_page_marker_in_front_is_ignored() -> None:
    plain = _article()
    split_across_pages = _article(
        text=_OCCUPATION_TEXT.replace(
            "licença municipal.\n",
            "licença municipal.\nPág. 726 N.º 5 7 de janeiro de 2022\n",
        )
    )

    result = deduplicate_structure([split_across_pages, Chapter(number="I", articles=[plain])])

    assert len(_all_articles(result.structure)) == 1


@pytest.mark.parametrize(
    "header",
    [
        "Diário da República, 2.ª série — N.º 82 — 28 de Abril de 2009",
        "Diário da República, 2.ª série — N.º 82 — 28 de Abril de 2009 16 704",
        "16 705 Diário da República, 2.ª série — N.º 82 — 28 de Abril de 2009",
        "Diário da República, 1.ª série-A — N.º 82 — 28 de Abril de 2009",
    ],
)
def test_old_layout_page_header_in_the_middle_of_the_text_is_ignored(header: str) -> None:
    plain = _article()
    split_across_pages = _article(text=_OCCUPATION_TEXT.replace("municipal.\n", f"municipal.\n{header}\n"))

    result = deduplicate_structure([split_across_pages, Chapter(number="I", articles=[plain])])

    assert len(_all_articles(result.structure)) == 1


def test_page_header_digits_do_not_count_towards_the_digits_check() -> None:
    # Different pages carry different issue numbers/dates/page numbers; if
    # those leaked into the digits comparison the same article would look
    # like two different ones.
    first = _article(text=_OCCUPATION_TEXT.replace("municipal.\n", "municipal.N.º 5 7 de janeiro de 2022 Pág. 725\n"))
    second = _article(text=_OCCUPATION_TEXT.replace("municipal.\n", "municipal.N.º 9 14 de março de 2023 Pág. 1203\n"))

    result = deduplicate_structure([first, Chapter(number="I", articles=[second])])

    assert len(_all_articles(result.structure)) == 1


def test_a_real_number_that_resembles_a_header_fragment_still_counts() -> None:
    first = _article(text="1 — O prazo é de 30 dias.")
    second = _article(text="1 — O prazo é de 60 dias.")

    result = deduplicate_structure([first, Chapter(number="I", articles=[second])])

    assert len(_all_articles(result.structure)) == 2


def test_line_break_hyphenation_is_ignored() -> None:
    # Deliberately short and heavily wrapped: in a long text a few split
    # words stay under the similarity threshold's radar anyway, which would
    # let this pass even with the hyphenation left in.
    wrapped = Article(number="9.º", text="Re-\nvo-\nga-\ndo")
    plain = Article(number="9.º", text="Revogado")

    result = deduplicate_structure([wrapped, Chapter(number="I", articles=[plain])])

    assert len(_all_articles(result.structure)) == 1


def test_hyphenation_across_a_page_break_is_ignored() -> None:
    wrapped = Article(number="9.º", text="Re-N.º 5 7 de janeiro de 2022 Pág. 725\nvo-\ngado")
    plain = Article(number="9.º", text="Revogado")

    result = deduplicate_structure([wrapped, Chapter(number="I", articles=[plain])])

    assert len(_all_articles(result.structure)) == 1


def test_accents_case_and_punctuation_are_ignored() -> None:
    plain = _article()
    shouting = _article(
        heading="OCUPAÇÃO DO ESPAÇO PÚBLICO.",
        text=_OCCUPATION_TEXT.upper().replace("—", "-").replace("PÚBLICO", "PUBLICO"),
    )
    no_diacritics = _article(
        number="58.o",
        heading="Ocupacao do espaco publico",
        text=_OCCUPATION_TEXT.replace("ç", "c").replace("ã", "a").replace("ú", "u").replace("é", "e"),
    )

    result = deduplicate_structure([plain, shouting, Chapter(number="I", articles=[no_diacritics])])

    assert len(_all_articles(result.structure)) == 1
    assert len(result.duplicates) == 2


def test_text_with_one_stray_character_is_still_the_same_article() -> None:
    noisy = _article(text=_OCCUPATION_TEXT.replace("licença", "licençaa"))

    result = deduplicate_structure([noisy, Chapter(number="I", articles=[_article()])])

    assert len(_all_articles(result.structure)) == 1


def test_substantially_different_text_with_identical_digits_is_not_merged() -> None:
    different = _article(
        text=(
            "1 — A instalação de toldos e guarda-ventos é proibida nas zonas históricas.\n"
            "2 — Excetuam-se os casos previstos em regulamento próprio.\n"
            "3 — A proibição vigora por 12 meses a contar de 1:1000."
        )
    )

    result = deduplicate_structure([different, Chapter(number="I", articles=[_article()])])

    assert len(_all_articles(result.structure)) == 2


def test_heading_split_differently_between_copies_is_still_the_same_article() -> None:
    # A long heading wraps in the PDF and the parser only takes its first line;
    # where the break falls depends on each copy's column width.
    narrow = Article(
        number="58.º",
        heading="Ocupação do espaço público por",
        text="esplanadas e similares\n" + _OCCUPATION_TEXT,
    )
    wide = Article(
        number="58.º",
        heading="Ocupação do espaço público por esplanadas e similares",
        text=_OCCUPATION_TEXT,
    )

    result = deduplicate_structure([narrow, Chapter(number="I", articles=[wide])])

    assert len(_all_articles(result.structure)) == 1
    assert result.duplicates[0].article == narrow


def test_duplicates_are_reported_in_document_order() -> None:
    a_cited, b_cited = _article(number="1.º", text="Texto A."), _article(number="2.º", text="Texto B.")
    structure = [
        a_cited,
        b_cited,
        Chapter(
            number="I",
            articles=[_article(number="2.º", text="Texto B."), _article(number="1.º", text="Texto A.")],
        ),
    ]

    result = deduplicate_structure(structure)

    assert [duplicate.article.number for duplicate in result.duplicates] == ["1.º", "2.º"]


def test_the_same_article_instance_placed_twice_is_deduplicated_by_position() -> None:
    shared = _article()

    result = deduplicate_structure([shared, Chapter(number="I", articles=[shared])])

    assert len(result.structure) == 1
    assert isinstance(result.structure[0], Chapter)
    assert len(result.structure[0].articles) == 1
    assert len(result.duplicates) == 1


def test_a_synthesised_container_without_number_still_gets_a_breadcrumb() -> None:
    cited = _article()
    structure = [
        Chapter(number="I", sections=[Section(number="", subsections=[Subsection(number="I", articles=[_article()])])]),
        cited,
    ]

    result = deduplicate_structure(structure)

    assert result.duplicates[0].kept_location == "Capítulo I › Secção › Subsecção I"


def test_deduplicating_twice_changes_nothing_and_the_input_is_not_modified() -> None:
    cited = _article()
    structure: list[StructureNode] = [
        cited,
        Chapter(
            number="III",
            sections=[Section(number="IV", articles=[_article(), _article(number="59.º", heading="Outro", text="Outro texto.")])],
        ),
        Chapter(number="IV", articles=[_article(number="60.º", heading="Taxa", text="Taxa de 10 euros.")]),
        Chapter(number="V", articles=[_article(number="60.º", heading="Taxa", text="Taxa de 20 euros.")]),
    ]
    snapshot = [node.model_copy(deep=True) for node in structure]

    first = deduplicate_structure(structure)
    second = deduplicate_structure(first.structure)

    assert structure == snapshot
    assert len(first.duplicates) == 1
    assert second.structure == first.structure
    assert second.duplicates == []
    assert second.emptied == []


def test_result_tree_does_not_share_nodes_with_the_input() -> None:
    chapter = Chapter(number="I", articles=[_article()])

    result = deduplicate_structure([chapter])

    assert result.structure == [chapter]
    assert result.structure[0] is not chapter
    result.structure[0].articles.append(_article(number="1.º", text="x"))
    assert len(chapter.articles) == 1


def test_empty_structure() -> None:
    result = deduplicate_structure([])

    assert result.structure == []
    assert result.duplicates == []
    assert result.emptied == []


def test_describe_duplicates_lists_at_most_the_first_ten() -> None:
    structure: list[StructureNode] = [_article(number=f"{n}.º", text=f"Texto do artigo {n}.") for n in range(1, 13)]
    structure.append(
        Chapter(number="I", articles=[_article(number=f"{n}.º", text=f"Texto do artigo {n}.") for n in range(1, 13)])
    )

    result = deduplicate_structure(structure)
    description = describe_duplicates(result.duplicates)

    assert len(result.duplicates) == 12
    assert "Artigo 10.º (raiz) -> mantido em Capítulo I" in description
    assert "Artigo 11.º" not in description
    assert description.endswith("... e mais 2")


def test_parse_structure_returns_an_amended_article_once_when_cited_and_republished() -> None:
    text = "\n".join(
        [
            "Aviso n.º 123/2022",
            "Alteração ao Regulamento Municipal de Taxas",
            "Artigo 58.º",
            "Ocupação do espaço público",
            "1 — A ocupação do espaço público por esplanadas fica sujeita a licença municipal.",
            "2 — O pedido é instruído com planta de localização à escala 1:1000.",
            "3 — A licença é válida por 12 meses.",
            "CAPÍTULO I",
            "Disposições gerais",
            "Artigo 1.º",
            "Objeto",
            "O presente regulamento define as taxas municipais.",
            "CAPÍTULO III",
            "Espaço público",
            "SECÇÃO IV",
            "Esplanadas",
            "Artigo 58.º",
            "Ocupação do espaço público",
            "1 — A ocupação do espaço público por esplanadas fica sujeita a licença municipal.",
            "2 — O pedido é instruído com planta de localização à escala 1:1000.",
            "3 — A licença é válida por 12 meses.",
        ]
    )

    nodes = parse_structure(text)

    assert all(isinstance(node, Chapter) for node in nodes)
    assert [node.number for node in nodes] == ["I", "III"]
    articles = _all_articles(nodes)
    assert _numbers(articles) == ["1.º", "58.º"]
    chapter_three = nodes[1]
    assert isinstance(chapter_three, Chapter)
    assert chapter_three.sections[0].articles[0].number == "58.º"


def test_parse_structure_keeps_both_when_the_republished_copy_differs() -> None:
    text = "\n".join(
        [
            "Artigo 5.º",
            "Taxa",
            "A taxa é de 10 euros.",
            "CAPÍTULO I",
            "Artigo 5.º",
            "Taxa",
            "A taxa é de 12 euros.",
        ]
    )

    nodes = parse_structure(text)

    assert _numbers(_all_articles(nodes)) == ["5.º", "5.º"]
