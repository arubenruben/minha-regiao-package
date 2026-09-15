from pydantic import BaseModel


class Article(BaseModel):
    """One Artigo parsed out of a regulation's extracted text.

    Always a leaf: an Artigo never contains further nesting, only its own
    text. See extract_pdms.services.StructureParser.parse_structure for how
    this is derived from a document's raw extracted text.
    """

    # e.g. "1.º", "10.º-A"
    number: str
    heading: str | None = None
    text: str


class Subsection(BaseModel):
    """One Subsecção -- the deepest heading level a regulation can nest,
    holding only Artigos directly (there's nothing below it to nest
    further).
    """

    number: str
    heading: str | None = None
    articles: list[Article] = []


class Section(BaseModel):
    """One Secção. `articles` holds any Artigos that appear directly under
    this Secção, ahead of (or without) any Subsecção; `subsections` holds
    any Subsecções it's further divided into.
    """

    number: str
    heading: str | None = None
    subsections: list[Subsection] = []
    articles: list[Article] = []


class Chapter(BaseModel):
    """One Capítulo. `articles` holds any Artigos that appear directly
    under this Capítulo, ahead of (or without) any Secção; `sections` holds
    any Secções it's further divided into.
    """

    number: str
    heading: str | None = None
    sections: list[Section] = []
    articles: list[Article] = []


class Title(BaseModel):
    """One Título. `articles` holds any Artigos that appear directly under
    this Título, ahead of (or without) any Capítulo; `chapters` holds any
    Capítulos it's further divided into.
    """

    number: str
    heading: str | None = None
    chapters: list[Chapter] = []
    articles: list[Article] = []


class Part(BaseModel):
    """One Parte -- the outermost heading level. `articles` holds any
    Artigos that appear directly under this Parte, ahead of (or without)
    any Título; `titles` holds any Títulos it's further divided into.
    """

    number: str
    heading: str | None = None
    titles: list[Title] = []
    articles: list[Article] = []


# A regulation's structure is a nested Parte > Título > Capítulo > Secção >
# Subsecção > Artigo hierarchy. Every level above Artigo is optional -- a
# regulation may use none of them, skip a level entirely (e.g. Capítulos
# with no Títulos above them), or stop nesting partway (e.g. Secções with
# no Subsecções) -- so the top-level list of a document's structure can hold
# a mix of any of these node types, whichever level(s) that document
# actually opens at.
StructureNode = Part | Title | Chapter | Section | Subsection | Article
