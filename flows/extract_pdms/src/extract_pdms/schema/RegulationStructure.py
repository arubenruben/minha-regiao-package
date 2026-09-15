from pydantic import BaseModel


class ArticleSection(BaseModel):
    """One Artigo parsed out of a regulation's extracted text, together with
    the Parte/Título/Capítulo/Secção/Subsecção heading in force at the point
    it appears in the document.

    These headings form a nested legal-document hierarchy (Parte > Título >
    Capítulo > Secção > Subsecção > Artigo). Every level above Artigo is
    optional -- a regulation may use none of them, skip a level entirely
    (e.g. Capítulos with no Títulos above them), or stop nesting partway
    (e.g. Secções with no Subsecções). Whichever levels a given regulation
    does use are carried on every Artigo that falls under them; levels it
    doesn't use are left null.

    See extract_pdms.services.StructureParser.parse_structure for how this
    is derived from a document's raw extracted text.
    """

    parte: str | None = None
    parte_heading: str | None = None
    titulo: str | None = None
    titulo_heading: str | None = None
    capitulo: str | None = None
    capitulo_heading: str | None = None
    seccao: str | None = None
    seccao_heading: str | None = None
    subseccao: str | None = None
    subseccao_heading: str | None = None

    # e.g. "1.º", "10.º-A" -- always present, an Artigo is what anchors a row.
    artigo: str
    artigo_heading: str | None = None
    text: str
