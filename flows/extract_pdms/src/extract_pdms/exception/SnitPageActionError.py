class SnitPageActionError(RuntimeError):
    """Raised when the portal page doesn't produce a result for our
    page_action -- e.g. scrapling itself logs a page_action failure (seen in
    practice as the portal serving an error page with no __wc_csrfToken
    element, most likely anti-bot/rate-limiting) and swallows it, leaving
    the page in an unusable state instead of propagating the failure.
    """
