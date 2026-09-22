"""Shared validation and rendering for typed fabric-level ANTA exclusions."""


def build_avd_catalogs_filters(anta_enabled: bool | None, test_names: object) -> list[dict[str, list[str]]]:
    """Build the global PyAVD ANTA skip-test filter configured on a fabric.

    The Infrahub schema deliberately exposes a simple list of ANTA test names.
    PyAVD expects a list of filter mappings, so this helper owns the conversion
    used by both hostvar and ANTA catalog generation.
    """
    if not anta_enabled or test_names in (None, []):
        return []
    if not isinstance(test_names, list):
        msg = "avd_catalogs_filters must be a list of non-empty ANTA test names"
        raise TypeError(msg)

    normalized: list[str] = []
    for test_name in test_names:
        if not isinstance(test_name, str) or not test_name.strip():
            msg = "avd_catalogs_filters must contain only non-empty ANTA test names"
            raise ValueError(msg)
        stripped_name = test_name.strip()
        if stripped_name not in normalized:
            normalized.append(stripped_name)

    return [{"skip_tests": normalized}] if normalized else []
