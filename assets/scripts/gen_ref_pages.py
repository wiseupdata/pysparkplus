"""Generate the code package pages and navigation."""

PACKAGE_NAME = "pysparkplus"

from pathlib import Path

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()

for path in sorted(Path(PACKAGE_NAME).rglob("*.py")):
    module_path = path.relative_to(PACKAGE_NAME).with_suffix("")
    doc_path = path.relative_to(PACKAGE_NAME).with_suffix(".md")
    full_doc_path = Path(f"{PACKAGE_NAME}", doc_path)

    parts = tuple(module_path.parts)

    if parts[-1] == "__init__":
        parts = parts[:-1]
        doc_path = doc_path.with_name("index.md")
        full_doc_path = full_doc_path.with_name("index.md")
        if len(parts) == 0:
            # parts = ("__init__")
            continue
    elif parts[-1] == "__main__":
        continue

    nav[parts] = doc_path.as_posix()

    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        ident = ".".join(parts)
        fd.write(f"::: {PACKAGE_NAME}.{ident}")

    mkdocs_gen_files.set_edit_path(full_doc_path, path)

with mkdocs_gen_files.open(f"{PACKAGE_NAME}/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
