poetry config pypi-token.pypi 



rm -Rf ./dist
poetry publish --build -vv


mkdocs gh-deploy