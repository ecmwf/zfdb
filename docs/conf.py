import os
import sys

sys.path.insert(0, os.path.abspath('../../src'))

project = 'Documentation template'
author = 'ECMWF'
release = '0.1.0'
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'nbsphinx'
]
exclude_patterns = []

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

# html_theme = 'ecmwf'
# html_theme_path = ['_themes']
