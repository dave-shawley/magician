#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import alabaster

from magician import __version__, version_info


project = 'magician'
copyright = '2015, Dave Shawley'
version = __version__
release = '.'.join(str(x) for x in version_info[:2])

needs_sphinx = '1.0'
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'sphinx.ext.viewcode',
    'alabaster',
]
templates_path = []
source_suffix = '.rst'
source_encoding = 'utf-8-sig'
master_doc = 'index'
pygments_style = 'sphinx'
html_theme = 'alabaster'
html_theme_path = [alabaster.get_path()]
html_static_path = []
html_sidebars = {
    '**': [
        'about.html',
        'navigation.html',
        'searchbox.html',
    ],
}
html_theme_options = {
    'github_user': 'dave-shawley',
    'github_repo': 'magician',
    'description': 'Pulling rabbits out of an async hat',
    'github_banner': True,
    'travis_button': True,
}
exclude_patterns = []

intersphinx_mapping = {
    'python': ('http://docs.python.org/3/', None),
}
