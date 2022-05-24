# wikipedia-project
Contains all code for the wikipedia project.

# Code files

* ``extract-minor.py``: Extract the minor info from the downloaded wiki dump.
* ``process-enwiki-columns.py``: Process the content in the enwiki-columns folder to get the metrics we want. 

* TCM folder
    * ``create-title-look-up.ipynb``: Create the title lookup table. 
    * ``extract-TCM-pages.ipynb``: Extract TCM articles.
    * ``get-former-info.ipynb``: Get the former info (number of added, modified and removed text, wikilinks and urls) for TCM.
    * ``select-editors-based-on-significant-contributions.ipynb``: Get the number of significant contributions, and select editors based on it.
    * ``get-refs-former-and-bibliography-matrix.ipynb``: Get former info for references