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
    * ``get-refs-former-and-bibliography-matrix.ipynb``: Get former info for references.
    * ``TCM-get-c-score.ipynb``: Get c score for articles and editors.
    * ``TCM-get-cc-score.ipynb``: Get cc score for editors.
    * ``get-editor-profile.ipynb``: Get the editor profile for TCM.
    * ``editor-profile-merge-pages.ipynb``: The editor profile computed before is per page per editor, in this step we want to merge the result for all pages (article and talk separately).
    * ``co-authorship.ipynb``: For selected editors, get the co-authorship and weighted co-authorship.
    * ``get-revert-graph.ipynb``: Get the revert graph for selected editors.
    * ``compute-sr-hl-ipynb``: Get sr and hl info using Wikiwho API
    * cluster-infomap-0.2 folder
        * ``get-editor-clusters.ipynb``: Get the cluster result using infomap.
        * ``get-group-graph-and-similarity.ipynb``: Get group revert graph and similarity matrix
        * ``compute-added-removed-words``: Get the top 100 added and removed words by revert graph and by revisions.
        * ``get-top-10-articles.ipynb``: Get the top 10 articles for each group by significant contributions and by mutual revert.
        * ``get-mutual-revert-info.ipynb`: Get mutual and minimum revert info for each pair of groups. 
        * ``TCM-top-ranked-group-sr-hl.ipynb`: Get the SR and HL for top ranked group pairs selected by their minimum revert.
    * ``editor-article-revision-matrix.ipynb``: Create the editor-article matrix to show which editor contributes to which article by how many times.


