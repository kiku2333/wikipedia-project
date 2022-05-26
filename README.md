# wikipedia-project
Contains all code for the wikipedia project.\
Provides results to [WhoColor-updates](https://github.com/kiku2333/WhoColor-updates).

# Code files

* ``extract-minor.py``: Extract the minor info from the downloaded wiki dump.
* ``process-enwiki-columns.py``: Process the content in the enwiki-columns folder to get the metrics we want. 

* TCM
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
    * cluster-infomap-0.2
        * ``get-editor-clusters.ipynb``: Get the cluster result using infomap.
        * ``get-group-graph-and-similarity.ipynb``: Get group revert graph and similarity matrix
        * ``compute-added-removed-words``: Get the top 100 added and removed words by revert graph and by revisions.
        * ``get-top-10-articles.ipynb``: Get the top 10 articles for each group by significant contributions and by mutual revert.
        * ``get-mutual-revert-info.ipynb``: Get mutual and minimum revert info for each pair of groups. 
        * ``TCM-top-ranked-group-sr-hl.ipynb``: Get the SR and HL for top ranked group pairs selected by their minimum revert.
    * ``editor-article-revision-matrix.ipynb``: Create the editor-article matrix to show which editor contributes to which article by how many times.

* article-clustering
    * ``TCM-article-similarity.ipynb``: Compute TCM article similarity using wikilinks in all history revisions. 
    * ``article-similarity-tf-idf.ipynb``: Use the sublinear tf scaling to compute the article similarity. 

* all-data
    * code used to further select editors based on their significant contributions. 
        * ``sig-contrib-process-text-all-data.py``: Process text for all wikipedia articles and talk pages for all editors and articles.
        * ``sig-contrib-compute-text-diff.py``: Compute the number of words added for each revision. 
        * ``sig-contrib-get-sig-contributoins.py``: Extract all significant contributions from all data. 
        * ``sig-contrib-stat.ipynb``: Select editors based on significant contributions. 
    * code used to select editors based on their c score and cc score.
        * ``split_titles.py``: Split the title list (which folder contains which article and talk) into articles and talks. 
        * ``user-contribution.py``: Get the editor contribution info (which editor contributes to which page with which revision ids). 
        * ``get-training-data.ipynb``: Get the training data used to create the regression model.
        * ``create-regression-model.ipynb``: Create the regression model for all data. 
        * ``all_crc.py``: Get article and editor c score.
        * ``sig-contrib-info.ipynb``: Get the number of significant contributions for each selected editor (from previous part), each page.
        * ``cc-score.py``: Get the cc score for selected editors.
        * ``cc-stat-to-select-editors.ipynb``: Use the editor cc score to select editors. 
    * code used to comppute editor profile.
        * ``get-added-removed-info.py``: Get added and removed text/wikilinks/urls info for each selected editor and articles. 
        * ``revert-all-data.py``: Get revert info for selected editors and articles. 
        * ``biblio-extract.refs.py``: Extract references for selected editors and articles.
        * ``find-add-modified-removed-text-wikilinks-url.py``: Get modified info based on added and removed content. 
        * ``later-info.py``: Get later info for selected editors and articles. 
        * ``contribution-info.py``: Get the number of contributions for selected editors and articles. 
        * ``added-modified-removed-info-with-editor-page-info.py``: Get the number of text/wikilinks/urls added, modified and removed for each selected editor and selected article. 
        * bibliography
            * ``find-ref-diff.py``: Get added and removed references for selected editors and articles.
            * ``ref-find-added-modified-removed.py``: Get the modified references and update the added and removed references. 
            * ``bibliography.py``: Get the bibliography info (which editor added which reference to which page). 
            * ``split-folders.ipynb``: Split and group the folders, 50 a group, then merge the results.
            * ``mege_result.py``: Merge the reference list for each splitted folders. 
            * ``update-ref-info-for-selected-articles.ipynb``: Updated the reference list for each splitted group.
            * ``final-merge.py``: Merge the results for splitted groups.
            * ``update-res.py``: For the page editor reference list computed before, update the reference id with matched ids.
            * ``get-matrix.ipynb``: Get the bibliography coupling for selected editors and new selected articles. 
            * ``ref-info-update.py``: For the ref info result (number of added, modified and removed references), add the editor and page info.
        * threshold-100
            * ``ref-added-removed.ipynb``: Merge the result in each folder and get the number of added, modified and removed references for each editor on new selected articles. 
            * ``amr-info-all-data.ipynb``: Merge the result in each folder and get the number of added, modified and removed text/wikilinks/urls for each editor on new selected articles.
            * ``later-info.ipynb``: Merge the result in each folder and get the number of ban, rule and arbcom for each editor on new selected articles.
            * ``revert-info.ipynb``: Merge the result in each folder and get the number of reverts for each editor on new selected articles.
            * ``contribution-info.py``: Get the number of contributions for selected editors on all articles.
            * ``merge-contribution-info.ipynb``: Merge the result in each folder and get the number of contributions for each editor on all articles. 
            * ``c-score.ipynb``: Extract the c score for selected editors. 
            * ``final-editor-profile.ipynb``: Merge the result computed above to get the final editor profile.
            * ``article-profile.ipynb``: Get the article profile. 
    * code used to compute the cluster results.
        * threshold-100
            * ``revert-graph.py``: Create the revert graph for each folder on selected editors and new selected articles. 
            * ``create-global-revert-graph.ipynb``: Merge the revert graph for each folder to get the global revert graph.
            * ``editor-similarity.ipynb``: Compute the editor cosine similarity using revert graph. 
            * ``cluster-by-infomap.ipynb``: Cluster the editors based on similarity using InfoMap, on different thresholds (0.0,0.1,...,1.0). 
            * ``editor-cluster-result.ipynb``: Use 0.4 as the threshold as the final cutoff, then extract the editor group info.
            * ``group-info.ipynb``: Get group revert graph, group similarity and group revert info.
            * ``words-added-removed-based-on-revert-graph.ipynb``: Merge the results for words added and removed based on revert graphs. Get the top 100 words added and removed, then sort them by the absolute value of their difference. 
            * ``revert-user-index-info-all-data.ipynb``: Merge the result to get words added and removed info for all reverts, with the title info and user info.
            * ``words-added-removed-all-revision.py``: Compute words added and removed info for all revisions (selected editors and new selected articles) for each group, each article.
            * ``words-added-removed-all-revision-final.ipynb``: Get words added and removed info for each group. Compute their differences then sort the words by the absolute value of difference. Then select the top 100 based on the absolute value. 
            * ``get-top-25-articles.ipynb``: Get top 25 articles for each group by significant contributions and by mutual revert. 
            * ``All-Data-mutual-revert-on-articles.ipynb``: For each pair of groups, get the articles they are fighting on and the number of mutual reverts. 
            * ``all-data-top-ranked-group-sr-hl.ipynb``: Compute group sr and hl for new selected articles for top ranked (and all) group pairs. 
            * ``all-data-top-ranked-group-sr-hl-by-topic-area.ipynb``: Computed group sr and hl for topic areas for top ranked (and all) group pairs.
            * ``SR-HL-for-article-and-editors.ipynb``: Get sr and hl for each selected article and editors. 
        * ``all-data-word-added-removed-info.py``: Compute words added and removed info based on revert graphs for each folder. 
        * ``sr-hl.py``: Computed sr and hl for old selected articles. 
    * code used to compute other info for all data.
        * threshold-100
            * ``co-authorship.py``: Compute the co-authorship and weighted co-authorship matrix for selected editors and new selected articles (for each folder).
            * ``co-authorship-final.ipynb``: Merge the co-authorship and the weighted co-authorship in each folder to get the final results.
            * ``editor-article-revision-matrix.ipynb``: Compute the editor-article-revision matrix, which is how many times each selected editor contributes to the selected articles and the corresponding talk pages. 
            * ``editor-profile-infomap.py``: Compute editor cluster result using infomap on different thresholds.
            * ``co-authorship-infomap.py``: Compute editor cluster result based on co-authorship.
            * ``bibliography-infomap.py``: Compute editor cluster result based on bibliography matrix.

* all-data-article-clustering
    * ``get-contribution-info-for-selected-editors.py``: Get contribution info for selected editors.
    * ``select-articles-based-on-threshold.ipynb``: Select the articles based on a threshold (100).
    * ``get-wikilinks.py``: Extract wikilinks for selected editors and new selected articles.
    * ``all-data-wikilinks.py``: Count the number of each wikilink for each article (from all revision history). 
    * ``similarity.ipynb``: Compute article similarity using the number of wikilinks. 
    * ``wikilinks-count.ipynb``: Count the number of times each wikilink occurs in all selected articles. 
    * ``wikilinks-count-revision-page-info.py``: Count the number of wikilinks for each article. The result is the same with all-data-wikilinks.py, but in a different format.
    * ``get-df-idf.ipynb``: Get df and idf for each wikilink. 
    * ``sub-linear-tf-scaling.py``: Get wf-idf for each term on each page.
    * ``similarity-wf-idf.py``: Compute the similarity based on wf-idf.