### NCA Specify Migrator

Scripts used for importing specimen data into Specify for the National Collection of Arachnida, so that we don't have to split large files into chunks. 

This was done as an alternative to using the Specify Workbench, due to the size of the original dataset (> 80 000 records). 

See `importdata.py`, and the supporing `check_taxa_or_geography.py` for entry points. Expects taxonomy to have been imported via the workbench first. 

`update_names.py` and `updatesynonyms.py` were attempts to include data from the World Spider catalog (see [wsc-species-extractor](https://github.com/NSCF/wsc-species-extractor) for the attempt to scrape the names from their site), but this was abandoned.

#### note that the official names of countries list comes from https://worldpopulationreview.com/country-rankings/official-names-of-countries, accessed 2024-06-17