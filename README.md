# Covid Jupyter Notebook

## Overview
The New York Times publishes their Covid-19 dataset daily to github here:
  https://github.com/nytimes/covid-19-data.git

This dataset is included as a sub-module to this repo.

This notebook was inspired by the Minute Physics YouTube video:

  https://www.youtube.com/watch?v=54XLXg4fYsc

and the plots created by Aatish Bhatia on his site:

  https://aatishb.com/covidtrends/

I wanted to recreate Aatish's plots at the county level as well as explore
the velocity of the spread by looking at the time it takes for the total
number of cases to increase 10x.

## Steps To Use Nhis Notebook
1. The dataset should be created for you. If not, check it out in to the
same directory were the `.ipynb` file is located.
1. Install the python package dependencies: `pip install -r requirements.txt`
1. Start your jupyter notebook server `jupyter notebook` and load the file.

## Feedback
1. Please generate pull requests if you make improvements to the notebook.
