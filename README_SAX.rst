==========
timeseries
==========


To perform an iSAX search run both_SAX.sh


Description
===========

The prompts are relatively informative, but some additional notes are that you should first finish with all the windows stemming from the window that does not indicate that you should start with the other window (ie. start with the window starting with, "This is a brief introduction to the similarity search for time series"). Only load data from an existing database if one exists and the files should be ascii for the database and .npy for the time series. If unsure of how to format the files, run the code first without requesting to input a time series and after having the program generate some time series automatically, check the formatting of the files. A note on the threshold, it is effectively the maximum number of time series that will be returned by an internal search and that distances will be computed from.

Note
====

This project has been set up using PyScaffold 2.5.5. For details and usage
information on PyScaffold see http://pyscaffold.readthedocs.org/.
