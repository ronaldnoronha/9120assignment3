# OlympicsDb-Java #

This is the skeleton code for a 2-tier graphical client for the Olympics Database assignment scenario. It implements a front-end to the core functionality required for Assignment 3, with dummy implementations of the actual database interactions.

Snapshots are sometimes uploaded to elearning, but you can get the most up-to-date version from the [BitBucket repo](https://bitbucket.org/sit-info2120/olympicsdb-java).

If you want the Python equivalent check out [OlympicsDb-Java](https://bitbucket.org/sit-info2120/olympicsdb-python).


# Configuration #

1. Import into your preferred IDE such as Eclipse (contrary to myth, real coders use an IDE)
1. Edit the `olympicsdb.properties` file with your own database settings (see the comments in the file for suggestions)
1. Add the JDBC libraries in `lib/` to your build path
1. Run the main class OlympicsDBClient to test everything works (requires accessible database)

# Getting started with assignment #

There are a lot of files here, but you only need to edit one. `usyd.it.olympics.DatabaseBackend` is a single class containing all the database interaction code. Each of its public methods is called by the client (`usyd.it.olympics.OlympicsDBClient`) and the results are passed back to the various parts of the GUI. 

For S1 2017 the interface has had a drastic revision to the way data is passed back from the DatabaseBackend methods. Previously all data was packaged in objects of classes specific to each type of information (e.g one class per entity). Now everything is returned as a `HashMap<String, Object>` (a dictionary object, if you're more familar with Python), or `ArrayList<HashMap<String, Object>>` (a list of dictionaries) when multiple rows are returned together.

# Wow, you've written so much code so that I don't have to! #

You're welcome, but it's nice that you appreciate the discretionary effort. If you want to re-use some of this code feel free, but if you use significant chunks (whole methods or classes) please credit us (Bryn and Chris) as the original authors.