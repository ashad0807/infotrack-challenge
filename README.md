# infotrack-challenge

***ASIC Data Handler***

This solution reads data from ASIC by reading a xls file in S3 as the "source".

Once read the XLS file is converted to a Pandas Dataframe and then grouped using Firstname, MiddleName, LastName, DateOfBirth, PlaceOfBirth fields. The idea is that a person with all these fields can be uniquely identifiable. After grouping based on these fields I aggregate over the grouped data and then apply last to get the last update for other columns for that date.

UUID: I calcluate and apply a unqiue ID column to each row using the UUID function in python that generates a unique key for each grouped user. for eg: 

 FirstName MiddleName    LastName  ...  Position        ACN                                  uuid
0        Ada   Isabella    Lovelace  ...  Director  951752186  8300e1b5-7159-4457-827d-26b29a15eeed
1   Annabela  Kimberley  Neugeboren  ...  Director  882699605  29def17e-3e28-4671-8c32-d3e557b7d14c
2       Bond      James        Bond  ...  Director  677662666  f7c16397-7af1-4acd-bdae-9ef9360b5a9d

Finally when we aggregate over all the rows in the XLS file we go ahead and insert them into AWS RedShift tables which we use as a Datastore:



***Improvements TBD***

The ideal stage for this solution would be to create a staging table in Redshift and merge updated rows into existing rows of the target table there by performing an UPSERT of rows, there by every new row updated preferably using a messaging system as events would be captured and upserted in the target table. 



