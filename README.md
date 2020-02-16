# Coffee.cup.guru Data Pipeline
I created this data pipeline to complement the logging capabilities of the [coffee.cup.guru](http://coffee.cup.guru/) app. Within the app, users are able to record their coffee brewing experiments. Each record contains the following data:
- recipe name
- datestamp for when the coffee was brewed
- a 1-5 rating that the user provides after trying out the coffee
- a text field where users can type additional notes
Since I would like to record additional details in order to more accurately describe the brewing method that was used and the results, I decided to save those in the notes field using a standardized format that can later be transformed into a pandas DataFrame:
```
Bean: <name of the coffee bean/blend> / Grind: <grind setting used> / Flavor <a short description of how the coffee tasted and how intense the flavor was> / Balance: <a short description of the body and clarity of the coffee>
```
However, after using the app for a few months, I found that it was difficult and tedious to review past records within the app to see what are the best ways to brew each type of coffee since that would require manually scrolling through the chronologically sorted list of log records.

Fortunately, the app allows users to export their coffee brewing log as a CSV file. The data pipeline consists of the following steps:
1. the CSV file is uploaded (using Android's "share" functionality) to [Box](https://www.box.com/)
2. the included Python script uses the Box API to download the most recent CSV log file and transforms it into a pandas DataFrame
3. the text data in the notes section is transformed into separate DataFrame columns
4. the DataFrame is saved locally as a new CSV file and as a SQLite staging table called `raw_logs`
5. a separate SQL script updates the main table called `coffee_logs`  new records found in `raw_logs`

With the data stored in a SQLite database, it's much easier to review and analyze past data in order to learn from past trial and error. For example, I can simply run the following query to see which of my coffee brewing recipes was best suited for brewing French roast Mocha coffee beans:
```
select bean,
	recipe,
    avg(score),
    count(score)
from coffee_logs
where bean like '%french mocha%'
group by recipe
```