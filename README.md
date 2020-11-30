# ETL Challenge 8
## Summary
Amazing Prime wants to keep data updated on a daily basis and We have to create an automated pipeline that takes in new data, performs the appropriate transformations, and loads the data into existing tables.We will use Wikipedia data, Kaggle metadata, and the MovieLens rating data and perform the ETL process by adding the data to a PostgreSQL database.

## Results
### Deliverable 1 :ETL Function to Read Three Data Files
In this deliverable we will use Python, Pandas, the ETL process, and code refactoring to write a function that reads in the three data files and creates three separate DataFrames.The following steps will read the three data files.
1. Create a function called movie_data_etl and inside this function we are reading movies_metadata.csv filed data to kaggle_metadata,ratings.csv file data to ratings and wikipedia-movies.json to wiki_movies_raw.And finally we read the wiki_movies_raw to a Pandas DataFrame called Wikipedia_data.
Here is the code snipet for the function created
```
# 1. Create a function that takes in three arguments;
# Wikipedia data, Kaggle metadata, and MovieLens rating data (from Kaggle)
def movie_data_etl(Wikipedia_data, kaggle_metadata, ratings):

#def extract_transform_load():
    # 2. Read in the kaggle metadata and MovieLens ratings CSV files as Pandas DataFrames.
    kaggle_metadata = pd.read_csv(f'{file_dir}/movies_metadata.csv', low_memory=False)
    ratings = pd.read_csv(f'{file_dir}/ratings.csv')

    # 3. Open the read the Wikipedia data JSON file.
    with open(f'{file_dir}/wikipedia-movies.json', mode='r') as file:
        wiki_movies_raw = json.load(file)
    
    # 4. Read in the raw wiki movie data as a Pandas DataFrame.
    Wikipedia_data = pd.DataFrame(wiki_movies_raw)
    
    # 5. Return the three DataFrames
    return Wikipedia_data, kaggle_metadata, ratings
```

2. We create the path to the file directory and variables for the three files.
a. file_dir = 'C://Users/aksha/Desktop/Class folder/ETL/Movies-ETL'
b. wiki_file = f'{file_dir}/wikipedia.movies.json'
c. kaggle_file = f'{file_dir}/movies_metadata.csv'
d. ratings_file = f'{file_dir}/ratings.csv'

3. Call the function movie_data_etl, now we have the data in the three variables wiki_file, kaggle_file and ratings_file as Pandas DataFrame.

4. Assining these variable to wiki_movies_df = wiki_file,
kaggle_metadata = kaggle_file and ratings = ratings_file

5. Below are the links to screenshots for the output, and all the image are available in the reference image folder. 

[wiki_movies_df][1]

[kaggle_metadata][2]

[ratings][3]

### Deliverable 2 : Extract and Transform the Wikipedia Data
In this deliverable we will use Python, Pandas, the ETL process, and code refactoring to extract and transform the Wikipedia data so we can merge it with the Kaggle metadata.The following steps will extract and transform the Wikipedia data.

1 . Create a function called clean_movie(), inside this function we will Make an empty dict alt_titles = {} to hold all of the alternative titles, using list comprehensions Loop through a list of all alternative title keys,Check if the current key exists in the movie object,If so, remove the key-value pair and add to the alternative titles dictionary,After looping through every key, add the alternative titles dict to the movie object.

2. In the next function change_column_name we will change the column names.

3. The next function named movie_data_etl transforms the data as required.
 Below are the steps involved
 
    a . Read the raw data into wiki_movies_df from wikipedia-movies.json , kaggle_metadata from movies_metadata.csv, ratings from ratings.csv.
  
    b. list comprehension to filter out TV shows with below code snippet.
  
```
      wiki_movies = [movie for movie in wiki_movies_raw
                  if ('Director' in movie or 'Directed by' in movie)
                       and 'imdb_link' in movie
                       and 'No. of episodes' not in movie]
    
```
      Try-except block to catch errors while extracting the IMDb ID using a regular expression string and dropping any imdb_id duplicates. If there is an error, capture and          print the exceptionwith the below code snippet.
```
      try:
              wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
              print(len(wiki_movies_df))
              wiki_movies_df.head()
          except :
              print ('imdb_link format is incorrect')
```
  c.  Using list comprehension to keep columns with non-null values.
      The below code is written for the the columns box office,budget, release date and running time.
```
      wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
      wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

```
  d . The non-null box office data is converted to string values using the lambda and join functions.
```
       box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
```
  e. A regular expression is used to match the six elements of "form_one" of the box office data.
```
     form_one = r'\$\d+\.?\d*\s*[mb]illion'
```

  f. A regular expression is used to match the three elements of "form_two" of the box office data.
```
      form_two = r'\$\d{1,3}(?:,\d{3})+'
```
  g. The following columns are cleaned in the Wikipedia DataFrame.
      Refer the links for the code screenshots.

[The box office column][4]

[The budget column][5]

[The release date column][6]

[The running time column][7]


4. The cleaned Wikipedia data is converted to a Pandas DataFrame, and the DataFrame is displayed in the ETL_clean_wiki_movies.ipynb file.
[Cleaned Wikipedia data][8]


### Deliverable 3 : Extract and Transform the Kaggle Data
In this deliverable we will use Python, Pandas, the ETL process, and code refactoring to extract and transform the Kaggle metadata and MovieLens rating data, then convert the transformed data into separate DataFrames. Then we will merge the Kaggle metadata DataFrame with the Wikipedia movies DataFrame to create the movies_df DataFrame and finally merge the MovieLens rating data DataFrame with the movies_df DataFrame to create the movies_with_ratings_df.
#### A. The extraction and transformation of the Kaggle metadata using the ETL function does the following:
1. Clean the Kaggle data by removing the bad data,converting data to numeric datatypes for some columns and convert release date to datetime.
[Cleaned Kaggle data][9]
2. Merge the Wikipedia and Kaggle DataFrames into movies_df. Below is the code snippet
```
movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])

```
3. The movies_df is then further cleaned using the below steps
a. Dropping unnecessary columns from movies_df
b. A function fill_missing_kaggle_data() is used to fill in the missing Kaggle data.
c. The movies_df DataFrame is filtered to keep specific columns.
d. The movies_df DataFrame columns are renamed.
The following link is the snapshot for the code.
[Cleaned_movies_df][10]

#### B. The extraction and transformation of the MovieLens ratings data using the ETL function does the following:

1. The ratings counts are cleaned using list comprehension to pivot the data so that movieId is the index, the columns will be all the rating values,and the rows will be the counts for each rating value.
2. The movies_df DataFrame is merged with the cleaned ratings DataFrame to create the movies_with_ratings_df DataFrame.
3. The empty values in the movies_with_ratings_df DataFrame are filled with “0”

The following link is the snapshot for the code.
[movies_with_ratings_df_code][11]

4. Below is a snapshot for the movies_with_ratings_df and the movies_df DataFrames.
[movies_with_ratings_df][12]
[movies_df][13]


### Deliverable 4 : Create the Movie Database
In this deliverable we will use Python, Pandas, the ETL process, code refactoring, and PostgreSQL to add the movies_df DataFrame and MovieLens rating CSV data to a SQL database.

1. The data from the movies_df DataFrame replaces the current data in the movies table in the SQL database, as determined by the movies_query.png.
[movies_query][14]

2. The data from the MovieLens rating CSV file is added to the ratings table in the SQL database, as determined by the ratings_query.png
[ratings_query][15]

3. The elapsed time to add the data to the database is displayed in the ETL_create_database.ipynb
[Elapsed time][16]

[1]:https://github.com/Akshaya-Kamble/Movies-ETL/blob/main/Reference%20Images/1.PNG
[2]:https://github.com/Akshaya-Kamble/Movies-ETL/blob/main/Reference%20Images/2.PNG
[3]:https://github.com/Akshaya-Kamble/Movies-ETL/blob/main/Reference%20Images/3.PNG
[4]:https://github.com/Akshaya-Kamble/Movies-ETL/blob/main/Reference%20Images/4.PNG
[5]:https://github.com/Akshaya-Kamble/Movies-ETL/blob/main/Reference%20Images/5.PNG
[6]:https://github.com/Akshaya-Kamble/Movies-ETL/blob/main/Reference%20Images/6.PNG
[7]:https://github.com/Akshaya-Kamble/Movies-ETL/blob/main/Reference%20Images/7.PNG
[8]:https://github.com/Akshaya-Kamble/Movies-ETL/blob/main/Reference%20Images/8.PNG
[9]:https://github.com/Akshaya-Kamble/Movies-ETL/blob/main/Reference%20Images/9.PNG
[10]:https://github.com/Akshaya-Kamble/Movies-ETL/blob/main/Reference%20Images/10.PNG
[11]:https://github.com/Akshaya-Kamble/Movies-ETL/blob/main/Reference%20Images/11.PNG
[12]:https://github.com/Akshaya-Kamble/Movies-ETL/blob/main/Reference%20Images/12.PNG
[13]:https://github.com/Akshaya-Kamble/Movies-ETL/blob/main/Reference%20Images/13.PNG
[14]:https://github.com/Akshaya-Kamble/Movies-ETL/blob/main/Resources/movies_query.png
[15]:https://github.com/Akshaya-Kamble/Movies-ETL/blob/main/Resources/ratings_query.png
[16]:https://github.com/Akshaya-Kamble/Movies-ETL/blob/main/Reference%20Images/16.PNG
