DB_FILE="/home/harsha/Documents/Movie-Reccomendation-App/database_v2/movies.sqlite"

# Set the SQL query to run
SQL_QUERY="SELECT COUNT(*) ,MAX(date) as newest_date ,MIN(date) as oldest_date from tweets"

sqlite3 $DB_FILE "$SQL_QUERY" 
