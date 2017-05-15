To Run ETL Program (quartet_project) 

1. Move SQLite DB file (SQLiteTest.db) to an accessible directory.

2. Modify config.ini file with your specifications:
	DB_PATH=[Path to your SQLiteTest.db]
	MAIL_DEFAULT_SENDER=[Sender email address]
	MAIL_DEFAULT_PASSWORD=[Sender email address password]
	ERROR_EMAIL_LIST=[Comma seperated email recipient list]

3. Install required python modules using the following command in root directory:
	$ pip freeze -r requirements.txt

4. Navigate to the root directory and enter following command:
	$ python etl\LoadHealthFacilities.py LoadFacilityDimensions --local-scheduler


Note: 
Program requires python 3.5 or later to be installed
Program only tested on Windows operating system.
Your email provider may require you authorize permisson before it sends emails from this program. (I know google does)
You can view UI friendly results in the database by using: http://sqlitebrowser.org/
